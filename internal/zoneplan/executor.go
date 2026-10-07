package zoneplan

import (
	"context"
	"fmt"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/pump"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
)

// Executor turns a ready plan into commands. Every apply re-proves the bound
// watermark: if telemetry advanced, aged out, the zone moved or the hydraulic
// envelope no longer fits, the plan is rejected and nothing is sent.
type Executor struct {
	planner  *Planner
	builder  *telemetry.SnapshotBuilder
	zones    *network.State
	registry *network.Registry
	pumps    *pump.Service
	valves   *valve.Service
	valveOps *valve.Operations
	audit    *audit.Recorder
}

func NewExecutor(
	planner *Planner,
	builder *telemetry.SnapshotBuilder,
	zones *network.State,
	registry *network.Registry,
	pumps *pump.Service,
	valves *valve.Service,
	valveOps *valve.Operations,
	recorder *audit.Recorder,
) *Executor {
	return &Executor{
		planner: planner, builder: builder, zones: zones, registry: registry,
		pumps: pumps, valves: valves, valveOps: valveOps, audit: recorder,
	}
}

// Apply dispatches the allowed operations of a plan in safe order (bypass,
// then pump stage, then per-station valve trims).
func (e *Executor) Apply(ctx context.Context, planID, actor string) (Plan, error) {
	plan, ok := e.planner.Plan(planID)
	if !ok {
		return Plan{}, fmt.Errorf("balance plan %s not found", planID)
	}
	if reason := gateReason(plan.Status); reason != "" {
		return plan, fmt.Errorf("balance plan %s is %s: %s", planID, plan.Status, reason)
	}

	zone, ok := e.zones.Get(plan.ZoneID)
	if !ok {
		return e.refuse(plan, actor, "zone vanished from the registry", nil)
	}
	stationIDs := make([]string, 0, len(plan.Measures))
	for _, measure := range plan.Measures {
		stationIDs = append(stationIDs, measure.StationID)
	}

	if err := e.verifyWatermark(plan, zone, stationIDs, time.Now().UTC()); err != nil {
		return e.refuse(plan, actor, err.Error(), map[string]any{"watermark": plan.Watermark})
	}
	if err := ctx.Err(); err != nil {
		return plan, err
	}

	allowed := make([]ProposedOperation, 0, len(plan.Operations))
	for _, operation := range plan.Operations {
		if operation.Allowed {
			allowed = append(allowed, operation)
		}
	}
	if len(allowed) == 0 {
		return plan, fmt.Errorf("balance plan %s carries no allowed operation", planID)
	}

	applied := make([]ProposedOperation, 0, len(allowed))
	for _, operation := range allowed {
		if err := ctx.Err(); err != nil {
			return e.fail(plan, applied, actor, "context canceled before "+string(operation.Type))
		}
		switch operation.Type {
		case ActionBypass:
			if _, _, err := e.valves.SetBypass(ctx, plan.ZoneID, operation.ValveID, true, actor); err != nil {
				return e.fail(plan, applied, actor, err.Error())
			}
		case ActionPumpStage:
			if _, _, err := e.pumps.SetStage(ctx, plan.ZoneID, int(operation.To), actor); err != nil {
				return e.fail(plan, applied, actor, err.Error())
			}
		case ActionValveTrim:
			if _, err := e.valveOps.Start(operation.ValveID, "zoneplan:"+plan.ID, operation.To); err != nil {
				return e.fail(plan, applied, actor, err.Error())
			}
			_ = e.audit.Accepted("zoneplan.valve_trim", operation.StationID, actor,
				fmt.Sprintf("control valve %s moving to %.1f%%", operation.ValveID, operation.To),
				map[string]any{"plan_id": plan.ID, "watermark": plan.Watermark, "target": operation.To})
		default:
			return e.fail(plan, applied, actor, "unknown operation type "+string(operation.Type))
		}
		applied = append(applied, operation)
	}

	finished, _ := e.planner.finish(planID, func(stored *Plan) {
		stored.Status = StatusApplied
		stored.AppliedAt = time.Now().UTC()
		stored.AppliedOps = applied
		stored.Message = "all allowed operations dispatched on watermark " + fmt.Sprint(plan.Watermark)
	})
	_ = e.audit.Accepted("zoneplan.apply", plan.ZoneID, actor, finished.Message,
		map[string]any{"plan_id": plan.ID, "watermark": plan.Watermark, "operations": len(applied)})
	return finished, nil
}

// verifyWatermark re-proves at apply time that the plan still describes the
// live network. This is the last barrier between a plan and the actuators.
func (e *Executor) verifyWatermark(plan Plan, zone network.Zone, stationIDs []string, now time.Time) error {
	if !plan.Dispatchable || plan.Status != StatusReady {
		return fmt.Errorf("plan is not dispatchable")
	}
	if !now.Before(plan.ExpiresAt) {
		return fmt.Errorf("plan expired at %s; regenerate on a fresh watermark", plan.ExpiresAt.Format(time.RFC3339))
	}
	if zone.Phase != network.PhaseActive {
		return fmt.Errorf("zone %s is %s, hydraulic dispatch is interlocked", zone.ID, zone.Phase)
	}
	if zone.PumpStage != plan.CurrentStage || zone.BypassOpen != plan.BypassOpen {
		return fmt.Errorf("zone moved since binding: pump stage %d→%d bypass %t→%t",
			plan.CurrentStage, zone.PumpStage, plan.BypassOpen, zone.BypassOpen)
	}
	common := e.builder.AlignedWatermarks(stationIDs, telemetry.HydraulicKinds)
	if len(common) == 0 {
		return fmt.Errorf("no aligned telemetry remains for the zone")
	}
	latest := common[len(common)-1]
	if latest != plan.Watermark {
		return fmt.Errorf("watermark %d is no longer current; %d is available, regenerate the plan", plan.Watermark, latest)
	}
	oldest := now
	for _, stationID := range stationIDs {
		snapshot, err := e.builder.BuildAt(stationID, plan.Watermark)
		if err != nil {
			return fmt.Errorf("station %s lost watermark %d: %w", stationID, plan.Watermark, err)
		}
		if snapshot.CapturedAt.Before(oldest) {
			oldest = snapshot.CapturedAt
		}
	}
	if now.Sub(oldest) > e.planner.opts.MaxStaleness {
		return fmt.Errorf("bound telemetry aged past the freshness limit; regenerate on a fresh watermark")
	}
	candidate := hydraulic.Candidate{PumpStage: plan.TargetStage, BypassOpen: plan.BypassNeeded || plan.BypassOpen}
	if err := hydraulic.Validate(candidate); err != nil {
		return fmt.Errorf("target pump envelope rejected: %w", err)
	}
	return nil
}

func (e *Executor) refuse(plan Plan, actor, reason string, details map[string]any) (Plan, error) {
	if details == nil {
		details = map[string]any{}
	}
	details["plan_id"] = plan.ID
	_ = e.audit.Rejected("zoneplan.apply", plan.ZoneID, actor, reason, details)
	if plan.Status == StatusReady || plan.Status == StatusBlocked {
		finished, _ := e.planner.finish(plan.ID, func(stored *Plan) {
			stored.Status = StatusRejected
			stored.Message = reason
		})
		return finished, fmt.Errorf("%s", reason)
	}
	return plan, fmt.Errorf("%s", reason)
}

func (e *Executor) fail(plan Plan, applied []ProposedOperation, actor, reason string) (Plan, error) {
	finished, _ := e.planner.finish(plan.ID, func(stored *Plan) {
		stored.Status = StatusFailed
		stored.AppliedOps = append([]ProposedOperation(nil), applied...)
		stored.Message = reason
	})
	_ = e.audit.Rejected("zoneplan.apply", plan.ZoneID, actor, reason,
		map[string]any{"plan_id": plan.ID, "watermark": plan.Watermark, "applied_before_failure": len(applied)})
	return finished, fmt.Errorf("%s", reason)
}

func gateReason(status Status) string {
	switch status {
	case StatusReady:
		return ""
	case StatusAwaitingData:
		return "telemetry is stale or incomplete, supply the listed gaps and regenerate before dispatch"
	case StatusBlocked:
		return "a hydraulic safety condition blocks this zone"
	case StatusRejected, StatusFailed, StatusApplied, StatusSuperseded:
		return "terminal plan, generate a new one"
	default:
		return "unknown plan state"
	}
}
