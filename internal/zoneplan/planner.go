package zoneplan

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/station"
	"heatpulse/internal/telemetry"
)

// Planner assembles zonal balance plans. It never issues commands: callers
// must pass a ready, dispatchable plan through the Executor, which re-checks
// the watermark before anything is sent to a pump or valve.
type Planner struct {
	stations *station.Registry
	zones    *network.State
	registry *network.Registry
	builder  *telemetry.SnapshotBuilder
	balancer *hydraulic.Balancer
	opts     Options

	mu    sync.RWMutex
	plans map[string]Plan
}

func NewPlanner(
	stations *station.Registry,
	zones *network.State,
	registry *network.Registry,
	builder *telemetry.SnapshotBuilder,
	balancer *hydraulic.Balancer,
	opts Options,
) *Planner {
	if opts.MaxStaleness <= 0 {
		opts = DefaultOptions()
	}
	return &Planner{
		stations: stations, zones: zones, registry: registry, builder: builder,
		balancer: balancer, opts: opts, plans: make(map[string]Plan),
	}
}

// Generate binds a fresh plan for the zone to the newest common watermark.
// Stale or incomplete evidence yields an awaiting_data plan carrying gaps.
func (p *Planner) Generate(zoneID string) (Plan, error) {
	zone, ok := p.zones.Get(zoneID)
	if !ok {
		return Plan{}, fmt.Errorf("zone %s not found", zoneID)
	}
	bindings := p.registry.ForZone(zoneID)
	if len(bindings) == 0 {
		return Plan{}, fmt.Errorf("zone %s has no bound stations", zoneID)
	}
	stationIDs := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		stationIDs = append(stationIDs, binding.StationID)
	}
	sort.Strings(stationIDs)

	now := time.Now().UTC()
	plan := Plan{
		ID: uuid.NewString(), ZoneID: zoneID, Status: StatusAwaitingData,
		CurrentStage: zone.PumpStage, TargetStage: zone.PumpStage,
		BypassOpen: zone.BypassOpen, CreatedAt: now,
	}

	watermark, gaps := p.collectGaps(zoneID, stationIDs, now)
	plan.Watermark = watermark
	plan.Gaps = gaps
	if len(gaps) > 0 {
		plan.Message = "telemetry incomplete or stale; supply the listed measurements before dispatch"
		p.save(&plan)
		return plan.Clone(), nil
	}

	snapshots := make(map[string]telemetry.HydraulicSnapshot, len(stationIDs))
	boundAt := now
	for _, stationID := range stationIDs {
		snapshot, err := p.builder.BuildAt(stationID, watermark)
		if err != nil {
			// Defensive: collectGaps verified alignment, so this means the
			// series moved underneath us; ask for fresh data rather than act.
			plan.Gaps = append(plan.Gaps, InputGap{
				StationID: stationID, Code: GapMeasurementMissing, Watermark: watermark,
				Message: err.Error(), ObservedAt: now,
			})
			plan.Message = "telemetry moved while binding; retry on the latest watermark"
			p.save(&plan)
			return plan.Clone(), nil
		}
		snapshots[stationID] = snapshot
		if snapshot.CapturedAt.Before(boundAt) {
			boundAt = snapshot.CapturedAt
		}
	}
	plan.BoundAt = boundAt
	plan.ExpiresAt = boundAt.Add(p.opts.MaxStaleness)

	measures, blocked := p.measure(stationIDs, snapshots, zone)
	plan.Measures = measures

	if blocked != "" {
		plan.Status = StatusBlocked
		plan.Message = blocked
		plan.Risks = rankRisks(risksForMeasures(measures, p.opts))
		p.save(&plan)
		return plan.Clone(), nil
	}
	if zone.Phase != network.PhaseActive {
		plan.Status = StatusBlocked
		plan.Message = fmt.Sprintf("zone %s is %s, hydraulic dispatch is interlocked", zoneID, zone.Phase)
		plan.Risks = rankRisks(risksForMeasures(measures, p.opts))
		p.save(&plan)
		return plan.Clone(), nil
	}

	plan.TargetStage, plan.BypassNeeded = p.decideStage(zone, measures)
	stageDelta := plan.TargetStage - plan.CurrentStage
	for index := range plan.Measures {
		p.applyTargets(&plan.Measures[index], zone, stageDelta)
	}
	plan.Risks = rankRisks(append(risksForMeasures(plan.Measures, p.opts), zoneRisks(plan.Measures, plan.TargetStage, p.opts)...))
	plan.Operations = p.proposeOperations(zone, plan.Measures, plan.TargetStage, plan.BypassNeeded, stageDelta)
	plan.Status = StatusReady
	for _, operation := range plan.Operations {
		if operation.Allowed {
			plan.Dispatchable = true
			break
		}
	}
	if !plan.Dispatchable {
		plan.Message = "zone is within balance tolerance; no command required"
	}
	p.save(&plan)
	return plan.Clone(), nil
}

// collectGaps returns the newest watermark common to every bound station plus
// the evidence gaps that disqualify it. When stations have advanced to
// different watermarks, the gaps name the zone target (highest observed
// watermark) and list exactly which measurements each lagging station owes.
func (p *Planner) collectGaps(zoneID string, stationIDs []string, now time.Time) (uint64, []InputGap) {
	gaps := make([]InputGap, 0)
	var target uint64
	for _, stationID := range stationIDs {
		if max, has := p.builder.MaxWatermark(stationID); has && max > target {
			target = max
		}
	}
	common := p.builder.AlignedWatermarks(stationIDs, telemetry.HydraulicKinds)
	var watermark uint64
	if len(common) > 0 {
		watermark = common[len(common)-1]
	}
	var boundAt time.Time
	for _, stationID := range stationIDs {
		_, has := p.builder.MaxWatermark(stationID)
		if !has {
			gaps = append(gaps, InputGap{
				StationID: stationID, Code: GapNoTelemetry, ObservedAt: now,
				Message: fmt.Sprintf("station %s has no hydraulic telemetry", stationID),
			})
			continue
		}
		snapshot, err := p.builder.BuildAt(stationID, target)
		if err != nil {
			if target != watermark {
				gaps = append(gaps, InputGap{
					StationID: stationID, Code: GapNoAlignedWatermark, Watermark: target, ObservedAt: now,
					Message: fmt.Sprintf("station %s has not caught up with zone watermark %d", stationID, target),
				})
			}
			for _, kind := range p.builder.MissingKinds(stationID, target) {
				gaps = append(gaps, InputGap{
					StationID: stationID, Code: GapMeasurementMissing, Kind: string(kind),
					Watermark: target, ObservedAt: now,
					Message: fmt.Sprintf("station %s owes %s at watermark %d", stationID, kind, target),
				})
			}
			continue
		}
		if target != watermark {
			gaps = append(gaps, InputGap{
				StationID: stationID, Code: GapNoAlignedWatermark, Watermark: target, ObservedAt: now,
				Message: fmt.Sprintf("station %s is complete at %d but peer stations lag behind", stationID, target),
			})
			continue
		}
		if snapshot.DifferentialPressure() < 0 {
			gaps = append(gaps, InputGap{
				StationID: stationID, Code: GapMeasurementInvalid, Watermark: target, ObservedAt: now,
				Message: fmt.Sprintf("negative differential pressure %.2f", snapshot.DifferentialPressure()),
			})
		}
		if snapshot.Flow < 0 {
			gaps = append(gaps, InputGap{
				StationID: stationID, Code: GapMeasurementInvalid, Watermark: target, ObservedAt: now,
				Message: fmt.Sprintf("negative flow %.2f", snapshot.Flow),
			})
		}
		if snapshot.ValvePosition < 0 || snapshot.ValvePosition > 100 {
			gaps = append(gaps, InputGap{
				StationID: stationID, Code: GapMeasurementInvalid, Watermark: target, ObservedAt: now,
				Message: fmt.Sprintf("valve position %.1f outside 0-100", snapshot.ValvePosition),
			})
		}
		if boundAt.IsZero() || snapshot.CapturedAt.Before(boundAt) {
			boundAt = snapshot.CapturedAt
		}
	}
	if watermark != 0 && len(gaps) == 0 {
		if age := now.Sub(boundAt); age > p.opts.MaxStaleness {
			gaps = append(gaps, InputGap{
				Code: GapStaleWatermark, Watermark: watermark, ObservedAt: now,
				Message: fmt.Sprintf("zone %s watermark %d is %s old, freshness limit is %s",
					zoneID, watermark, age.Round(time.Second), p.opts.MaxStaleness),
			})
		}
	}
	sortGaps(gaps)
	return watermark, gaps
}

// measure evaluates every station against the existing hydraulic model and
// reports whether any station is suspected to be leaking.
func (p *Planner) measure(
	stationIDs []string, snapshots map[string]telemetry.HydraulicSnapshot, zone network.Zone,
) ([]StationMeasure, string) {
	measures := make([]StationMeasure, 0, len(stationIDs))
	blocked := ""
	for _, stationID := range stationIDs {
		snapshot := snapshots[stationID]
		balance, err := p.balancer.Evaluate(snapshot)
		if err != nil {
			// Alignment and physical ranges were checked upstream.
			blocked = fmt.Sprintf("cannot evaluate station %s on watermark %d: %s", stationID, snapshot.Watermark, err)
			continue
		}
		info, _ := p.stations.Get(stationID)
		item := StationMeasure{
			StationID: stationID, Watermark: snapshot.Watermark, Online: info.Online, Held: info.Hold,
			SupplyPressure: snapshot.Supply, ReturnPressure: snapshot.Return,
			DifferentialPressure: balance.DifferentialPressure, MeasuredFlow: snapshot.Flow,
			ExpectedFlow: balance.ExpectedFlow, ValvePosition: snapshot.ValvePosition,
			SuggestedPosition: snapshot.ValvePosition, ImbalanceRatio: balance.ImbalanceRatio,
			LeakSuspected:  balance.LeakLikely,
			ControlValveID: stationID + "-cv",
		}
		p.applyTargets(&item, zone, 0)
		measures = append(measures, item)
		if balance.LeakLikely {
			blocked = fmt.Sprintf("station %s shows a %.0f%% flow imbalance on watermark %d; isolate the leak before moving pumps or valves",
				stationID, balance.ImbalanceRatio*100, snapshot.Watermark)
		}
	}
	return measures, blocked
}

// applyTargets fills the design target flow, the deviation and the valve
// opening that would satisfy the target, given the chosen pump-stage delta.
func (p *Planner) applyTargets(item *StationMeasure, zone network.Zone, stageDelta int) {
	zoneDP := math.Max(zone.DifferentialPressure(), 0.01)
	drive := 1 + p.opts.StageGain*float64(stageDelta)
	target := 18 * math.Sqrt(zoneDP) * p.opts.DesignOpening * drive
	item.TargetFlow = target
	item.FlowDelta = item.MeasuredFlow - target
	item.FlowDeltaRatio = item.FlowDelta / math.Max(target, 1)

	opening := target / (18 * math.Sqrt(math.Max(item.DifferentialPressure, 0.01)))
	required := clamp(opening*100, p.opts.ValveMin, p.opts.ValveMax)
	item.RequiredOpening = math.Round(required*10) / 10
	// Limit the single-command move to ValveStep, then re-clamp to the valve's
	// physical envelope so a suggested position can never exceed 100%.
	suggested := clamp(opening*100, item.ValvePosition-p.opts.ValveStep, item.ValvePosition+p.opts.ValveStep)
	suggested = clamp(suggested, p.opts.ValveMin, p.opts.ValveMax)
	suggested = math.Round(suggested*10) / 10
	if math.Abs(suggested-item.ValvePosition) < p.opts.ValveDeadband {
		suggested = item.ValvePosition
	}
	item.SuggestedPosition = suggested
}

// decideStage moves the zone pump at most one step: stations starved while
// their control valve is already saturated open need more drive; every
// station throttled against surplus needs less.
func (p *Planner) decideStage(zone network.Zone, measures []StationMeasure) (int, bool) {
	stage := zone.PumpStage
	starved, throttled := 0, 0
	for _, item := range measures {
		if !item.Online || item.LeakSuspected {
			continue
		}
		if item.FlowDeltaRatio < -p.opts.FlowTolerance && item.RequiredOpening >= p.opts.ValveMax-0.01 {
			starved++
		}
		if item.FlowDeltaRatio > p.opts.FlowTolerance && item.RequiredOpening <= p.opts.ValveMin+0.01 {
			throttled++
		}
	}
	active := 0
	for _, item := range measures {
		if item.Online {
			active++
		}
	}
	switch {
	case starved > 0 && stage < p.opts.MaxStage:
		stage++
	case active > 0 && throttled == active && stage > p.opts.MinStage:
		stage--
	}
	return stage, stage >= 3
}

func (p *Planner) proposeOperations(
	zone network.Zone, measures []StationMeasure, targetStage int, bypassNeeded bool, stageDelta int,
) []ProposedOperation {
	operations := make([]ProposedOperation, 0, len(measures)+2)
	step := 1
	stageMoving := stageDelta != 0
	if bypassNeeded && !zone.BypassOpen {
		operations = append(operations, ProposedOperation{
			Type: ActionBypass, Step: step, ZoneID: zone.ID,
			ValveID: zone.ID + "-bypass", From: 0, To: 1, Allowed: true,
			Reason: "hydraulic interlock requires an open bypass at pump stage 3 and above",
		})
		step++
	}
	if stageMoving {
		reason := ""
		allowed := true
		blockedBy := 0
		if targetStage > p.opts.MaxStage {
			allowed, blockedBy = false, p.opts.MaxStage
		}
		if targetStage < p.opts.MinStage {
			allowed, blockedBy = false, p.opts.MinStage
		}
		if !allowed {
			reason = fmt.Sprintf("pump stage envelope limits the correction to stage %d", blockedBy)
		} else if targetStage >= 3 && !(bypassNeeded || zone.BypassOpen) {
			allowed = false
			reason = "bypass must be opened before staging above 2"
		} else if stageDelta > 0 {
			reason = "stations starved with saturated control valves require more pump drive"
		} else {
			reason = "all stations throttle against surplus flow"
		}
		operations = append(operations, ProposedOperation{
			Type: ActionPumpStage, Step: step, ZoneID: zone.ID,
			From: float64(zone.PumpStage), To: float64(targetStage), Allowed: allowed, Reason: reason,
		})
		step++
	}
	for _, item := range measures {
		if item.SuggestedPosition == item.ValvePosition {
			continue
		}
		operation := ProposedOperation{
			Type: ActionValveTrim, Step: step, ZoneID: zone.ID, StationID: item.StationID,
			ValveID: item.ControlValveID, From: item.ValvePosition, To: item.SuggestedPosition,
		}
		switch {
		case !item.Online:
			operation.Reason = "station is offline"
		case item.Held:
			operation.Reason = "station is manually held; release the hold before trimming"
		case stageMoving:
			// A pump move invalidates this watermark for valve-level balance;
			// the next plan on the new watermark performs the trim.
			operation.Reason = "pump stage changes in this plan; recompute valve trims on the next watermark"
		default:
			operation.Allowed = true
			if item.SuggestedPosition < item.ValvePosition {
				operation.Reason = "close toward target flow"
			} else {
				operation.Reason = "open toward target flow"
			}
		}
		operations = append(operations, operation)
		step++
	}
	return operations
}

func (p *Planner) save(plan *Plan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if plan.Status == StatusReady || plan.Status == StatusAwaitingData || plan.Status == StatusBlocked {
		for id, existing := range p.plans {
			if existing.ZoneID == plan.ZoneID && !existing.Status.Terminal() && id != plan.ID {
				existing.Status = StatusSuperseded
				existing.Message = "superseded by plan " + plan.ID + " on watermark " + fmt.Sprint(plan.Watermark)
				p.plans[id] = existing
			}
		}
	}
	p.plans[plan.ID] = plan.Clone()
}

func (p *Planner) Plan(planID string) (Plan, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	plan, ok := p.plans[planID]
	return plan.Clone(), ok
}

// finish applies a state transition under the planner lock and returns the
// stored copy. A missing plan yields the zero value without invoking change.
func (p *Planner) finish(planID string, change func(*Plan)) (Plan, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	stored, ok := p.plans[planID]
	if !ok {
		return Plan{}, false
	}
	change(&stored)
	p.plans[planID] = stored.Clone()
	return stored, true
}

func (p *Planner) Plans() []Plan {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]Plan, 0, len(p.plans))
	for _, plan := range p.plans {
		result = append(result, plan.Clone())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

func clamp(value, low, high float64) float64 {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}

func sortGaps(gaps []InputGap) {
	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].StationID != gaps[j].StationID {
			return gaps[i].StationID < gaps[j].StationID
		}
		if gaps[i].Kind != gaps[j].Kind {
			return gaps[i].Kind < gaps[j].Kind
		}
		return gaps[i].Code < gaps[j].Code
	})
}
