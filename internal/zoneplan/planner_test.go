package zoneplan

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/pump"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
)

type fixture struct {
	series   *telemetry.Series
	builder  *telemetry.SnapshotBuilder
	zones    *network.State
	registry *network.Registry
	stations *station.Registry
	planner  *Planner
	executor *Executor
	valveOps *valve.Operations
}

func newFixture(t *testing.T, zone network.Zone, items []station.Station, opts Options) *fixture {
	t.Helper()
	state := network.NewState([]network.Zone{zone})
	registry := station.NewRegistry(items)
	bindings := make([]network.StationBinding, 0, len(items))
	for _, item := range items {
		bindings = append(bindings, network.StationBinding{StationID: item.ID, ZoneID: item.ZoneID, Required: true})
	}
	netRegistry := network.NewRegistry(bindings)
	series := telemetry.NewSeries(128)
	builder := telemetry.NewSnapshotBuilder(series)
	interlock := hydraulic.NewInterlock(state)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl")))
	planner := NewPlanner(registry, state, netRegistry, builder, hydraulic.NewBalancer(0.28), opts)
	ops := valve.NewOperations()
	executor := NewExecutor(planner, builder, state, netRegistry,
		pump.NewService(interlock, recorder), valve.NewService(interlock, recorder), ops, recorder)
	return &fixture{
		series: series, builder: builder, zones: state, registry: netRegistry,
		stations: registry, planner: planner, executor: executor, valveOps: ops,
	}
}

func (f *fixture) seedStation(stationID string, watermark uint64, supply, ret, flow, position float64, at time.Time) {
	values := []struct {
		kind  telemetry.Kind
		value float64
	}{
		{telemetry.KindSupplyPressure, supply},
		{telemetry.KindReturnPressure, ret},
		{telemetry.KindFlow, flow},
		{telemetry.KindValvePosition, position},
	}
	for _, entry := range values {
		if err := f.series.Add(telemetry.Sample{
			StationID: stationID, Kind: entry.kind, Watermark: watermark, Value: entry.value, At: at,
		}); err != nil {
			panic(err)
		}
	}
}

func sampleTime() time.Time { return time.Now().UTC() }

func TestPlanner_BalancedPlanRequiresNoCommand(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	// dP 0.34, valve 70% -> expected flow ~7.35, matching the design target.
	fx.seedStation("s1", 1, 0.82, 0.48, 7.35, 70, now)
	fx.seedStation("s2", 1, 0.82, 0.48, 7.35, 70, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusReady {
		t.Fatalf("status = %s, want ready; message: %s", plan.Status, plan.Message)
	}
	if plan.Watermark != 1 {
		t.Fatalf("watermark = %d, want 1", plan.Watermark)
	}
	if plan.Dispatchable {
		t.Fatalf("balanced plan must not be dispatchable")
	}
	if len(plan.Operations) != 0 {
		t.Fatalf("balanced plan carries %d operations", len(plan.Operations))
	}
	if len(plan.Gaps) != 0 {
		t.Fatalf("unexpected gaps: %+v", plan.Gaps)
	}
}

func TestPlanner_ValveTrimOnSurplus(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, DefaultOptions())
	fx.seedStation("s1", 1, 0.78, 0.46, 9.5, 80, sampleTime())

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !plan.Dispatchable {
		t.Fatalf("plan should carry an allowed trim: %+v", plan.Operations)
	}
	var trim *ProposedOperation
	for i := range plan.Operations {
		if plan.Operations[i].Type == ActionValveTrim {
			trim = &plan.Operations[i]
		}
	}
	if trim == nil {
		t.Fatalf("no valve trim proposed: %+v", plan.Operations)
	}
	if !trim.Allowed || trim.From != 80 || trim.To >= 80 {
		t.Fatalf("trim should close toward target: %+v", trim)
	}
}

func TestPlanner_MissingMeasurementAwaitsData(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	fx.seedStation("s1", 1, 0.82, 0.48, 7.35, 70, now)
	// s2 never reports flow: its four kinds never share a watermark.
	for _, kind := range []telemetry.Kind{telemetry.KindSupplyPressure, telemetry.KindReturnPressure, telemetry.KindValvePosition} {
		if err := fx.series.Add(telemetry.Sample{StationID: "s2", Kind: kind, Watermark: 1, Value: 0.5, At: now}); err != nil {
			t.Fatal(err)
		}
	}

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusAwaitingData {
		t.Fatalf("status = %s, want awaiting_data", plan.Status)
	}
	if plan.Dispatchable || len(plan.Operations) != 0 {
		t.Fatal("awaiting plan must not carry commands")
	}
	found := false
	for _, gap := range plan.Gaps {
		if gap.StationID == "s2" && gap.Code == GapMeasurementMissing && gap.Kind == string(telemetry.KindFlow) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected measurement_missing flow gap for s2, got %+v", plan.Gaps)
	}
}

func TestPlanner_MisalignedWatermarksAwaitData(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	fx.seedStation("s1", 2, 0.82, 0.48, 7.35, 70, now)
	fx.seedStation("s2", 1, 0.82, 0.48, 7.35, 70, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusAwaitingData {
		t.Fatalf("status = %s, want awaiting_data", plan.Status)
	}
	stationCodes := map[string]map[GapCode]bool{}
	for _, gap := range plan.Gaps {
		if stationCodes[gap.StationID] == nil {
			stationCodes[gap.StationID] = map[GapCode]bool{}
		}
		stationCodes[gap.StationID][gap.Code] = true
	}
	if !stationCodes["s1"][GapNoAlignedWatermark] {
		t.Fatalf("s1 must be flagged as ahead of the common watermark, got %+v", plan.Gaps)
	}
	if !stationCodes["s2"][GapNoAlignedWatermark] || !stationCodes["s2"][GapMeasurementMissing] {
		t.Fatalf("s2 must owe measurements at the zone watermark, got %+v", plan.Gaps)
	}
}

func TestPlanner_StaleWatermarkAwaitsData(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	opts := DefaultOptions()
	opts.MaxStaleness = 50 * time.Millisecond
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, opts)
	fx.seedStation("s1", 1, 0.82, 0.48, 7.35, 70, time.Now().Add(-time.Second).UTC())

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusAwaitingData {
		t.Fatalf("status = %s, want awaiting_data", plan.Status)
	}
	found := false
	for _, gap := range plan.Gaps {
		if gap.Code == GapStaleWatermark && gap.Watermark == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected stale_watermark gap, got %+v", plan.Gaps)
	}
}

func TestPlanner_LeakSuspectedBlocksCommands(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	// Valve wide open at 95% expects ~9.97 L/s; measured 4.0 is a 60% loss.
	fx.seedStation("s1", 1, 0.82, 0.48, 4.0, 95, now)
	fx.seedStation("s2", 1, 0.82, 0.48, 7.35, 70, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusBlocked {
		t.Fatalf("status = %s, want blocked", plan.Status)
	}
	for _, op := range plan.Operations {
		if op.Allowed {
			t.Fatalf("blocked plan must not carry allowed ops: %+v", op)
		}
	}
	if len(plan.Risks) == 0 || plan.Risks[0].Code != RiskLeakSuspected || plan.Risks[0].StationID != "s1" {
		t.Fatalf("leak must be the top-ranked risk, got %+v", plan.Risks)
	}
	if _, err := fx.executor.Apply(context.Background(), plan.ID, "ops"); err == nil {
		t.Fatal("applying a blocked plan must fail")
	}
}

func TestPlanner_StarvedSaturatedStationsStagePumpUp(t *testing.T) {
	// Zone dP 0.34, stations see 0.15 with a 95% valve: target opening exceeds
	// 100% while measured flow stays under target, so the pump must stage up.
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	fx.seedStation("s1", 1, 0.55, 0.40, 5.5, 95, now)
	fx.seedStation("s2", 1, 0.55, 0.40, 5.5, 95, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.TargetStage != 3 {
		t.Fatalf("target stage = %d, want 3", plan.TargetStage)
	}
	if !plan.BypassNeeded {
		t.Fatal("stage 3 must require the bypass")
	}
	var pumpOp *ProposedOperation
	for i := range plan.Operations {
		if plan.Operations[i].Type == ActionPumpStage {
			pumpOp = &plan.Operations[i]
		}
		if plan.Operations[i].Type == ActionValveTrim && plan.Operations[i].Allowed {
			t.Fatal("valve trims must wait for the next watermark when the pump moves")
		}
	}
	if pumpOp == nil || !pumpOp.Allowed || pumpOp.From != 2 || pumpOp.To != 3 {
		t.Fatalf("expected allowed 2->3 pump op, got %+v", pumpOp)
	}
	for _, op := range plan.Operations {
		if op.Type == ActionValveTrim && (op.To < 0 || op.To > 100) {
			t.Fatalf("valve trim target %.1f outside the 0-100 envelope", op.To)
		}
	}
}

func TestExecutor_BypassOpensBeforePumpStage(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: false, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	fx.seedStation("s1", 1, 0.55, 0.40, 5.5, 95, now)
	fx.seedStation("s2", 1, 0.55, 0.40, 5.5, 95, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Operations[0].Type != ActionBypass || !plan.Operations[0].Allowed {
		t.Fatalf("first op must be the bypass, got %+v", plan.Operations[0])
	}
	applied, err := fx.executor.Apply(context.Background(), plan.ID, "ops")
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied.Status != StatusApplied {
		t.Fatalf("status = %s, want applied (%s)", applied.Status, applied.Message)
	}
	current, _ := fx.zones.Get("north")
	if current.PumpStage != 3 {
		t.Fatalf("pump stage = %d, want 3", current.PumpStage)
	}
	if !current.BypassOpen {
		t.Fatal("bypass must be open at stage 3")
	}
	if len(applied.AppliedOps) != 2 {
		t.Fatalf("applied %d ops, want bypass + pump", len(applied.AppliedOps))
	}
}

func TestExecutor_RejectsWhenWatermarkAdvanced(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	fx.seedStation("s1", 1, 0.78, 0.46, 9.5, 80, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	// Newer aligned telemetry arrives before the operator applies.
	fx.seedStation("s1", 2, 0.78, 0.46, 9.4, 80, time.Now().UTC())
	if _, err := fx.executor.Apply(context.Background(), plan.ID, "ops"); err == nil {
		t.Fatal("apply on a superseded watermark must be rejected")
	}
	stored, _ := fx.planner.Plan(plan.ID)
	if stored.Status != StatusRejected {
		t.Fatalf("status = %s, want rejected", stored.Status)
	}
	current, _ := fx.zones.Get("north")
	if current.PumpStage != 2 {
		t.Fatalf("pump stage changed to %d despite rejection", current.PumpStage)
	}
}

func TestExecutor_RejectsExpiredPlan(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	opts := DefaultOptions()
	opts.MaxStaleness = 40 * time.Millisecond
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, opts)
	fx.seedStation("s1", 1, 0.78, 0.46, 9.5, 80, time.Now().UTC())
	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusReady {
		t.Fatalf("fresh plan should be ready, got %s", plan.Status)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := fx.executor.Apply(context.Background(), plan.ID, "ops"); err == nil {
		t.Fatal("expired plan must be rejected")
	}
}

func TestExecutor_RejectsAwaitingPlanWithoutCommands(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, DefaultOptions())
	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusAwaitingData {
		t.Fatalf("status = %s, want awaiting_data", plan.Status)
	}
	if _, err := fx.executor.Apply(context.Background(), plan.ID, "ops"); err == nil {
		t.Fatal("awaiting-data plan must never dispatch")
	}
	stored, _ := fx.planner.Plan(plan.ID)
	if stored.Status != StatusAwaitingData {
		t.Fatalf("awaiting plan status changed to %s", stored.Status)
	}
}

func TestPlanner_HeldStationTrimIsNotAllowed(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true, Hold: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	now := sampleTime()
	fx.seedStation("s1", 1, 0.78, 0.46, 9.5, 80, now)
	fx.seedStation("s2", 1, 0.78, 0.46, 9.5, 80, now)

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	allowedStations := map[string]bool{}
	for _, op := range plan.Operations {
		if op.Type == ActionValveTrim && op.Allowed {
			allowedStations[op.StationID] = true
		}
		if op.StationID == "s1" && op.Allowed {
			t.Fatal("held station trim must not be allowed")
		}
	}
	if !allowedStations["s2"] {
		t.Fatalf("non-held station trim should remain allowed: %+v", plan.Operations)
	}
	applied, err := fx.executor.Apply(context.Background(), plan.ID, "ops")
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied.Status != StatusApplied {
		t.Fatalf("status = %s", applied.Status)
	}
	if owner := fx.valveOps.Owner("s1-cv"); owner != "" {
		t.Fatalf("held station valve was moved, owner = %q", owner)
	}
	if owner := fx.valveOps.Owner("s2-cv"); owner == "" {
		t.Fatal("s2 control valve should have an active operation")
	}
}

func TestPlanner_NewPlanSupersedesPrevious(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, DefaultOptions())
	fx.seedStation("s1", 1, 0.78, 0.46, 9.5, 80, sampleTime())
	first, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate first: %v", err)
	}
	fx.seedStation("s1", 2, 0.78, 0.46, 9.4, 80, time.Now().UTC())
	second, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate second: %v", err)
	}
	stored, _ := fx.planner.Plan(first.ID)
	if stored.Status != StatusSuperseded {
		t.Fatalf("first plan status = %s, want superseded", stored.Status)
	}
	if second.Watermark != 2 {
		t.Fatalf("second plan watermark = %d, want 2", second.Watermark)
	}
	if _, err := fx.executor.Apply(context.Background(), first.ID, "ops"); err == nil {
		t.Fatal("superseded plan must not apply")
	}
}

func TestPlanner_StationWithoutAnyTelemetryAwaitsData(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
		{ID: "s2", ZoneID: "north", Online: true},
	}, DefaultOptions())
	fx.seedStation("s1", 1, 0.82, 0.48, 7.35, 70, sampleTime())

	plan, err := fx.planner.Generate("north")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if plan.Status != StatusAwaitingData {
		t.Fatalf("status = %s, want awaiting_data", plan.Status)
	}
	found := false
	for _, gap := range plan.Gaps {
		if gap.StationID == "s2" && gap.Code == GapNoTelemetry {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected no_telemetry gap for silent s2, got %+v", plan.Gaps)
	}
}

func TestPlanner_UnknownZoneErrors(t *testing.T) {
	zone := network.Zone{ID: "north", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48}
	fx := newFixture(t, zone, []station.Station{
		{ID: "s1", ZoneID: "north", Online: true},
	}, DefaultOptions())
	if _, err := fx.planner.Generate("south"); err == nil {
		t.Fatal("unknown zone must error")
	}
}
