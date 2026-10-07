package hydraulic

import (
	"context"
	"math"
	"testing"
	"time"

	"heatpulse/internal/network"
	"heatpulse/internal/telemetry"
)

type fakeLookup map[string]StationState

func (f fakeLookup) Get(id string) (StationState, bool) {
	item, ok := f[id]
	return item, ok
}

type zoneFixture struct {
	planner *ZoneBalancePlanner
	series  *telemetry.Series
	now     time.Time
}

func newZoneFixture(t *testing.T, zone network.Zone, ids []string) *zoneFixture {
	t.Helper()
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	series := telemetry.NewSeries(128)
	builder := telemetry.NewSnapshotBuilder(series)
	bindings := make([]network.StationBinding, 0, len(ids))
	lookup := fakeLookup{}
	for _, id := range ids {
		bindings = append(bindings, network.StationBinding{StationID: id, ZoneID: zone.ID, Required: true})
		lookup[id] = StationState{ID: id, ZoneID: zone.ID, Online: true}
	}
	state := network.NewState([]network.Zone{zone})
	interlock := NewInterlock(state)
	planner := NewZoneBalancePlanner(builder, network.NewRegistry(bindings), lookup, interlock, NewBalancer(0.28))
	planner.cfg.now = func() time.Time { return now }
	return &zoneFixture{planner: planner, series: series, now: now}
}

func (f *zoneFixture) seed(id string, watermark uint64, at time.Time, supply, ret, flow, position float64) {
	samples := []telemetry.Sample{
		{StationID: id, Kind: telemetry.KindSupplyPressure, Watermark: watermark, Value: supply, At: at},
		{StationID: id, Kind: telemetry.KindReturnPressure, Watermark: watermark, Value: ret, At: at},
		{StationID: id, Kind: telemetry.KindFlow, Watermark: watermark, Value: flow, At: at},
		{StationID: id, Kind: telemetry.KindValvePosition, Watermark: watermark, Value: position, At: at},
	}
	for _, sample := range samples {
		if err := f.series.Add(sample); err != nil {
			panic(err)
		}
	}
}

func (f *zoneFixture) seedPartial(id string, watermark uint64, at time.Time, values map[telemetry.Kind]float64) {
	for kind, value := range values {
		if err := f.series.Add(telemetry.Sample{StationID: id, Kind: kind, Watermark: watermark, Value: value, At: at}); err != nil {
			panic(err)
		}
	}
}

func TestZonePlan_ReadyFreshAlignedSnapshot(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "west", PumpStage: 2, BypassOpen: true}, []string{"nanhu", "xijiao"})
	at := fx.now.Add(-5 * time.Second)
	fx.seed("nanhu", 7, at, 0.80, 0.46, 8.4, 70)
	fx.seed("xijiao", 7, at, 0.78, 0.46, 7.8, 68)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "west"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanReady || !plan.Issuable {
		t.Fatalf("expected ready+issuable, got %s issuable=%v missing=%+v blockers=%+v", plan.Status, plan.Issuable, plan.Missing, plan.Blockers)
	}
	if plan.Watermark != 7 {
		t.Fatalf("plan must bind to shared watermark 7, got %d", plan.Watermark)
	}
	for _, sp := range plan.Stations {
		if sp.Snapshot.Watermark != 7 {
			t.Fatalf("station %s snapshot bound to watermark %d", sp.StationID, sp.Snapshot.Watermark)
		}
	}
	// Flow conservation: allocated target flows must sum to measured demand.
	sumTarget, sumCurrent := 0.0, 0.0
	for _, sp := range plan.Stations {
		sumTarget += sp.TargetFlow
		sumCurrent += sp.CurrentFlow
	}
	if math.Abs(sumTarget-sumCurrent) > 1e-9 {
		t.Fatalf("target flows sum %.4f != measured sum %.4f", sumTarget, sumCurrent)
	}
	// Risk ranking must be sorted by descending score.
	for i := 1; i < len(plan.Risks); i++ {
		if plan.Risks[i-1].Score < plan.Risks[i].Score {
			t.Fatalf("risk ranking not descending: %v", plan.Risks)
		}
	}
	// Every ready plan carries valve guidance within one bounded step.
	for _, sp := range plan.Stations {
		if len(sp.AllowedOperations) != 1 {
			t.Fatalf("station %s expected one valve operation, got %+v", sp.StationID, sp.AllowedOperations)
		}
		if math.Abs(sp.TargetValvePosition-sp.CurrentValvePosition) > 15.0001 {
			t.Fatalf("valve step exceeds 15%%: %.1f -> %.1f", sp.CurrentValvePosition, sp.TargetValvePosition)
		}
	}
}

func TestZonePlan_MissingKindIsPendingDataAndNotIssuable(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "west", PumpStage: 2, BypassOpen: true}, []string{"nanhu", "xijiao"})
	at := fx.now.Add(-5 * time.Second)
	// nanhu at wm9 lacks flow entirely; xijiao complete but stuck at wm8.
	fx.seedPartial("nanhu", 9, at, map[telemetry.Kind]float64{
		telemetry.KindSupplyPressure: 0.80, telemetry.KindReturnPressure: 0.46, telemetry.KindValvePosition: 70,
	})
	fx.seed("xijiao", 8, at, 0.78, 0.46, 7.8, 68)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "west"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanPendingData || plan.Issuable {
		t.Fatalf("expected pending_data non-issuable, got %s issuable=%v", plan.Status, plan.Issuable)
	}
	if len(plan.Stations) != 0 {
		t.Fatalf("pending plan must not carry recommendations, got %d stations", len(plan.Stations))
	}
	sawFlowGap, sawAlignment := false, false
	for _, gap := range plan.Missing {
		if gap.StationID == "nanhu" {
			for _, kind := range gap.MissingKinds {
				if kind == telemetry.KindFlow {
					sawFlowGap = true
				}
			}
		}
		if gap.StationID == "xijiao" && gap.LocalWatermark == 8 {
			sawAlignment = true
		}
	}
	if !sawFlowGap || !sawAlignment {
		t.Fatalf("expected missing-flow and alignment gaps, got %+v", plan.Missing)
	}
}

func TestZonePlan_StaleSnapshotIsPendingData(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: true}, []string{"xincheng"})
	fx.seed("xincheng", 3, fx.now.Add(-2*time.Minute), 0.82, 0.48, 8.1, 72)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanPendingData || plan.Issuable {
		t.Fatalf("stale snapshot must be pending_data, got %s", plan.Status)
	}
	if len(plan.Missing) != 1 || !plan.Missing[0].Stale {
		t.Fatalf("expected one stale gap, got %+v", plan.Missing)
	}
}

func TestZonePlan_StaleSnapshotFreshWithinCustomMaxAge(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: true}, []string{"xincheng"})
	fx.seed("xincheng", 3, fx.now.Add(-90*time.Second), 0.82, 0.48, 8.1, 72)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north", MaxAge: 2 * time.Minute})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanReady || !plan.Issuable {
		t.Fatalf("snapshot should be fresh within custom max age, got %s %+v", plan.Status, plan.Missing)
	}
}

func TestZonePlan_NegativeDifferentialIsBlocked(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: true}, []string{"xincheng"})
	fx.seed("xincheng", 2, fx.now.Add(-2*time.Second), 0.40, 0.48, 8.1, 72)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanBlocked || plan.Issuable {
		t.Fatalf("negative differential must block, got %s", plan.Status)
	}
	if len(plan.Blockers) != 1 {
		t.Fatalf("expected one blocker, got %+v", plan.Blockers)
	}
}

func TestZonePlan_PumpRaiseWithBypassInterlock(t *testing.T) {
	// Bypass closed: a raise into stage 3 must be rejected by the envelope and
	// surface a bypass.open prerequisite — never an executable stage command.
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: false}, []string{"xincheng"})
	// Flow far above valve/pressure capacity forces desired opening past 0.95.
	fx.seed("xincheng", 4, fx.now.Add(-2*time.Second), 0.82, 0.48, 16.0, 80)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanReady {
		t.Fatalf("expected ready, got %s blockers=%+v", plan.Status, plan.Blockers)
	}
	advice := plan.PumpAdvice
	if advice.Direction != "up" || advice.Suggested != 3 {
		t.Fatalf("expected raise to stage 3, got %+v", advice)
	}
	if advice.Allowed || !advice.BypassRequired {
		t.Fatalf("stage 3 with closed bypass must be disallowed and require bypass, got %+v", advice)
	}
	foundBypass := false
	for _, op := range plan.AllowedOperations {
		if op.Kind == OpBypassOpen && op.Allowed {
			foundBypass = true
		}
		if op.Kind == OpPumpRaise && op.Allowed {
			t.Fatalf("pump raise must not be allowed until bypass is open: %+v", op)
		}
	}
	if !foundBypass {
		t.Fatalf("expected bypass.open prerequisite, got %+v", plan.AllowedOperations)
	}
}

func TestZonePlan_PumpLowerWhenValvesThrottled(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: true}, []string{"xincheng"})
	// Low demand relative to capacity pushes desired opening below 0.35.
	fx.seed("xincheng", 4, fx.now.Add(-2*time.Second), 0.82, 0.48, 1.5, 72)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanReady {
		t.Fatalf("expected ready, got %s", plan.Status)
	}
	if plan.PumpAdvice.Direction != "down" || plan.PumpAdvice.Suggested != 1 || !plan.PumpAdvice.Allowed {
		t.Fatalf("expected allowed lower to stage 1, got %+v", plan.PumpAdvice)
	}
}

func TestZonePlan_StationOfflineBlocks(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: true}, []string{"xincheng"})
	fx.seed("xincheng", 1, fx.now.Add(-2*time.Second), 0.82, 0.48, 8.1, 72)
	fx.planner.stations.(fakeLookup)["xincheng"] = StationState{ID: "xincheng", ZoneID: "north"}

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Status != ZonePlanBlocked || len(plan.Blockers) != 1 {
		t.Fatalf("offline station must block, got %s %+v", plan.Status, plan.Blockers)
	}
	if plan.Issuable {
		t.Fatal("blocked plan must not be issuable")
	}
}

func TestZonePlan_StoredRoundTrip(t *testing.T) {
	fx := newZoneFixture(t, network.Zone{ID: "north", PumpStage: 2, BypassOpen: true}, []string{"xincheng"})
	fx.seed("xincheng", 5, fx.now.Add(-2*time.Second), 0.82, 0.48, 8.1, 72)

	plan, err := fx.planner.Plan(context.Background(), ZonePlanRequest{ZoneID: "north"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	stored, ok := fx.planner.StoredPlan(plan.ID)
	if !ok || stored.Watermark != 5 {
		t.Fatalf("stored plan lookup failed: %+v ok=%v", stored, ok)
	}
	if plans := fx.planner.StoredPlans(); len(plans) != 1 {
		t.Fatalf("expected one stored plan, got %d", len(plans))
	}
}

func TestSharedWatermarkAcrossStations(t *testing.T) {
	series := telemetry.NewSeries(64)
	builder := telemetry.NewSnapshotBuilder(series)
	at := time.Now().UTC()
	add := func(station string, wm uint64) {
		for _, kind := range telemetry.HydraulicKinds {
			if err := series.Add(telemetry.Sample{StationID: station, Kind: kind, Watermark: wm, Value: 1, At: at}); err != nil {
				t.Fatal(err)
			}
		}
	}
	add("a", 1)
	add("a", 2)
	add("b", 1)
	if wm, ok := builder.SharedWatermark([]string{"a", "b"}); !ok || wm != 1 {
		t.Fatalf("expected shared watermark 1, got %d ok=%v", wm, ok)
	}
	if wm, ok := builder.SharedWatermark([]string{"a", "c"}); ok {
		t.Fatalf("station c has no data, got watermark %d", wm)
	}
}
