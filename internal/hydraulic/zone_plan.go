package hydraulic

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/network"
	"heatpulse/internal/telemetry"
)

// ZonePlanStatus describes how far a plan got. Only ZonePlanReady carries
// operational guidance; pending_data and blocked plans are advisory and must
// never be turned into pump or valve commands.
type ZonePlanStatus string

const (
	ZonePlanPendingData ZonePlanStatus = "pending_data"
	ZonePlanBlocked     ZonePlanStatus = "blocked"
	ZonePlanReady       ZonePlanStatus = "ready"
)

// Operation kinds surfaced as guidance. The planner never executes any of
// these; callers must route accepted operations through the interlock-backed
// pump/valve services themselves.
const (
	OpValveOpen  = "valve.open"
	OpValveClose = "valve.close"
	OpValveHold  = "valve.hold"
	OpPumpRaise  = "pump.raise_stage"
	OpPumpLower  = "pump.lower_stage"
	OpBypassOpen = "bypass.open"
)

const (
	RiskLow    = "low"
	RiskMedium = "medium"
	RiskHigh   = "high"
)

// ZonePlanRequest scopes a plan to one zone. An empty StationIDs list expands
// to every required station binding for the zone.
type ZonePlanRequest struct {
	ZoneID       string        `json:"zone_id"`
	StationIDs   []string      `json:"station_ids,omitempty"`
	MaxAge       time.Duration `json:"-"`
	ZoneTargetDP float64       `json:"zone_target_dp,omitempty"`
}

// AllowedOperation is one guidance entry. Allowed=false means the interlock or
// current measurements reject the move and Reason explains why.
type AllowedOperation struct {
	Kind    string `json:"kind"`
	Target  string `json:"target,omitempty"`
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// RiskRank is the per-station ordering key. Higher score is handled first.
type RiskRank struct {
	StationID string             `json:"station_id"`
	Score     float64            `json:"score"`
	Level     string             `json:"level"`
	Factors   map[string]float64 `json:"factors"`
	Reasons   []string           `json:"reasons,omitempty"`
}

// StationPlan binds every recommendation to the single watermarked snapshot it
// was derived from.
type StationPlan struct {
	StationID            string                      `json:"station_id"`
	Snapshot             telemetry.HydraulicSnapshot `json:"snapshot"`
	DifferentialPressure float64                     `json:"differential_pressure"`
	CurrentFlow          float64                     `json:"current_flow"`
	TargetFlow           float64                     `json:"target_flow"`
	HydraulicCapacity    float64                     `json:"hydraulic_capacity"`
	CurrentValvePosition float64                     `json:"current_valve_position"`
	DesiredValvePosition float64                     `json:"desired_valve_position"`
	TargetValvePosition  float64                     `json:"target_valve_position"`
	FlowDeviation        float64                     `json:"flow_deviation"`
	ImbalanceRatio       float64                     `json:"imbalance_ratio"`
	LeakLikely           bool                        `json:"leak_likely"`
	AllowedOperations    []AllowedOperation          `json:"allowed_operations"`
	Risk                 RiskRank                    `json:"risk"`
}

// PumpStageAdvice is zone-level pump guidance checked against the interlock
// envelope (stages 0-4, bypass open for stages >= 3).
type PumpStageAdvice struct {
	Current        int       `json:"current"`
	Suggested      int       `json:"suggested"`
	Direction      string    `json:"direction"` // up, down, hold
	Allowed        bool      `json:"allowed"`
	BypassRequired bool      `json:"bypass_required"`
	Reason         string    `json:"reason"`
	CapturedAt     time.Time `json:"captured_at"`
}

// MissingData is a补数据 request: exactly what the operator must refresh
// before a plan can become ready. It never accompanies a command.
type MissingData struct {
	StationID      string           `json:"station_id"`
	LocalWatermark uint64           `json:"local_watermark,omitempty"`
	MissingKinds   []telemetry.Kind `json:"missing_kinds,omitempty"`
	Stale          bool             `json:"stale,omitempty"`
	Detail         string           `json:"detail"`
}

// Blocker marks input that is present but unusable (offline station,
// non-physical reading).
type Blocker struct {
	StationID string `json:"station_id"`
	Reason    string `json:"reason"`
}

// ZonePlan is the full result. Issuable is true only when Status is ready and
// every station snapshot shares Watermark and is fresh.
type ZonePlan struct {
	ID                string             `json:"id"`
	ZoneID            string             `json:"zone_id"`
	Watermark         uint64             `json:"watermark"`
	Status            ZonePlanStatus     `json:"status"`
	Issuable          bool               `json:"issuable"`
	MaxAge            time.Duration      `json:"max_age"`
	ZoneTargetDP      float64            `json:"zone_target_dp"`
	CurrentPumpStage  int                `json:"current_pump_stage"`
	GeneratedAt       time.Time          `json:"generated_at"`
	Stations          []StationPlan      `json:"stations,omitempty"`
	Missing           []MissingData      `json:"missing_data,omitempty"`
	Blockers          []Blocker          `json:"blockers,omitempty"`
	PumpAdvice        PumpStageAdvice    `json:"pump_advice"`
	AllowedOperations []AllowedOperation `json:"zone_allowed_operations,omitempty"`
	Risks             []RiskRank         `json:"risk_ranking,omitempty"`
}

// StationState is the narrow station view the planner needs. The wiring layer
// adapts the full station registry to it, keeping hydraulic free of an import
// on the station package (that direction would cycle through valve).
type StationState struct {
	ID          string `json:"id"`
	ZoneID      string `json:"zone_id"`
	Online      bool   `json:"online"`
	Maintenance bool   `json:"maintenance"`
	Hold        bool   `json:"hold"`
}

// StationLookup resolves station state for the planner.
type StationLookup interface {
	Get(stationID string) (StationState, bool)
}

type zonePlannerConfig struct {
	maxAge           time.Duration
	valveDeadband    float64
	maxValveStep     float64
	stageUpOpening   float64
	stageDownOpening float64
	now              func() time.Time
}

// ZoneBalancePlanner computes zonal hydraulic balance plans. It holds no
// reference to the pump/valve command services: its outputs are guidance
// bound to one telemetry watermark, never dispatched commands.
type ZoneBalancePlanner struct {
	builder   *telemetry.SnapshotBuilder
	bindings  *network.Registry
	stations  StationLookup
	interlock *Interlock
	balancer  *Balancer
	cfg       zonePlannerConfig
	mu        sync.RWMutex
	plans     map[string]ZonePlan
}

func NewZoneBalancePlanner(
	builder *telemetry.SnapshotBuilder,
	bindings *network.Registry,
	stations StationLookup,
	interlock *Interlock,
	balancer *Balancer,
) *ZoneBalancePlanner {
	if balancer == nil {
		balancer = NewBalancer(0)
	}
	return &ZoneBalancePlanner{
		builder:   builder,
		bindings:  bindings,
		stations:  stations,
		interlock: interlock,
		balancer:  balancer,
		plans:     make(map[string]ZonePlan),
		cfg: zonePlannerConfig{
			maxAge:           30 * time.Second,
			valveDeadband:    2,
			maxValveStep:     15,
			stageUpOpening:   0.95,
			stageDownOpening: 0.35,
			now:              func() time.Time { return time.Now().UTC() },
		},
	}
}

// WithMaxAge overrides the snapshot freshness window (non-positive disables it).
func (p *ZoneBalancePlanner) WithMaxAge(maxAge time.Duration) *ZoneBalancePlanner {
	p.cfg.maxAge = maxAge
	return p
}

// Plan builds the plan for a zone, stores it, and returns it. Incomplete,
// misaligned or stale input does not error: it returns a pending_data plan
// listing exactly what to refresh. Such a plan is stored with Issuable=false
// and exposes no executable commands.
func (p *ZoneBalancePlanner) Plan(ctx context.Context, req ZonePlanRequest) (ZonePlan, error) {
	plan, err := p.compute(ctx, req)
	if err != nil {
		return ZonePlan{}, err
	}
	p.mu.Lock()
	p.plans[plan.ID] = plan
	p.mu.Unlock()
	return plan, nil
}

// StoredPlan returns a previously generated plan.
func (p *ZoneBalancePlanner) StoredPlan(id string) (ZonePlan, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	plan, ok := p.plans[id]
	return plan, ok
}

// StoredPlans returns every generated plan, newest first.
func (p *ZoneBalancePlanner) StoredPlans() []ZonePlan {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]ZonePlan, 0, len(p.plans))
	for _, plan := range p.plans {
		result = append(result, plan)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].GeneratedAt.After(result[j].GeneratedAt) })
	return result
}

func (p *ZoneBalancePlanner) compute(ctx context.Context, req ZonePlanRequest) (ZonePlan, error) {
	if err := ctx.Err(); err != nil {
		return ZonePlan{}, err
	}
	if req.ZoneID == "" {
		return ZonePlan{}, fmt.Errorf("zone id is required")
	}
	now := p.cfg.now()
	maxAge := req.MaxAge
	if maxAge == 0 {
		maxAge = p.cfg.maxAge
	}
	plan := ZonePlan{
		ID: uuid.NewString(), ZoneID: req.ZoneID, Status: ZonePlanPendingData,
		MaxAge: maxAge, GeneratedAt: now,
	}

	stationIDs, err := p.resolveStations(req)
	if err != nil {
		return ZonePlan{}, err
	}

	// Gate 1: every station must have a four-signal aligned watermark.
	missing := p.collectAlignmentGaps(stationIDs)

	// Gate 2: all stations must share one watermark; the whole plan binds to it.
	shared, sharedOK := p.builder.SharedWatermark(stationIDs)
	if sharedOK {
		plan.Watermark = shared
	} else {
		for _, id := range stationIDs {
			aligned := p.builder.AlignedWatermarks(id)
			if len(aligned) == 0 {
				continue // already reported by collectAlignmentGaps
			}
			local := aligned[len(aligned)-1]
			missing = append(missing, MissingData{
				StationID: id, LocalWatermark: local,
				Detail: fmt.Sprintf("station aligned at watermark %d; waiting for a watermark shared by every zone station", local),
			})
		}
	}
	if len(missing) > 0 {
		plan.Missing = missing
		return plan, nil
	}

	// Gate 3: snapshots bound to that watermark must be complete and fresh.
	snapshots := make(map[string]telemetry.HydraulicSnapshot, len(stationIDs))
	for _, id := range stationIDs {
		snap, buildErr := p.builder.BuildAt(id, shared)
		if buildErr != nil {
			plan.Missing = []MissingData{{StationID: id, LocalWatermark: shared, Detail: buildErr.Error()}}
			return plan, nil
		}
		if p.builder.StaleAt(snap, now, maxAge) {
			plan.Missing = append(plan.Missing, MissingData{
				StationID: id, LocalWatermark: shared, Stale: true,
				Detail: fmt.Sprintf("snapshot captured %s is older than max age %s", snap.CapturedAt.Format(time.RFC3339), maxAge),
			})
		}
		snapshots[id] = snap
	}
	if len(plan.Missing) > 0 {
		return plan, nil
	}

	// Current pump envelope is read-only state, recorded for traceability.
	candidate, err := p.interlock.Snapshot(req.ZoneID)
	if err != nil {
		return ZonePlan{}, err
	}
	plan.CurrentPumpStage = candidate.PumpStage
	plan.PumpAdvice = PumpStageAdvice{Current: candidate.PumpStage, Suggested: candidate.PumpStage, Direction: "hold", CapturedAt: now}

	// Gate 4: present-but-unusable input (station state or non-physical values).
	for _, id := range stationIDs {
		snap := snapshots[id]
		if reason, bad := p.stationBlocker(id, snap); bad {
			plan.Blockers = append(plan.Blockers, Blocker{StationID: id, Reason: reason})
		}
	}
	if len(plan.Blockers) > 0 {
		plan.Status = ZonePlanBlocked
		return plan, nil
	}

	p.fillRecommendations(&plan, req, stationIDs, snapshots, candidate, now)
	return plan, nil
}

func (p *ZoneBalancePlanner) resolveStations(req ZonePlanRequest) ([]string, error) {
	var ids []string
	if len(req.StationIDs) > 0 {
		ids = append(ids, req.StationIDs...)
	} else {
		ids = p.bindings.RequiredForZone(req.ZoneID)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("zone %s has no bound stations", req.ZoneID)
	}
	sort.Strings(ids)
	uniq := ids[:0]
	for _, id := range ids {
		if len(uniq) > 0 && uniq[len(uniq)-1] == id {
			continue
		}
		item, ok := p.stations.Get(id)
		if !ok {
			return nil, fmt.Errorf("station %s not found", id)
		}
		if item.ZoneID != req.ZoneID {
			return nil, fmt.Errorf("station %s belongs to zone %s, not %s", id, item.ZoneID, req.ZoneID)
		}
		uniq = append(uniq, id)
	}
	return uniq, nil
}

func (p *ZoneBalancePlanner) collectAlignmentGaps(stationIDs []string) []MissingData {
	var gaps []MissingData
	for _, id := range stationIDs {
		aligned := p.builder.AlignedWatermarks(id)
		if len(aligned) == 0 {
			gap := MissingData{StationID: id, Detail: "no aligned hydraulic watermark; supply, return, flow and valve position must share one watermark"}
			gap.MissingKinds = p.builder.AbsentKinds(id)
			gaps = append(gaps, gap)
		}
	}
	return gaps
}

func (p *ZoneBalancePlanner) stationBlocker(id string, snap telemetry.HydraulicSnapshot) (string, bool) {
	item, ok := p.stations.Get(id)
	if !ok {
		return "station not registered", true
	}
	switch {
	case !item.Online:
		return "station offline", true
	case item.Maintenance:
		return "station in maintenance", true
	case item.Hold:
		return "station manually held", true
	}
	switch {
	case snap.Supply <= 0 || snap.Return < 0:
		return fmt.Sprintf("non-physical pressures supply=%.3f return=%.3f", snap.Supply, snap.Return), true
	case snap.DifferentialPressure() <= 0:
		return fmt.Sprintf("differential pressure %.3f not positive", snap.DifferentialPressure()), true
	case snap.ValvePosition < 0 || snap.ValvePosition > 100:
		return fmt.Sprintf("valve position %.1f outside 0-100", snap.ValvePosition), true
	case snap.Flow < 0:
		return fmt.Sprintf("negative flow %.3f", snap.Flow), true
	}
	return "", false
}

func (p *ZoneBalancePlanner) fillRecommendations(
	plan *ZonePlan,
	req ZonePlanRequest,
	stationIDs []string,
	snapshots map[string]telemetry.HydraulicSnapshot,
	candidate Candidate,
	now time.Time,
) {
	k := 18.0
	dps := make([]float64, 0, len(stationIDs))
	openings := make(map[string]float64, len(stationIDs))
	balances := make(map[string]Balance, len(stationIDs))
	totalDemand := 0.0
	for _, id := range stationIDs {
		snap := snapshots[id]
		opening := math.Max(snap.ValvePosition/100, 0.05)
		openings[id] = opening
		dps = append(dps, snap.DifferentialPressure())
		totalDemand += snap.Flow
		balance, err := p.balancer.Evaluate(snap)
		if err != nil {
			plan.Status = ZonePlanBlocked
			plan.Blockers = append(plan.Blockers, Blocker{StationID: id, Reason: err.Error()})
			return
		}
		balances[id] = balance
	}

	targetDP := req.ZoneTargetDP
	if targetDP <= 0 {
		targetDP = median(dps)
	}
	plan.ZoneTargetDP = targetDP

	totalCapacity := 0.0
	for _, id := range stationIDs {
		totalCapacity += math.Sqrt(targetDP) * openings[id] * k
	}
	if totalCapacity <= 0 {
		plan.Status = ZonePlanBlocked
		plan.Blockers = append(plan.Blockers, Blocker{Reason: "zone has no hydraulic capacity at target differential pressure"})
		return
	}

	stationPlans := make([]StationPlan, 0, len(stationIDs))
	needRaise := false
	canLower := true
	for _, id := range stationIDs {
		snap := snapshots[id]
		dp := snap.DifferentialPressure()
		share := (math.Sqrt(targetDP) * openings[id] * k) / totalCapacity
		targetFlow := totalDemand * share
		desiredOpening := targetFlow / (math.Sqrt(targetDP) * k)
		desiredPos := desiredOpening * 100
		if desiredOpening > p.cfg.stageUpOpening {
			needRaise = true
		}
		if desiredOpening >= p.cfg.stageDownOpening {
			canLower = false
		}
		targetPos := stepLimitedPosition(snap.ValvePosition, desiredPos, p.cfg.valveDeadband, p.cfg.maxValveStep)

		balance := balances[id]
		flowDeviation := targetFlow - snap.Flow
		risk := p.rankRisk(id, balance, targetFlow, desiredOpening, dp, targetDP)

		ops := p.valveOperations(snap.ValvePosition, targetPos, desiredPos)
		stationPlans = append(stationPlans, StationPlan{
			StationID: id, Snapshot: snap, DifferentialPressure: dp,
			CurrentFlow: snap.Flow, TargetFlow: targetFlow, HydraulicCapacity: balance.ExpectedFlow,
			CurrentValvePosition: snap.ValvePosition, DesiredValvePosition: clamp(desiredPos, 5, 100),
			TargetValvePosition: targetPos, FlowDeviation: flowDeviation,
			ImbalanceRatio: balance.ImbalanceRatio, LeakLikely: balance.LeakLikely,
			AllowedOperations: ops, Risk: risk,
		})
	}

	sort.SliceStable(stationPlans, func(i, j int) bool {
		if stationPlans[i].Risk.Score != stationPlans[j].Risk.Score {
			return stationPlans[i].Risk.Score > stationPlans[j].Risk.Score
		}
		if stationPlans[i].FlowDeviation != stationPlans[j].FlowDeviation {
			return math.Abs(stationPlans[i].FlowDeviation) > math.Abs(stationPlans[j].FlowDeviation)
		}
		return stationPlans[i].StationID < stationPlans[j].StationID
	})

	advice, zoneOps := p.pumpGuidance(candidate, needRaise, canLower, now)
	plan.PumpAdvice = advice
	plan.AllowedOperations = zoneOps
	plan.Stations = stationPlans
	plan.Risks = make([]RiskRank, 0, len(stationPlans))
	for _, sp := range stationPlans {
		plan.Risks = append(plan.Risks, sp.Risk)
	}
	plan.Status = ZonePlanReady
	plan.Issuable = true
}

func (p *ZoneBalancePlanner) valveOperations(current, target, desired float64) []AllowedOperation {
	delta := target - current
	if math.Abs(desired-current) <= p.cfg.valveDeadband {
		return []AllowedOperation{{
			Kind: OpValveHold, Allowed: true,
			Reason: fmt.Sprintf("target within %.0f%% valve deadband", p.cfg.valveDeadband),
		}}
	}
	stepReason := fmt.Sprintf("step limited to %.0f%% per plan; final target %.1f%%", p.cfg.maxValveStep, clamp(desired, 5, 100))
	if delta > 0 {
		return []AllowedOperation{{
			Kind: OpValveOpen, Target: fmt.Sprintf("%.1f", target), Allowed: true, Reason: stepReason,
		}}
	}
	return []AllowedOperation{{
		Kind: OpValveClose, Target: fmt.Sprintf("%.1f", target), Allowed: true, Reason: stepReason,
	}}
}

func (p *ZoneBalancePlanner) pumpGuidance(candidate Candidate, needRaise, canLower bool, now time.Time) (PumpStageAdvice, []AllowedOperation) {
	advice := PumpStageAdvice{
		Current: candidate.PumpStage, Suggested: candidate.PumpStage,
		Direction: "hold", Allowed: true, Reason: "current pump stage adequate", CapturedAt: now,
	}
	ops := []AllowedOperation{
		{Kind: OpPumpRaise, Allowed: false, Reason: "no station is saturation-limited"},
		{Kind: OpPumpLower, Allowed: false, Reason: "at least one station still needs opening"},
	}
	switch {
	case needRaise && candidate.PumpStage >= 4:
		advice.Reason = "station saturated above 95% opening but pump already at stage 4; relieve via valve/load, not a pump raise"
	case needRaise && candidate.PumpStage < 4:
		advice.Direction = "up"
		advice.Suggested = candidate.PumpStage + 1
		advice.Reason = "a station needs >95% opening to carry its target flow"
	case canLower && candidate.PumpStage > 0 && !needRaise:
		advice.Direction = "down"
		advice.Suggested = candidate.PumpStage - 1
		advice.Reason = "all stations balance below 35% opening; pump head can be reduced"
	default:
		return advice, ops
	}
	if advice.Direction == "" {
		return advice, ops
	}
	if err := Validate(Candidate{PumpStage: advice.Suggested, BypassOpen: candidate.BypassOpen}); err != nil {
		advice.Allowed = false
		advice.BypassRequired = advice.Suggested >= 3 && !candidate.BypassOpen
		advice.Reason = fmt.Sprintf("interlock rejects stage %d: %s", advice.Suggested, err)
	}
	kind := OpPumpLower
	if advice.Direction == "up" {
		kind = OpPumpRaise
	}
	ops = []AllowedOperation{{Kind: kind, Allowed: advice.Allowed, Reason: advice.Reason}}
	if advice.BypassRequired {
		ops = append(ops, AllowedOperation{Kind: OpBypassOpen, Allowed: true, Reason: "open bypass before pump stage reaches 3"})
	}
	return advice, ops
}

func (p *ZoneBalancePlanner) rankRisk(id string, balance Balance, targetFlow, desiredOpening, dp, targetDP float64) RiskRank {
	imbalance := math.Min(balance.ImbalanceRatio, 1) * 45
	deviation := math.Min(math.Abs(targetFlow-balance.MeasuredFlow)/math.Max(targetFlow, 1), 1) * 25
	saturation := 0.0
	if desiredOpening > 0.95 || desiredOpening < 0.05 {
		saturation = 15
	}
	inequity := 0.0
	if targetDP > 0 {
		inequity = math.Min(math.Abs(dp-targetDP)/targetDP, 1) * 15
	}
	score := imbalance + deviation + saturation + inequity
	factors := map[string]float64{
		"imbalance":            imbalance,
		"flow_deviation":       deviation,
		"valve_saturation":     saturation,
		"pressure_inequity":    inequity,
		"imbalance_ratio":      balance.ImbalanceRatio,
		"flow_deviation_ratio": math.Abs(targetFlow-balance.MeasuredFlow) / math.Max(targetFlow, 1),
	}
	reasons := make([]string, 0)
	if balance.LeakLikely {
		score = math.Max(score, 85)
		reasons = append(reasons, "leak likely: imbalance over threshold")
	}
	if saturation > 0 {
		reasons = append(reasons, "valve saturated at operating envelope")
	}
	if imbalance >= 20 {
		reasons = append(reasons, fmt.Sprintf("measured flow deviates %.0f%% from valve/pressure capacity", balance.ImbalanceRatio*100))
	}
	level := RiskLow
	switch {
	case score >= 70:
		level = RiskHigh
	case score >= 35:
		level = RiskMedium
	}
	return RiskRank{StationID: id, Score: math.Round(score*10) / 10, Level: level, Factors: factors, Reasons: reasons}
}

func stepLimitedPosition(current, desired, deadband, maxStep float64) float64 {
	if math.Abs(desired-current) <= deadband {
		return clamp(current, 5, 100)
	}
	if desired > current {
		return clamp(current+math.Min(desired-current, maxStep), 5, 100)
	}
	return clamp(current-math.Min(current-desired, maxStep), 5, 100)
}

func clamp(v, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, v))
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}
