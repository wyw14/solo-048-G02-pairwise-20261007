// Package zoneplan builds zonal hydraulic balance plans bound to a single
// telemetry watermark. A plan is only dispatchable when every station in the
// zone shares one fresh, complete watermark; stale or partial input yields an
// awaiting-data plan with explicit gaps and never a pump or valve command.
package zoneplan

import (
	"time"
)

type Status string

const (
	StatusAwaitingData Status = "awaiting_data"
	StatusReady        Status = "ready"
	StatusBlocked      Status = "blocked"
	StatusApplied      Status = "applied"
	StatusRejected     Status = "rejected"
	StatusFailed       Status = "failed"
	StatusSuperseded   Status = "superseded"
)

// Terminal reports whether a plan can no longer be applied.
func (s Status) Terminal() bool {
	switch s {
	case StatusApplied, StatusRejected, StatusFailed, StatusSuperseded:
		return true
	default:
		return false
	}
}

type GapCode string

const (
	GapNoTelemetry        GapCode = "no_telemetry"
	GapMeasurementMissing GapCode = "measurement_missing"
	GapNoAlignedWatermark GapCode = "no_aligned_watermark"
	GapStaleWatermark     GapCode = "stale_watermark"
	GapMeasurementInvalid GapCode = "measurement_invalid"
)

// InputGap describes one piece of evidence that must be supplied before a
// plan can carry commands.
type InputGap struct {
	StationID  string    `json:"station_id,omitempty"`
	Code       GapCode   `json:"code"`
	Kind       string    `json:"kind,omitempty"`
	Watermark  uint64    `json:"watermark,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
	Message    string    `json:"message"`
}

type RiskCode string

const (
	RiskLeakSuspected  RiskCode = "leak_suspected"
	RiskHighImbalance  RiskCode = "high_imbalance"
	RiskLowDP          RiskCode = "low_differential_pressure"
	RiskFlowDeficit    RiskCode = "flow_deficit"
	RiskFlowSurplus    RiskCode = "flow_surplus"
	RiskStationHeld    RiskCode = "station_held"
	RiskStageAtCeiling RiskCode = "pump_stage_at_ceiling"
	RiskStageAtFloor   RiskCode = "pump_stage_at_floor"
	RiskValveFullyOpen RiskCode = "control_valve_saturated_open"
)

// Risk is ranked worst-first so operators action the top entry first.
type Risk struct {
	Rank      int      `json:"rank"`
	StationID string   `json:"station_id,omitempty"`
	Code      RiskCode `json:"code"`
	Severity  float64  `json:"severity"`
	Message   string   `json:"message"`
}

type StationMeasure struct {
	StationID            string  `json:"station_id"`
	Watermark            uint64  `json:"watermark"`
	Online               bool    `json:"online"`
	Held                 bool    `json:"held"`
	SupplyPressure       float64 `json:"supply_pressure"`
	ReturnPressure       float64 `json:"return_pressure"`
	DifferentialPressure float64 `json:"differential_pressure"`
	MeasuredFlow         float64 `json:"measured_flow"`
	ExpectedFlow         float64 `json:"expected_flow"`
	TargetFlow           float64 `json:"target_flow"`
	FlowDelta            float64 `json:"flow_delta"`
	FlowDeltaRatio       float64 `json:"flow_delta_ratio"`
	ValvePosition        float64 `json:"valve_position"`
	RequiredOpening      float64 `json:"required_opening"`
	SuggestedPosition    float64 `json:"suggested_position"`
	ImbalanceRatio       float64 `json:"imbalance_ratio"`
	LeakSuspected        bool    `json:"leak_suspected"`
	ControlValveID       string  `json:"control_valve_id,omitempty"`
}

type ActionType string

const (
	ActionBypass    ActionType = "bypass"
	ActionPumpStage ActionType = "pump_stage"
	ActionValveTrim ActionType = "valve_trim"
)

// ProposedOperation is a candidate command. Allowed is false whenever the
// gate conditions for that command are not met; the executor will not issue
// anything from a non-dispatchable plan regardless.
type ProposedOperation struct {
	Type      ActionType `json:"type"`
	Step      int        `json:"step"`
	ZoneID    string     `json:"zone_id,omitempty"`
	StationID string     `json:"station_id,omitempty"`
	ValveID   string     `json:"valve_id,omitempty"`
	From      float64    `json:"from"`
	To        float64    `json:"to"`
	Allowed   bool       `json:"allowed"`
	Reason    string     `json:"reason,omitempty"`
}

type Plan struct {
	ID           string              `json:"id"`
	ZoneID       string              `json:"zone_id"`
	Watermark    uint64              `json:"watermark"`
	Status       Status              `json:"status"`
	Dispatchable bool                `json:"dispatchable"`
	CurrentStage int                 `json:"current_stage"`
	TargetStage  int                 `json:"target_stage"`
	BypassOpen   bool                `json:"bypass_open"`
	BypassNeeded bool                `json:"bypass_needed"`
	Measures     []StationMeasure    `json:"measures"`
	Operations   []ProposedOperation `json:"operations"`
	Risks        []Risk              `json:"risks"`
	Gaps         []InputGap          `json:"gaps"`
	CreatedAt    time.Time           `json:"created_at"`
	BoundAt      time.Time           `json:"bound_at"`
	ExpiresAt    time.Time           `json:"expires_at"`
	AppliedAt    time.Time           `json:"applied_at,omitempty"`
	AppliedOps   []ProposedOperation `json:"applied_operations,omitempty"`
	Message      string              `json:"message,omitempty"`
}

func (p Plan) Clone() Plan {
	copy := p
	copy.Measures = append([]StationMeasure(nil), p.Measures...)
	copy.Operations = append([]ProposedOperation(nil), p.Operations...)
	copy.AppliedOps = append([]ProposedOperation(nil), p.AppliedOps...)
	copy.Risks = append([]Risk(nil), p.Risks...)
	copy.Gaps = append([]InputGap(nil), p.Gaps...)
	return copy
}

// Options tunes the balancing heuristics and the safety envelope.
type Options struct {
	MaxStaleness   time.Duration
	FlowTolerance  float64 // fraction of target flow inside which no trim is issued
	LeakThreshold  float64
	LowDPThreshold float64
	DesignOpening  float64 // valve opening the design target flow is based on
	ValveMin       float64
	ValveMax       float64
	ValveDeadband  float64 // percentage points
	ValveStep      float64 // max single-command trim, percentage points
	StageGain      float64 // flow multiplier gained per pump stage (sqrt(dP) space)
	MinStage       int
	MaxStage       int
}

func DefaultOptions() Options {
	return Options{
		MaxStaleness:   90 * time.Second,
		FlowTolerance:  0.08,
		LeakThreshold:  0.28,
		LowDPThreshold: 0.15,
		DesignOpening:  0.70,
		ValveMin:       10,
		ValveMax:       95,
		ValveDeadband:  1.0,
		ValveStep:      10,
		StageGain:      0.075,
		MinStage:       1,
		MaxStage:       4,
	}
}
