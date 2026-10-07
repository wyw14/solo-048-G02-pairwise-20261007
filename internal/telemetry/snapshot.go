package telemetry

import (
	"fmt"
	"time"
)

type HydraulicSnapshot struct {
	StationID     string    `json:"station_id"`
	Watermark     uint64    `json:"watermark"`
	Supply        float64   `json:"supply_pressure"`
	Return        float64   `json:"return_pressure"`
	Flow          float64   `json:"flow"`
	ValvePosition float64   `json:"valve_position"`
	CapturedAt    time.Time `json:"captured_at"`
}

func (s HydraulicSnapshot) DifferentialPressure() float64 {
	return s.Supply - s.Return
}

// HydraulicKinds are the signals that must share one watermark before a
// hydraulic plan can be issued against a station.
var HydraulicKinds = []Kind{KindSupplyPressure, KindReturnPressure, KindFlow, KindValvePosition}

type SnapshotBuilder struct {
	series *Series
}

func NewSnapshotBuilder(series *Series) *SnapshotBuilder {
	return &SnapshotBuilder{series: series}
}

// Build assembles the newest snapshot whose four signals share one watermark.
func (b *SnapshotBuilder) Build(stationID string) (HydraulicSnapshot, error) {
	watermarks := b.series.Watermarks(stationID, HydraulicKinds)
	if len(watermarks) == 0 {
		return HydraulicSnapshot{}, fmt.Errorf("station %s has no aligned hydraulic telemetry", stationID)
	}
	return b.BuildAt(stationID, watermarks[len(watermarks)-1])
}

// BuildAt assembles a snapshot bound to one explicit watermark. CapturedAt is
// the oldest sample timestamp at that watermark, so freshness reflects the
// stalest input rather than wall-clock build time.
func (b *SnapshotBuilder) BuildAt(stationID string, watermark uint64) (HydraulicSnapshot, error) {
	if watermark == 0 {
		return HydraulicSnapshot{}, fmt.Errorf("station %s has no committed watermark", stationID)
	}
	if missing := b.series.MissingAt(stationID, HydraulicKinds, watermark); len(missing) > 0 {
		return HydraulicSnapshot{}, fmt.Errorf("station %s missing %v at watermark %d", stationID, missing, watermark)
	}
	supply, _ := b.series.At(stationID, KindSupplyPressure, watermark)
	ret, _ := b.series.At(stationID, KindReturnPressure, watermark)
	flow, _ := b.series.At(stationID, KindFlow, watermark)
	position, _ := b.series.At(stationID, KindValvePosition, watermark)
	captured := supply.At
	for _, sample := range []Sample{ret, flow, position} {
		if sample.At.Before(captured) {
			captured = sample.At
		}
	}
	if captured.IsZero() {
		captured = time.Now().UTC()
	}
	return HydraulicSnapshot{
		StationID: stationID, Watermark: watermark, Supply: supply.Value, Return: ret.Value,
		Flow: flow.Value, ValvePosition: position.Value, CapturedAt: captured,
	}, nil
}

// SharedWatermark finds the newest watermark aligned across every station in
// the group. Plans bind all station snapshots to this single watermark.
func (b *SnapshotBuilder) SharedWatermark(stationIDs []string) (uint64, bool) {
	return b.series.SharedWatermark(stationIDs, HydraulicKinds)
}

// AlignedWatermarks lists every watermark at which a single station carries
// all four hydraulic signals.
func (b *SnapshotBuilder) AlignedWatermarks(stationID string) []uint64 {
	return b.series.Watermarks(stationID, HydraulicKinds)
}

// AbsentKinds lists hydraulic signals the station has never reported at any
// watermark — used to populate pending-data requests.
func (b *SnapshotBuilder) AbsentKinds(stationID string) []Kind {
	missing := make([]Kind, 0, len(HydraulicKinds))
	for _, kind := range HydraulicKinds {
		if _, ok := b.series.Latest(stationID, kind); !ok {
			missing = append(missing, kind)
		}
	}
	return missing
}

// MissingAt reports which hydraulic signals a station lacks at a watermark.
func (b *SnapshotBuilder) MissingAt(stationID string, watermark uint64) []Kind {
	return b.series.MissingAt(stationID, HydraulicKinds, watermark)
}

// StaleAt reports whether a snapshot bound to watermark is older than maxAge
// relative to now. A non-positive maxAge disables the freshness gate.
func (b *SnapshotBuilder) StaleAt(snapshot HydraulicSnapshot, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 || snapshot.CapturedAt.IsZero() {
		return false
	}
	return now.Sub(snapshot.CapturedAt) > maxAge
}

func (b *SnapshotBuilder) Ready(stationID string) bool {
	_, err := b.Build(stationID)
	return err == nil
}
