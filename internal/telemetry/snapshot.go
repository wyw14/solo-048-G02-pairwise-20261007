package telemetry

import (
	"fmt"
	"sort"
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

type SnapshotBuilder struct {
	series *Series
}

func NewSnapshotBuilder(series *Series) *SnapshotBuilder {
	return &SnapshotBuilder{series: series}
}

// HydraulicKinds are the measurements required to reason about a station.
var HydraulicKinds = []Kind{KindSupplyPressure, KindReturnPressure, KindFlow, KindValvePosition}

func (b *SnapshotBuilder) Build(stationID string) (HydraulicSnapshot, error) {
	watermarks := b.series.Watermarks(stationID, HydraulicKinds)
	if len(watermarks) == 0 {
		return HydraulicSnapshot{}, fmt.Errorf("station %s has no aligned hydraulic telemetry", stationID)
	}
	return b.BuildAt(stationID, watermarks[len(watermarks)-1])
}

// BuildAt binds a snapshot to an explicit watermark; every measurement must
// carry that same watermark or the snapshot is refused.
func (b *SnapshotBuilder) BuildAt(stationID string, watermark uint64) (HydraulicSnapshot, error) {
	if watermark == 0 {
		return HydraulicSnapshot{}, fmt.Errorf("station %s has no committed watermark", stationID)
	}
	supply, ok := b.series.At(stationID, KindSupplyPressure, watermark)
	if !ok {
		return HydraulicSnapshot{}, fmt.Errorf("supply pressure missing at %d", watermark)
	}
	ret, ok := b.series.At(stationID, KindReturnPressure, watermark)
	if !ok {
		return HydraulicSnapshot{}, fmt.Errorf("return pressure missing at %d", watermark)
	}
	flow, ok := b.series.At(stationID, KindFlow, watermark)
	if !ok {
		return HydraulicSnapshot{}, fmt.Errorf("flow missing at %d", watermark)
	}
	position, ok := b.series.At(stationID, KindValvePosition, watermark)
	if !ok {
		return HydraulicSnapshot{}, fmt.Errorf("valve position missing at %d", watermark)
	}
	oldest := supply.At
	for _, sample := range []Sample{ret, flow, position} {
		if sample.At.Before(oldest) {
			oldest = sample.At
		}
	}
	if oldest.IsZero() {
		oldest = time.Now().UTC()
	}
	return HydraulicSnapshot{
		StationID: stationID, Watermark: watermark, Supply: supply.Value, Return: ret.Value,
		Flow: flow.Value, ValvePosition: position.Value, CapturedAt: oldest.UTC(),
	}, nil
}

// AlignedWatermarks returns the watermarks at which every station exposes the
// full set of kinds. A zone plan may only bind to one of these.
func (b *SnapshotBuilder) AlignedWatermarks(stationIDs []string, kinds []Kind) []uint64 {
	if len(stationIDs) == 0 {
		return nil
	}
	common := map[uint64]struct{}{}
	first := b.series.Watermarks(stationIDs[0], kinds)
	for _, watermark := range first {
		common[watermark] = struct{}{}
	}
	for _, stationID := range stationIDs[1:] {
		available := b.series.Watermarks(stationID, kinds)
		allowed := make(map[uint64]struct{}, len(available))
		for _, watermark := range available {
			allowed[watermark] = struct{}{}
		}
		for watermark := range common {
			if _, ok := allowed[watermark]; !ok {
				delete(common, watermark)
			}
		}
	}
	result := make([]uint64, 0, len(common))
	for watermark := range common {
		result = append(result, watermark)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// MissingKinds reports which measurements a station lacks at the watermark,
// preserving the canonical kind order so gaps read deterministically.
func (b *SnapshotBuilder) MissingKinds(stationID string, watermark uint64) []Kind {
	missing := make([]Kind, 0)
	for _, kind := range HydraulicKinds {
		if _, ok := b.series.At(stationID, kind, watermark); !ok {
			missing = append(missing, kind)
		}
	}
	return missing
}

// MaxWatermark reports the newest sample watermark observed for a station.
func (b *SnapshotBuilder) MaxWatermark(stationID string) (uint64, bool) {
	return b.series.MaxWatermark(stationID, HydraulicKinds)
}

func (b *SnapshotBuilder) Ready(stationID string) bool {
	_, err := b.Build(stationID)
	return err == nil
}
