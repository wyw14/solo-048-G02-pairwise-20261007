package telemetry

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type Kind string

const (
	KindSupplyPressure Kind = "supply_pressure"
	KindReturnPressure Kind = "return_pressure"
	KindFlow           Kind = "flow"
	KindValvePosition  Kind = "valve_position"
	KindTemperature    Kind = "temperature"
)

type Sample struct {
	StationID string    `json:"station_id"`
	Kind      Kind      `json:"kind"`
	Watermark uint64    `json:"watermark"`
	Value     float64   `json:"value"`
	At        time.Time `json:"at"`
}

type Series struct {
	mu      sync.RWMutex
	samples map[string]map[Kind][]Sample
	limit   int
}

func NewSeries(limit int) *Series {
	if limit < 4 {
		limit = 64
	}
	return &Series{samples: make(map[string]map[Kind][]Sample), limit: limit}
}

func (s *Series) Add(sample Sample) error {
	if sample.StationID == "" || sample.Kind == "" || sample.Watermark == 0 {
		return fmt.Errorf("station, kind and watermark are required")
	}
	if sample.At.IsZero() {
		sample.At = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.samples[sample.StationID] == nil {
		s.samples[sample.StationID] = make(map[Kind][]Sample)
	}
	items := append(s.samples[sample.StationID][sample.Kind], sample)
	sort.Slice(items, func(i, j int) bool { return items[i].Watermark < items[j].Watermark })
	if len(items) > s.limit {
		items = append([]Sample(nil), items[len(items)-s.limit:]...)
	}
	s.samples[sample.StationID][sample.Kind] = items
	return nil
}

func (s *Series) Latest(stationID string, kind Kind) (Sample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := s.samples[stationID][kind]
	if len(items) == 0 {
		return Sample{}, false
	}
	return items[len(items)-1], true
}

func (s *Series) At(stationID string, kind Kind, watermark uint64) (Sample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := s.samples[stationID][kind]
	index := sort.Search(len(items), func(i int) bool { return items[i].Watermark >= watermark })
	if index >= len(items) || items[index].Watermark != watermark {
		return Sample{}, false
	}
	return items[index], true
}

// MaxWatermark returns the highest watermark carried by any sample for the
// station across the given kinds, and whether the station has data at all.
func (s *Series) MaxWatermark(stationID string, kinds []Kind) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var max uint64
	found := false
	for _, kind := range kinds {
		for _, sample := range s.samples[stationID][kind] {
			found = true
			if sample.Watermark > max {
				max = sample.Watermark
			}
		}
	}
	return max, found
}

func (s *Series) Watermarks(stationID string, kinds []Kind) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := make(map[uint64]int)
	for _, kind := range kinds {
		seen := make(map[uint64]struct{})
		for _, sample := range s.samples[stationID][kind] {
			seen[sample.Watermark] = struct{}{}
		}
		for watermark := range seen {
			counts[watermark]++
		}
	}
	common := make([]uint64, 0)
	for watermark, count := range counts {
		if count == len(kinds) {
			common = append(common, watermark)
		}
	}
	sort.Slice(common, func(i, j int) bool { return common[i] < common[j] })
	return common
}
