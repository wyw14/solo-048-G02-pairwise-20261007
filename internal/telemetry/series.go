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

// SharedWatermark returns the newest watermark at which every station carries a
// sample for every requested kind. A zero result means no aligned snapshot
// exists across the whole group.
func (s *Series) SharedWatermark(stationIDs []string, kinds []Kind) (uint64, bool) {
	if len(stationIDs) == 0 || len(kinds) == 0 {
		return 0, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var shared map[uint64]struct{}
	for _, stationID := range stationIDs {
		perKind := make(map[uint64]int)
		for _, kind := range kinds {
			seen := make(map[uint64]struct{})
			for _, sample := range s.samples[stationID][kind] {
				seen[sample.Watermark] = struct{}{}
			}
			for watermark := range seen {
				perKind[watermark]++
			}
		}
		complete := make(map[uint64]struct{})
		for watermark, count := range perKind {
			if count == len(kinds) {
				complete[watermark] = struct{}{}
			}
		}
		if len(complete) == 0 {
			return 0, false
		}
		if shared == nil {
			shared = complete
			continue
		}
		for watermark := range shared {
			if _, ok := complete[watermark]; !ok {
				delete(shared, watermark)
			}
		}
		if len(shared) == 0 {
			return 0, false
		}
	}
	var newest uint64
	for watermark := range shared {
		if watermark > newest {
			newest = watermark
		}
	}
	return newest, true
}

// MissingAt lists requested kinds that have no sample for the station at the
// given watermark, preserving the order asked for.
func (s *Series) MissingAt(stationID string, kinds []Kind, watermark uint64) []Kind {
	s.mu.RLock()
	defer s.mu.RUnlock()
	missing := make([]Kind, 0)
	for _, kind := range kinds {
		found := false
		for _, sample := range s.samples[stationID][kind] {
			if sample.Watermark == watermark {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, kind)
		}
	}
	return missing
}
