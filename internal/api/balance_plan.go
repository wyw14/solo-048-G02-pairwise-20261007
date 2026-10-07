package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/hydraulic"
	"heatpulse/internal/station"
)

var errZonePlanNotFound = errors.New("zone balance plan not found")

// hydraulicStationLookup adapts *station.Registry to the narrow
// hydraulic.StationLookup interface so hydraulic does not import station.
type hydraulicStationLookup struct {
	registry *station.Registry
}

func (l hydraulicStationLookup) Get(stationID string) (hydraulic.StationState, bool) {
	item, ok := l.registry.Get(stationID)
	if !ok {
		return hydraulic.StationState{}, false
	}
	return hydraulic.StationState{
		ID: item.ID, ZoneID: item.ZoneID, Online: item.Online,
		Maintenance: item.Maintenance, Hold: item.Hold,
	}, true
}

func (a *App) createZoneBalancePlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		StationIDs    []string `json:"station_ids"`
		MaxAgeSeconds int      `json:"max_age_seconds"`
		ZoneTargetDP  float64  `json:"zone_target_dp"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	maxAge := time.Duration(input.MaxAgeSeconds) * time.Second
	plan, err := a.ZonePlanner.Plan(r.Context(), hydraulic.ZonePlanRequest{
		ZoneID:       chi.URLParam(r, "zoneID"),
		StationIDs:   input.StationIDs,
		MaxAge:       maxAge,
		ZoneTargetDP: input.ZoneTargetDP,
	})
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	// Both ready and pending_data plans are returned 202: neither one has
	// dispatched anything. Issuable=false tells the caller no command may run.
	writeJSON(w, http.StatusAccepted, plan)
}

func (a *App) listZoneBalancePlans(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.ZonePlanner.StoredPlans())
}

func (a *App) getZoneBalancePlan(w http.ResponseWriter, r *http.Request) {
	plan, ok := a.ZonePlanner.StoredPlan(chi.URLParam(r, "planID"))
	if !ok {
		writeError(w, http.StatusNotFound, errZonePlanNotFound)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}
