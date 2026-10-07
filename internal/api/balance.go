package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (a *App) createBalancePlan(w http.ResponseWriter, r *http.Request) {
	plan, err := a.BalancePlanner.Generate(chi.URLParam(r, "zoneID"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	// Awaiting-data and blocked plans are normal responses, not errors: the
	// body carries the gaps or risks an operator must act on. No command is
	// ever sent from this endpoint.
	writeJSON(w, http.StatusCreated, plan)
}

func (a *App) listBalancePlans(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.BalancePlanner.Plans())
}

func (a *App) getBalancePlan(w http.ResponseWriter, r *http.Request) {
	plan, ok := a.BalancePlanner.Plan(chi.URLParam(r, "planID"))
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("balance plan %s not found", chi.URLParam(r, "planID")))
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *App) applyBalancePlan(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if input.Actor == "" {
		input.Actor = "operator"
	}
	plan, err := a.BalanceExecutor.Apply(r.Context(), chi.URLParam(r, "planID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}
