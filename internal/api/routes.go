package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"heatpulse/internal/console"
)

func (a *App) Handler() http.Handler {
	router := chi.NewRouter()
	router.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer)
	router.Get("/healthz", a.health)
	console.Register(router)
	router.Route("/api/v1", func(api chi.Router) {
		api.Get("/overview", a.overview)
		api.Get("/zones", a.listZones)
		api.Post("/zones/{zoneID}/pump-stage", a.setPumpStage)
		api.Post("/zones/{zoneID}/bypass", a.setBypass)
		api.Post("/zones/{zoneID}/recovery", a.beginZoneRecovery)
		api.Post("/zones/{zoneID}/recovery/connect", a.connectRecoveryStation)
		api.Post("/zones/{zoneID}/recovery/proofs", a.recordRecoveryProofs)
		api.Get("/stations", a.listStations)
		api.Post("/stations/{stationID}/exchangers/switch", a.beginExchangerSwitch)
		api.Post("/stations/{stationID}/exchangers/flow", a.recordFlowReceipt)
		api.Post("/stations/{stationID}/exchangers/switch/{operationID}/advance", a.advanceExchangerSwitch)
		api.Post("/stations/{stationID}/antifreeze", a.startAntifreeze)
		api.Delete("/stations/{stationID}/antifreeze", a.stopAntifreeze)
		api.Post("/stations/{stationID}/manual-valve", a.startManualValve)
		api.Delete("/stations/{stationID}/manual-valve/{operationID}", a.cancelManualValve)
		api.Post("/stations/{stationID}/manual-valve/{operationID}/fail", a.failManualValve)
		api.Post("/stations/{stationID}/frost-valve", a.openFrostValve)
		api.Post("/stations/{stationID}/maintenance", a.enterMaintenance)
		api.Delete("/stations/{stationID}/maintenance", a.exitMaintenance)
		api.Post("/stations/{stationID}/latches", a.applyLatch)
		api.Delete("/stations/{stationID}/latches", a.resetLatches)
		api.Post("/stations/{stationID}/reconnect", a.reconnectStation)
		api.Post("/telemetry", a.recordTelemetry)
		api.Post("/stations/{stationID}/hydraulic/evaluate", a.evaluateHydraulic)
		api.Get("/zones/balance-plans", a.listZoneBalancePlans)
		api.Post("/zones/{zoneID}/balance-plans", a.createZoneBalancePlan)
		api.Get("/zones/balance-plans/{planID}", a.getZoneBalancePlan)
		api.Get("/incidents", a.listIncidents)
		api.Delete("/incidents/{incidentID}", a.resolveIncident)
		api.Post("/sections/{sectionID}/isolate", a.isolateSection)
		api.Get("/dispatch/plans", a.listPlans)
		api.Post("/dispatch/plans", a.createPlan)
		api.Post("/dispatch/plans/{planID}/publish", a.publishPlan)
		api.Post("/dispatch/holds", a.applyHold)
		api.Delete("/dispatch/holds/{stationID}", a.releaseHold)
		api.Post("/dispatch/buffer", a.bufferCommand)
	})
	return router
}

func (a *App) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "heatpulse"})
}
