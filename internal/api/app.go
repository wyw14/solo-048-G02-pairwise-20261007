package api

import (
	"context"
	"path/filepath"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/dispatch"
	"heatpulse/internal/exchanger"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/isolation"
	"heatpulse/internal/network"
	"heatpulse/internal/protection"
	"heatpulse/internal/pump"
	"heatpulse/internal/recovery"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
	"heatpulse/internal/zoneplan"
)

type App struct {
	DataDirectory        string
	Events               *store.EventLog
	Audit                *audit.Recorder
	AuditQuery           *audit.Query
	Network              *network.State
	Topology             *network.Topology
	NetworkRegistry      *network.Registry
	Stations             *station.Registry
	Snapshots            *store.StationSnapshotStore
	Interlock            *hydraulic.Interlock
	Pumps                *pump.Service
	PumpRunner           *pump.Runner
	Pressure             *pump.PressureMonitor
	Valves               *valve.Service
	ValveOperations      *valve.Operations
	Relief               *valve.ReliefService
	Boundaries           *valve.BoundaryService
	Primary              *valve.PrimaryService
	Telemetry            *telemetry.Series
	SnapshotsBuilder     *telemetry.SnapshotBuilder
	FlowReceipts         *telemetry.FlowReceipts
	Balancer             *hydraulic.Balancer
	LeakDetector         *hydraulic.LeakDetector
	LeakProtection       *protection.LeakProtection
	Protection           *protection.Service
	Latches              *protection.LatchStore
	Frost                *protection.FrostProtection
	ExchangerSwitch      *exchanger.SwitchService
	Circulation          *exchanger.CirculationRunner
	Antifreeze           *protection.Antifreeze
	Warmup               *exchanger.WarmupService
	Isolation            *isolation.Service
	IsolationCoordinator *isolation.Coordinator
	Targets              *station.TargetService
	Planner              *dispatch.Planner
	Publisher            *dispatch.Publisher
	DispatchBuffer       *dispatch.Buffer
	BalancePlanner       *zoneplan.Planner
	BalanceExecutor      *zoneplan.Executor
	StationCommands      *station.CommandService
	Reconnector          *recovery.Reconnector
	ZoneRecovery         *recovery.ZoneRecovery
	Manual               *station.ManualService
	Maintenance          *station.MaintenanceService
}

func NewApp(dataDirectory string) (*App, error) {
	events := store.NewEventLog(filepath.Join(dataDirectory, "events.jsonl"))
	recorder := audit.NewRecorder(events)
	zones := []network.Zone{
		{ID: "north", Name: "北环", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.82, ReturnPressure: 0.48},
		{ID: "east", Name: "东支线", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.76, ReturnPressure: 0.45},
		{ID: "west", Name: "西区", Phase: network.PhaseActive, PumpStage: 2, BypassOpen: true, SupplyPressure: 0.79, ReturnPressure: 0.46},
	}
	networkState := network.NewState(zones)
	topology := network.NewTopology([]network.Section{
		{ID: "east-branch", ZoneID: "east", Upstream: "east-supply", Downstream: "east-return", BoundaryValves: []string{"east-bv-a", "east-bv-b"}, ReliefValve: "east-rv"},
		{ID: "west-loop", ZoneID: "west", Upstream: "west-supply", Downstream: "west-return", BoundaryValves: []string{"west-bv-a", "west-bv-b"}, ReliefValve: "west-rv"},
	})
	if err := topology.Validate(); err != nil {
		return nil, err
	}
	stationItems := []station.Station{
		{ID: "xincheng", Name: "新城站", ZoneID: "north", Online: true, TargetCelsius: 68, TargetRevision: 1, ControllerEpoch: 1, Exchangers: map[string]station.ExchangerPhase{"A": station.ExchangerCarrying, "B": station.ExchangerStandby}},
		{ID: "binhe", Name: "滨河站", ZoneID: "east", Online: true, TargetCelsius: 66, TargetRevision: 1, ControllerEpoch: 1, Exchangers: map[string]station.ExchangerPhase{"A": station.ExchangerCarrying, "B": station.ExchangerStandby}},
		{ID: "nanhu", Name: "南湖站", ZoneID: "west", Online: true, TargetCelsius: 64, TargetRevision: 1, ControllerEpoch: 1, Exchangers: map[string]station.ExchangerPhase{"A": station.ExchangerCarrying, "B": station.ExchangerStandby}},
		{ID: "xijiao", Name: "西郊站", ZoneID: "west", Online: true, TargetCelsius: 65, TargetRevision: 1, ControllerEpoch: 1, Exchangers: map[string]station.ExchangerPhase{"A": station.ExchangerCarrying, "B": station.ExchangerStandby}},
	}
	stations := station.NewRegistry(stationItems)
	networkRegistry := network.NewRegistry([]network.StationBinding{
		{StationID: "xincheng", ZoneID: "north", Required: true},
		{StationID: "binhe", ZoneID: "east", Required: true},
		{StationID: "nanhu", ZoneID: "west", Required: true},
		{StationID: "xijiao", ZoneID: "west", Required: true},
	})
	snapshots := store.NewStationSnapshotStore(dataDirectory)
	interlock := hydraulic.NewInterlock(networkState)
	pumpRunner := pump.NewRunner()
	pressure := pump.NewPressureMonitor()
	valveOperations := valve.NewOperations()
	flowReceipts := telemetry.NewFlowReceipts()
	primary := valve.NewPrimaryService()
	circulation := exchanger.NewCirculationRunner(pumpRunner)
	antifreeze := protection.NewAntifreeze(circulation)
	warmup := exchanger.NewWarmupService(stations)
	relief := valve.NewReliefService(2 * time.Millisecond)
	boundaries := valve.NewBoundaryService()
	isolationService := isolation.NewService(topology, networkState, relief, boundaries, recorder)
	targets := station.NewTargetService(stations)
	planner := dispatch.NewPlanner(targets)
	dispatchBuffer := dispatch.NewBuffer()
	stationCommands := station.NewCommandService(stations)
	app := &App{
		DataDirectory: dataDirectory, Events: events, Audit: recorder, AuditQuery: audit.NewQuery(events),
		Network: networkState, Topology: topology, NetworkRegistry: networkRegistry, Stations: stations,
		Snapshots: snapshots, Interlock: interlock, Pumps: pump.NewService(interlock, recorder),
		PumpRunner: pumpRunner, Pressure: pressure, Valves: valve.NewService(interlock, recorder),
		ValveOperations: valveOperations, Relief: relief, Boundaries: boundaries, Primary: primary,
		Telemetry: telemetry.NewSeries(128), FlowReceipts: flowReceipts,
		Balancer: hydraulic.NewBalancer(0.28), LeakDetector: hydraulic.NewLeakDetector(),
		Protection: protection.NewService(), Latches: protection.NewLatchStore(snapshots),
		Frost: protection.NewFrostProtection(valveOperations), Circulation: circulation, Antifreeze: antifreeze,
		Warmup: warmup, Isolation: isolationService, IsolationCoordinator: isolation.NewCoordinator(isolationService),
		Targets: targets, Planner: planner, DispatchBuffer: dispatchBuffer, StationCommands: stationCommands,
		Manual: station.NewManualService(valveOperations), Maintenance: station.NewMaintenanceService(stations, snapshots),
	}
	app.SnapshotsBuilder = telemetry.NewSnapshotBuilder(app.Telemetry)
	app.LeakProtection = protection.NewLeakProtection(app.LeakDetector)
	app.ExchangerSwitch = exchanger.NewSwitchService(stations, flowReceipts, primary)
	app.Publisher = dispatch.NewPublisher(planner, targets, recorder)
	app.Reconnector = recovery.NewReconnector(dispatchBuffer, stations, stationCommands, recorder)
	app.ZoneRecovery = recovery.NewZoneRecovery(networkState, networkRegistry, warmup, pressure)
	app.BalancePlanner = zoneplan.NewPlanner(
		stations, networkState, networkRegistry, app.SnapshotsBuilder, app.Balancer, zoneplan.DefaultOptions(),
	)
	app.BalanceExecutor = zoneplan.NewExecutor(
		app.BalancePlanner, app.SnapshotsBuilder, networkState, networkRegistry,
		app.Pumps, app.Valves, valveOperations, recorder,
	)
	if err := app.seed(); err != nil {
		return nil, err
	}
	return app, nil
}

func (a *App) seed() error {
	for _, item := range a.Stations.List() {
		for _, sample := range []telemetry.Sample{
			{StationID: item.ID, Kind: telemetry.KindSupplyPressure, Watermark: 1, Value: 0.78},
			{StationID: item.ID, Kind: telemetry.KindReturnPressure, Watermark: 1, Value: 0.46},
			{StationID: item.ID, Kind: telemetry.KindFlow, Watermark: 1, Value: 8.1},
			{StationID: item.ID, Kind: telemetry.KindValvePosition, Watermark: 1, Value: 72},
			{StationID: item.ID, Kind: telemetry.KindTemperature, Watermark: 1, Value: 58},
		} {
			if err := a.Telemetry.Add(sample); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *App) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		a.Antifreeze.Close()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
