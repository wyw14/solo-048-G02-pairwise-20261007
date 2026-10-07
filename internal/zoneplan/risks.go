package zoneplan

import (
	"fmt"
	"math"
	"sort"
)

// risksForMeasures turns per-station evidence into ranked risks. Zone-level
// pump envelope risks are added separately once the target stage is known.
func risksForMeasures(measures []StationMeasure, opts Options) []Risk {
	risks := make([]Risk, 0, len(measures)*3)
	for _, item := range measures {
		if item.LeakSuspected {
			risks = append(risks, Risk{
				StationID: item.StationID, Code: RiskLeakSuspected, Severity: 1.0,
				Message: fmt.Sprintf("measured flow deviates %.0f%% from the valve model; possible leakage", item.ImbalanceRatio*100),
			})
		} else if item.ImbalanceRatio >= opts.LeakThreshold*0.6 {
			risks = append(risks, Risk{
				StationID: item.StationID, Code: RiskHighImbalance,
				Severity: 0.55 + clamp(item.ImbalanceRatio, 0, 1)*0.3,
				Message:  fmt.Sprintf("flow-vs-valve imbalance is %.0f%%, approaching the leak threshold", item.ImbalanceRatio*100),
			})
		}
		if item.DifferentialPressure < opts.LowDPThreshold {
			risks = append(risks, Risk{
				StationID: item.StationID, Code: RiskLowDP,
				Severity: 0.8 + clamp((opts.LowDPThreshold-item.DifferentialPressure)/opts.LowDPThreshold, 0, 0.15),
				Message: fmt.Sprintf("differential pressure %.2f MPa is below the %.2f MPa floor",
					item.DifferentialPressure, opts.LowDPThreshold),
			})
		}
		if item.FlowDeltaRatio < -opts.FlowTolerance {
			severity := 0.4 + clamp(-item.FlowDeltaRatio, 0, 0.5)
			code := RiskFlowDeficit
			if item.RequiredOpening >= opts.ValveMax-0.01 {
				code = RiskValveFullyOpen
				severity += 0.1
			}
			risks = append(risks, Risk{
				StationID: item.StationID, Code: code, Severity: severity,
				Message: fmt.Sprintf("flow %.1f is %.0f%% under the %.1f target",
					item.MeasuredFlow, -item.FlowDeltaRatio*100, item.TargetFlow),
			})
		} else if item.FlowDeltaRatio > opts.FlowTolerance {
			risks = append(risks, Risk{
				StationID: item.StationID, Code: RiskFlowSurplus,
				Severity: 0.4 + clamp(item.FlowDeltaRatio, 0, 0.5),
				Message: fmt.Sprintf("flow %.1f is %.0f%% over the %.1f target",
					item.MeasuredFlow, item.FlowDeltaRatio*100, item.TargetFlow),
			})
		}
		if item.Held {
			risks = append(risks, Risk{
				StationID: item.StationID, Code: RiskStationHeld, Severity: 0.3,
				Message: "station carries a manual hold and cannot receive valve trims",
			})
		}
	}
	return risks
}

// zoneRisks flags a pump that has reached the end of its travel while stations
// are still out of balance; valve trims alone cannot fix that.
func zoneRisks(measures []StationMeasure, targetStage int, opts Options) []Risk {
	risks := make([]Risk, 0, 2)
	starved, throttled := 0, 0
	for _, item := range measures {
		if !item.Online {
			continue
		}
		if item.FlowDeltaRatio < -opts.FlowTolerance {
			starved++
		}
		if item.FlowDeltaRatio > opts.FlowTolerance {
			throttled++
		}
	}
	if starved > 0 && targetStage >= opts.MaxStage {
		risks = append(risks, Risk{
			Code: RiskStageAtCeiling, Severity: 0.75,
			Message: fmt.Sprintf("stations remain starved at pump stage ceiling %d; inspect the heat source", opts.MaxStage),
		})
	}
	if throttled > 0 && targetStage <= opts.MinStage {
		risks = append(risks, Risk{
			Code: RiskStageAtFloor, Severity: 0.6,
			Message: fmt.Sprintf("stations remain over-fed at pump stage floor %d; inspect pressure control", opts.MinStage),
		})
	}
	return risks
}

func rankRisks(risks []Risk) []Risk {
	sort.SliceStable(risks, func(i, j int) bool {
		if risks[i].Severity != risks[j].Severity {
			return risks[i].Severity > risks[j].Severity
		}
		if risks[i].StationID != risks[j].StationID {
			return risks[i].StationID < risks[j].StationID
		}
		return risks[i].Code < risks[j].Code
	})
	for index := range risks {
		risks[index].Rank = index + 1
		risks[index].Severity = math.Round(risks[index].Severity*100) / 100
	}
	return risks
}
