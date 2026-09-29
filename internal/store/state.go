package store

import "solo-0014-zone-rollout-control/internal/domain"

const schemaVersion = 2

func blankState() domain.State {
	return normalize(domain.State{})
}

func normalize(state domain.State) domain.State {
	if state.SchemaVersion == 0 {
		state.SchemaVersion = schemaVersion
	}
	if state.Plans == nil {
		state.Plans = make(map[string]domain.RolloutPlan)
	}
	if state.Revisions == nil {
		state.Revisions = make(map[string]domain.ZoneRevision)
	}
	if state.Rollbacks == nil {
		state.Rollbacks = make(map[string]domain.RollbackFact)
	}
	if state.Nodes == nil {
		state.Nodes = make(map[string]domain.ResolverNode)
	}
	if state.Leases == nil {
		state.Leases = make(map[string]domain.NodeLease)
	}
	if state.LeaseGroups == nil {
		state.LeaseGroups = make(map[string]domain.LeaseGroup)
	}
	if state.Probes == nil {
		state.Probes = make(map[string]domain.Probe)
	}
	if state.Assessments == nil {
		state.Assessments = make(map[string]domain.RolloutAssessment)
	}
	if state.LeaseRequestKeys == nil {
		state.LeaseRequestKeys = make(map[string]string)
	}
	if state.RollbackRequestKeys == nil {
		state.RollbackRequestKeys = make(map[string]string)
	}
	if state.AssessmentRequestKeys == nil {
		state.AssessmentRequestKeys = make(map[string]string)
	}
	if state.AuditEvents == nil {
		state.AuditEvents = []domain.AuditEvent{}
	}
	backfillRevisionIdentity(&state)
	return state
}

func backfillRevisionIdentity(state *domain.State) {
	for leaseID, lease := range state.Leases {
		if lease.RevisionID == "" {
			if plan, ok := state.Plans[lease.PlanID]; ok && plan.LatestRevisionID != "" {
				lease.RevisionID = plan.LatestRevisionID
				state.Leases[leaseID] = lease
			}
		}
	}
	for groupID, group := range state.LeaseGroups {
		if group.RevisionID == "" {
			if plan, ok := state.Plans[group.PlanID]; ok && plan.LatestRevisionID != "" {
				group.RevisionID = plan.LatestRevisionID
				state.LeaseGroups[groupID] = group
			}
		}
	}
	for probeID, probe := range state.Probes {
		if probe.RevisionID == "" {
			if lease, ok := state.Leases[probe.LeaseID]; ok && lease.RevisionID != "" {
				probe.RevisionID = lease.RevisionID
				state.Probes[probeID] = probe
			}
		}
	}
}
