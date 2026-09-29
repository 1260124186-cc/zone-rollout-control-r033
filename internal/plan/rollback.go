package plan

import (
	"context"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/store"
)

type RollbackResult struct {
	Rollback domain.RollbackFact `json:"rollback"`
	Revision domain.ZoneRevision `json:"revision"`
	Plan     domain.RolloutPlan  `json:"plan"`
}

func (s *Service) Rollback(
	ctx context.Context,
	planID string,
	expectedVersion uint64,
	input domain.RollbackInput,
) (RollbackResult, error) {
	input.RequestKey = strings.TrimSpace(input.RequestKey)
	input.TargetRevisionID = strings.TrimSpace(input.TargetRevisionID)
	input.Reason = strings.TrimSpace(input.Reason)
	input.ApprovalID = strings.TrimSpace(input.ApprovalID)
	if err := domain.ValidateRollbackInput(input); err != nil {
		return RollbackResult{}, err
	}

	var result RollbackResult
	err := s.store.Write(ctx, func(state *domain.State) error {
		currentPlan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}

		target, err := resolveRollbackTarget(state, currentPlan, input)
		if err != nil {
			return err
		}
		input.TargetRevisionID = target.ID
		fingerprint := domain.RollbackFingerprint(planID, input)
		requestIndex := planID + "\x00" + input.RequestKey

		if existingID, exists := state.RollbackRequestKeys[requestIndex]; exists {
			existing, found := state.Rollbacks[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "rollback request index is inconsistent", map[string]any{
					"rollback_id": existingID,
				})
			}
			if existing.PayloadFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different rollback content", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			newRevision, found := state.Revisions[existing.NewRevisionID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "rollback references a missing revision", map[string]any{
					"revision_id": existing.NewRevisionID,
				})
			}
			plan, found := state.Plans[planID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "rollback references a missing plan", map[string]any{
					"plan_id": planID,
				})
			}
			result = RollbackResult{Rollback: existing, Revision: newRevision, Plan: plan}
			return nil
		}

		source, found := state.Revisions[currentPlan.LatestRevisionID]
		if !found {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
				"revision_id": currentPlan.LatestRevisionID,
			})
		}
		if currentPlan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for rollback", map[string]any{
				"plan_id": planID,
				"state":   currentPlan.State,
			})
		}
		if currentPlan.Version != expectedVersion {
			return versionConflict(planID, expectedVersion, currentPlan.Version)
		}
		if revisionContentEqual(target, source) {
			return domain.NewError(domain.CodeRollbackNotRequired, "target revision already matches the current revision content", map[string]any{
				"plan_id":             planID,
				"current_revision_id": source.ID,
				"target_revision_id":  target.ID,
			})
		}
		if err := validateRollbackApproval(state, currentPlan, source, input.ApprovalID); err != nil {
			return err
		}

		newSerial := source.Serial + 1
		if input.Serial != nil {
			newSerial = *input.Serial
		}
		if source.Serial == math.MaxUint32 || newSerial <= source.Serial {
			return domain.Validation("rollback serial must be higher than the current serial", map[string]any{
				"current_serial": source.Serial,
				"serial":         newSerial,
			})
		}
		for _, revision := range state.Revisions {
			if revision.PlanID == planID && revision.Serial == newSerial {
				return domain.NewError(domain.CodeDuplicateRevision, "plan already contains this serial", map[string]any{
					"plan_id": planID,
					"serial":  newSerial,
				})
			}
		}

		now := s.clock.Now().UTC()
		factID := s.ids.New("rbk")
		newID := s.ids.New("rev")
		supersededLeaseIDs := supersedePlanLeases(state, planID, factID, now)
		newRevision := domain.ZoneRevision{
			ID:         newID,
			PlanID:     planID,
			Serial:     newSerial,
			Authority:  target.Authority,
			Entries:    append([]domain.ZoneEntry(nil), target.Entries...),
			EntryCount: len(target.Entries),
			CreatedAt:  now,
			Rollback: &domain.RevisionRollback{
				FactID:           factID,
				SourceRevisionID: source.ID,
				TargetRevisionID: target.ID,
				Reason:           input.Reason,
				ApprovalID:       input.ApprovalID,
			},
		}
		fact := domain.RollbackFact{
			ID:                 factID,
			PlanID:             planID,
			RequestKey:         input.RequestKey,
			SourceRevisionID:   source.ID,
			TargetRevisionID:   target.ID,
			NewRevisionID:      newID,
			ApprovalID:         input.ApprovalID,
			Reason:             input.Reason,
			PayloadFingerprint: fingerprint,
			SupersededLeaseIDs: supersededLeaseIDs,
			CreatedAt:          now,
		}

		state.Revisions[newID] = newRevision
		state.Rollbacks[factID] = fact
		state.RollbackRequestKeys[requestIndex] = factID
		currentPlan.LatestRevisionID = newID
		currentPlan.LatestAssessmentID = ""
		currentPlan.State = domain.PlanDraft
		currentPlan.ActivatedAt = nil
		currentPlan.Version++
		state.Plans[planID] = currentPlan
		store.AddAudit(state, s.ids, "rollback", factID, "created", now, map[string]any{
			"plan_id":            planID,
			"source_revision_id": source.ID,
			"target_revision_id": target.ID,
			"new_revision_id":    newID,
			"superseded_leases":  len(supersededLeaseIDs),
			"approval_id":        input.ApprovalID,
		})
		result = RollbackResult{Rollback: fact, Revision: newRevision, Plan: currentPlan}
		return nil
	})
	return result, err
}

func resolveRollbackTarget(
	state *domain.State,
	plan domain.RolloutPlan,
	input domain.RollbackInput,
) (domain.ZoneRevision, error) {
	if input.TargetRevisionID != "" {
		revision, ok := state.Revisions[input.TargetRevisionID]
		if !ok || revision.PlanID != plan.ID {
			return domain.ZoneRevision{}, domain.NotFound("revision", input.TargetRevisionID)
		}
		return revision, nil
	}
	if input.TargetSerial == nil || *input.TargetSerial == 0 {
		return domain.ZoneRevision{}, domain.Validation("rollback input is invalid", map[string]any{
			"target_revision_id": "is required",
		})
	}
	var matches []domain.ZoneRevision
	for _, revision := range state.Revisions {
		if revision.PlanID == plan.ID && revision.Serial == *input.TargetSerial {
			matches = append(matches, revision)
		}
	}
	if len(matches) == 0 {
		return domain.ZoneRevision{}, domain.NotFound("revision", "")
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	return matches[0], nil
}

func validateRollbackApproval(
	state *domain.State,
	plan domain.RolloutPlan,
	source domain.ZoneRevision,
	approvalID string,
) error {
	approval, ok := state.Assessments[approvalID]
	if !ok {
		return domain.NewError(domain.CodeApprovalInvalid, "rollback approval was not found", map[string]any{
			"approval_id": approvalID,
		})
	}
	if approval.PlanID != plan.ID ||
		approval.RevisionID != source.ID ||
		plan.LatestAssessmentID != approvalID ||
		approval.Verdict != domain.VerdictReviewRequired {
		return domain.NewError(domain.CodeApprovalInvalid, "rollback approval is no longer valid", map[string]any{
			"approval_id": approvalID,
			"plan_id":     plan.ID,
		})
	}
	return nil
}

func supersedePlanLeases(state *domain.State, planID string, rollbackID string, now time.Time) []string {
	leaseIDs := make([]string, 0)
	for leaseID, lease := range state.Leases {
		if lease.PlanID != planID || lease.State != domain.LeaseClaimed {
			continue
		}
		finishedAt := now
		lease.State = domain.LeaseSuperseded
		lease.FinishedAt = &finishedAt
		lease.RollbackID = rollbackID
		state.Leases[leaseID] = lease
		leaseIDs = append(leaseIDs, leaseID)
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
	}
	sort.Strings(leaseIDs)
	return leaseIDs
}

func revisionContentEqual(left domain.ZoneRevision, right domain.ZoneRevision) bool {
	return left.Authority == right.Authority && reflect.DeepEqual(left.Entries, right.Entries)
}
