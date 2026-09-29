// Package rollback implements forward rollback: selecting an immutable
// historical zone revision and publishing its content as a brand-new, strictly
// higher-serial revision, while recording the rollback reason, source
// revision, approval and operational fact as an immutable record.
package rollback

import (
	"context"
	"math"
	"reflect"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

// Result bundles the rollback fact with the new revision identity and the
// updated plan, so callers can see every linked fact in one response.
type Result struct {
	Rollback domain.RollbackRecord `json:"rollback"`
	Revision domain.ZoneRevision   `json:"revision"`
	Plan     domain.RolloutPlan    `json:"plan"`
}

// Apply performs an idempotent forward rollback.
//
// Deterministic outcomes:
//   - identical target and current content -> rollback_noop;
//   - a completed (or non-active) plan -> invalid_state;
//   - an approval that is no longer valid -> approval_expired;
//   - replaying the same request key with the same content returns the
//     original fact; a different payload returns idempotency_conflict.
func (s *Service) Apply(
	ctx context.Context,
	planID string,
	input domain.RollbackInput,
) (Result, error) {
	now := s.clock.Now().UTC()
	input = domain.NormalizeRollbackInput(input)
	if err := domain.ValidateRollbackInput(input, now); err != nil {
		return Result{}, err
	}
	requestIndex := planID + "\x00" + input.RequestKey
	fingerprint := domain.RollbackFingerprint(planID, input)

	var result Result
	err := s.store.Write(ctx, func(state *domain.State) error {
		// Idempotency is resolved before any state gate, so a retried rollback
		// always returns the same fact even if the plan later completed.
		if existingID, exists := state.RollbackRequestKeys[requestIndex]; exists {
			existing, found := state.Rollbacks[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "rollback index is inconsistent", map[string]any{
					"rollback_id": existingID,
				})
			}
			if existing.Fingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different rollback content", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			return hydrateResult(state, existing, &result)
		}

		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for rollback", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if plan.LatestRevisionID == "" {
			return domain.InvalidState("plan has no current revision to roll back from", map[string]any{
				"plan_id": planID,
			})
		}
		source, ok := state.Revisions[input.SourceRevisionID]
		if !ok || source.PlanID != planID {
			return domain.NotFound("revision", input.SourceRevisionID)
		}
		current, ok := state.Revisions[plan.LatestRevisionID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
				"revision_id": plan.LatestRevisionID,
			})
		}

		// The target content must differ from what is currently published; a
		// rollback that would publish the same authority and entries is a
		// deterministic no-op rather than a fresh fact.
		if source.Authority == current.Authority &&
			reflect.DeepEqual(source.Entries, current.Entries) {
			return domain.NewError(domain.CodeRollbackNoop, "rollback target content is identical to current content", map[string]any{
				"plan_id":             planID,
				"source_revision_id":  source.ID,
				"current_revision_id": current.ID,
			})
		}

		newSerial := input.Serial
		if newSerial == 0 {
			if current.Serial == math.MaxUint32 {
				return domain.NewError(domain.CodeConflict, "cannot allocate a higher serial", map[string]any{
					"plan_id": planID,
					"serial":  current.Serial,
				})
			}
			newSerial = current.Serial + 1
		} else if newSerial <= current.Serial {
			return domain.Validation("rollback serial must be higher than the current serial", map[string]any{
				"serial":  newSerial,
				"current": current.Serial,
			})
		}

		newRevisionID := s.ids.New("rev")
		newRevision := domain.ZoneRevision{
			ID:         newRevisionID,
			PlanID:     planID,
			Serial:     newSerial,
			Authority:  source.Authority,
			Entries:    append([]domain.ZoneEntry(nil), source.Entries...),
			EntryCount: len(source.Entries),
			CreatedAt:  now,
		}
		rollbackID := s.ids.New("rbk")
		newRevision.RollbackID = rollbackID

		record := domain.RollbackRecord{
			ID:               rollbackID,
			PlanID:           planID,
			RequestKey:       input.RequestKey,
			Fingerprint:      fingerprint,
			Reason:           input.Reason,
			SourceRevisionID: source.ID,
			SourceSerial:     source.Serial,
			FromRevisionID:   current.ID,
			FromSerial:       current.Serial,
			NewRevisionID:    newRevisionID,
			NewSerial:        newSerial,
			ApprovalID:       input.ApprovalID,
			ApprovedUntil:    input.ApprovedUntil.UTC(),
			Operator:         input.Operator,
			CreatedAt:        now,
		}

		state.Revisions[newRevisionID] = newRevision
		state.Rollbacks[rollbackID] = record
		state.RollbackRequestKeys[requestIndex] = rollbackID
		plan.LatestRevisionID = newRevisionID
		plan.LatestRollbackID = rollbackID
		plan.Version++
		state.Plans[planID] = plan

		store.AddAudit(state, s.ids, "revision", newRevisionID, "added", now, map[string]any{
			"plan_id":       planID,
			"serial":        newSerial,
			"rollback_id":   rollbackID,
			"source_serial": source.Serial,
		})
		store.AddAudit(state, s.ids, "rollback", rollbackID, "created", now, map[string]any{
			"plan_id":            planID,
			"reason":             record.Reason,
			"source_revision_id": source.ID,
			"source_serial":      source.Serial,
			"from_revision_id":   current.ID,
			"from_serial":        current.Serial,
			"new_revision_id":    newRevisionID,
			"new_serial":         newSerial,
			"approval_id":        record.ApprovalID,
			"approved_until":     record.ApprovedUntil.Format("2006-01-02T15:04:05Z07:00"),
		})

		result = Result{Rollback: record, Revision: newRevision, Plan: plan}
		return nil
	})
	return result, err
}

// Latest returns the most recent rollback fact for a plan together with the
// revision it produced.
func (s *Service) Latest(ctx context.Context, planID string) (Result, error) {
	var result Result
	err := s.store.Read(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.LatestRollbackID == "" {
			return domain.NotFound("rollback", planID)
		}
		record, ok := state.Rollbacks[plan.LatestRollbackID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing rollback", map[string]any{
				"rollback_id": plan.LatestRollbackID,
			})
		}
		return hydrateResult(state, record, &result)
	})
	return result, err
}

func hydrateResult(state *domain.State, record domain.RollbackRecord, result *Result) error {
	revision, ok := state.Revisions[record.NewRevisionID]
	if !ok {
		return domain.NewError(domain.CodeStorageFailure, "rollback references a missing revision", map[string]any{
			"revision_id": record.NewRevisionID,
		})
	}
	plan, ok := state.Plans[record.PlanID]
	if !ok {
		return domain.NewError(domain.CodeStorageFailure, "rollback references a missing plan", map[string]any{
			"plan_id": record.PlanID,
		})
	}
	result.Rollback = record
	result.Revision = revision
	result.Plan = plan
	return nil
}
