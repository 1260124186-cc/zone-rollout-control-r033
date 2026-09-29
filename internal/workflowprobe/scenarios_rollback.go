package workflowprobe

import (
	"fmt"
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

// runForwardRollback exercises forward rollback end to end over real HTTP:
// selecting a historical revision, publishing a fresh higher-serial revision,
// linking reason/source/approval facts, determinism on edge cases, idempotent
// replay, and post-rollback leases/assessments referencing the new identity.
func runForwardRollback() error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	// Three revisions in draft; rev1 is the rollback target later.
	zone := "rollback.example"
	var plan domain.RolloutPlan
	if err := h.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                zone,
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    80,
		ExpectedNodes:       2,
	}, &plan); err != nil {
		return err
	}
	revisionAt := func(serial uint32, ip string) (domain.ZoneRevision, error) {
		var revision domain.ZoneRevision
		err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
			Serial:    serial,
			Authority: "ns1." + zone,
			Entries: []domain.ZoneEntry{
				{Owner: zone, Type: domain.EntryA, Value: ip},
				{Owner: "www." + zone, Type: domain.EntryCNAME, Value: zone},
			},
		}, &revision)
		return revision, err
	}
	rev1, err := revisionAt(5001, "192.0.2.10")
	if err != nil {
		return err
	}
	if _, err := revisionAt(5002, "192.0.2.11"); err != nil {
		return err
	}
	rev3, err := revisionAt(5003, "192.0.2.12")
	if err != nil {
		return err
	}

	// Rollback is rejected while the plan is still a draft.
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "draft must not roll back",
		ApprovalID:       "apr-draft-0001",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		RequestKey:       "rbk-draft-0001",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/activate", map[string]any{
		"expected_version": plan.Version + 3,
	}, &plan); err != nil {
		return err
	}
	if plan.State != domain.PlanActive {
		return ensure(false, "plan was not activated")
	}

	// Unknown source revision is a deterministic not-found.
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: "rev_does_not_exist",
		Reason:           "missing source",
		ApprovalID:       "apr-missing-0001",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		RequestKey:       "rbk-missing-0001",
	}, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	// Expired approval is deterministically rejected before any write.
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "approval window closed",
		ApprovalID:       "apr-expired-0001",
		ApprovedUntil:    h.clock.Now().Add(-time.Minute),
		RequestKey:       "rbk-expired-0001",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeApprovalExpired)); err != nil {
		return err
	}

	// Rolling back to content identical to current is a deterministic no-op.
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev3.ID,
		Reason:           "same content must be a no-op",
		ApprovalID:       "apr-noop-0001",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		RequestKey:       "rbk-noop-0001",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeRollbackNoop)); err != nil {
		return err
	}

	// Successful forward rollback to the historical rev1 content: a brand-new
	// higher serial revision with the full fact linkage.
	var first rollbackResult
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "post-deploy outage on serial 5003, restore last known good",
		ApprovalID:       "apr-incident-0421",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		Operator:         "oncall-dns",
		RequestKey:       "rbk-incident-0001",
	}, &first); err != nil {
		return err
	}
	if err := assertRollbackShape(first, rev1, rev3, 5004, "apr-incident-0421"); err != nil {
		return err
	}
	if first.Plan.LatestRevisionID != first.Revision.ID ||
		first.Plan.LatestRollbackID != first.Rollback.ID {
		return ensure(false, "plan summary does not point at the new rollback identity")
	}
	if first.Revision.RollbackID != first.Rollback.ID {
		return ensure(false, "new revision is not linked back to the rollback fact")
	}
	// Historical revisions remain immutable: rev1 keeps its identity and serial.
	if first.Rollback.SourceRevisionID != rev1.ID || first.Rollback.SourceSerial != 5001 {
		return ensure(false, "rollback fact does not reference the historical source revision")
	}

	// Replaying the exact request returns the same fact, never a second one.
	var replay rollbackResult
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "post-deploy outage on serial 5003, restore last known good",
		ApprovalID:       "apr-incident-0421",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		Operator:         "oncall-dns",
		RequestKey:       "rbk-incident-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.Rollback.ID != first.Rollback.ID ||
		replay.Revision.ID != first.Revision.ID ||
		replay.Revision.Serial != first.Revision.Serial {
		return ensure(false, "rollback replay produced a different rollback fact or revision")
	}

	// Same request key with different content is an idempotency conflict.
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev3.ID,
		Reason:           "reused key with a different target",
		ApprovalID:       "apr-other-0001",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		RequestKey:       "rbk-incident-0001",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// Latest rollback read returns the first fact before the second rollback.
	var latest rollbackResult
	if err := h.call(http.MethodGet, "/v1/plans/"+plan.ID+"/rollbacks/latest", nil, &latest); err != nil {
		return err
	}
	if latest.Rollback.ID != first.Rollback.ID || latest.Revision.ID != first.Revision.ID {
		return ensure(false, "latest rollback does not match the created fact")
	}

	// A second, distinct forward rollback restores the just-superseded rev3
	// content as another brand-new revision; chains stay linked and immutable.
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev3.ID,
		Serial:           first.Revision.Serial, // equal to current serial
		Reason:           "explicit serial must be higher than current",
		ApprovalID:       "apr-incident-0422",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		RequestKey:       "rbk-low-serial-001",
	}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	var second rollbackResult
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev3.ID,
		Reason:           "restore serial 5003 content after incident cleared",
		ApprovalID:       "apr-incident-0422",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		Operator:         "oncall-dns",
		RequestKey:       "rbk-incident-0002",
	}, &second); err != nil {
		return err
	}
	if second.Revision.ID == first.Revision.ID || second.Revision.Serial != 5005 {
		return ensure(false, "second rollback did not create a fresh higher-serial revision")
	}
	if second.Rollback.SourceRevisionID != rev3.ID ||
		second.Rollback.FromRevisionID != first.Revision.ID {
		return ensure(false, "chained rollback fact links are wrong")
	}
	if second.Revision.Entries[0].Value != "192.0.2.12" {
		return ensure(false, "second rollback did not restore rev3 content")
	}
	// The first rollback fact is unchanged: facts are immutable.
	var firstAgain rollbackResult
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "post-deploy outage on serial 5003, restore last known good",
		ApprovalID:       "apr-incident-0421",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		Operator:         "oncall-dns",
		RequestKey:       "rbk-incident-0001",
	}, &firstAgain); err != nil {
		return err
	}
	if firstAgain.Rollback.ID != first.Rollback.ID ||
		firstAgain.Rollback.NewRevisionID != first.Revision.ID {
		return ensure(false, "first rollback fact changed after a subsequent rollback")
	}

	// Post-rollback leases and assessments must reference the new revision
	// identity; "current" is now the second rollback revision (serial 5005).
	if _, err := enrollNode(h, "resolver-rbk-01", "north"); err != nil {
		return err
	}
	if _, err := enrollNode(h, "resolver-rbk-02", "north"); err != nil {
		return err
	}
	if _, err := enrollNode(h, "resolver-rbk-03", "north"); err != nil {
		return err
	}
	group, err := allocate(h, plan.ID, "rbk-lease-0001", "north", 3)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 3 {
		return ensure(false, "expected three post-rollback leases")
	}
	var leased domain.NodeLease
	if err := h.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[2]+"/release", nil, &leased); err != nil {
		return err
	}
	if leased.RevisionID != second.Revision.ID {
		return ensure(false, "post-rollback lease does not reference the new revision identity")
	}
	// Probes on two nodes observe the RESTORED-TO serial 5003 instead of the
	// new 5005 identity: the assessment is computed against the new revision
	// identity and flags the mismatch.
	for _, leaseID := range group.LeaseIDs[:2] {
		if _, err := submitProbe(h, leaseID, fmt.Sprintf("rbk-probe-%s", leaseID), true, rev3.Serial, 10); err != nil {
			return err
		}
	}
	var assessment domain.RolloutAssessment
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "rbk-assess-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.RevisionID != second.Revision.ID {
		return ensure(false, "post-rollback assessment does not reference the new revision identity")
	}
	if assessment.Verdict != domain.VerdictReviewRequired ||
		!containsReason(assessment.Reasons, "serial_mismatch") {
		return ensure(false, "assessment against the new revision serial did not detect the stale observation")
	}

	// Refresh and complete the plan; rollback must then be deterministically
	// rejected because the plan is finished, while replay still returns the
	// original single fact.
	if err := h.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &plan); err != nil {
		return err
	}
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &plan); err != nil {
		return err
	}
	err = h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "completed plan cannot roll back",
		ApprovalID:       "apr-after-0001",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		RequestKey:       "rbk-after-complete-001",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	var afterReplay rollbackResult
	if err := h.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/rollbacks", domain.RollbackInput{
		SourceRevisionID: rev1.ID,
		Reason:           "post-deploy outage on serial 5003, restore last known good",
		ApprovalID:       "apr-incident-0421",
		ApprovedUntil:    h.clock.Now().Add(2 * time.Hour),
		Operator:         "oncall-dns",
		RequestKey:       "rbk-incident-0001",
	}, &afterReplay); err != nil {
		return err
	}
	if afterReplay.Rollback.ID != first.Rollback.ID {
		return ensure(false, "replay after completion did not return the original fact")
	}
	return nil
}

type rollbackResult struct {
	Rollback domain.RollbackRecord `json:"rollback"`
	Revision domain.ZoneRevision   `json:"revision"`
	Plan     domain.RolloutPlan    `json:"plan"`
}

func assertRollbackShape(
	result rollbackResult,
	source domain.ZoneRevision,
	from domain.ZoneRevision,
	newSerial uint32,
	approvalID string,
) error {
	if result.Revision.Serial != newSerial {
		return ensure(false, fmt.Sprintf("new revision serial = %d, want %d", result.Revision.Serial, newSerial))
	}
	if result.Revision.ID == source.ID || result.Revision.ID == from.ID {
		return ensure(false, "rollback reused an existing revision identity instead of creating a new one")
	}
	if result.Revision.Authority != source.Authority ||
		len(result.Revision.Entries) != len(source.Entries) {
		return ensure(false, "new revision does not carry the historical source content")
	}
	if result.Revision.Entries[0].Value != "192.0.2.10" {
		return ensure(false, "rolled back content does not match the selected historical revision")
	}
	fact := result.Rollback
	if fact.NewRevisionID != result.Revision.ID || fact.NewSerial != newSerial {
		return ensure(false, "rollback fact is not linked to the new revision identity")
	}
	if fact.SourceRevisionID != source.ID || fact.SourceSerial != source.Serial ||
		fact.FromRevisionID != from.ID || fact.FromSerial != from.Serial {
		return ensure(false, "rollback fact does not link source and superseded revisions")
	}
	if fact.ApprovalID != approvalID || fact.Reason == "" {
		return ensure(false, "rollback fact misses reason or approval linkage")
	}
	return nil
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
