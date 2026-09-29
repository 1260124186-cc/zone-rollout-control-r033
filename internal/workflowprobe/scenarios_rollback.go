package workflowprobe

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runForwardRollback() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-rollback-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-rollback-02", "east"); err != nil {
		return err
	}

	var originalPlan domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "rollback.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    100,
		ExpectedNodes:       1,
	}, &originalPlan); err != nil {
		return err
	}

	var oldRevision domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5001,
		Authority: "ns1.rollback.example",
		Entries: []domain.ZoneEntry{
			{Owner: "rollback.example", Type: domain.EntryA, Value: "192.0.2.10"},
		},
	}, &oldRevision); err != nil {
		return err
	}
	var badRevision domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5002,
		Authority: "ns1.rollback.example",
		Entries: []domain.ZoneEntry{
			{Owner: "rollback.example", Type: domain.EntryA, Value: "192.0.2.11"},
		},
	}, &badRevision); err != nil {
		return err
	}

	originalPlan.Version += 2
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/activate", map[string]any{
		"expected_version": originalPlan.Version,
	}, &originalPlan); err != nil {
		return err
	}
	group, err := allocate(harness, originalPlan.ID, "rollback-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	if group.RevisionID != badRevision.ID {
		return ensure(false, "lease group did not record the active revision identity")
	}
	if _, err := submitProbe(harness, group.LeaseIDs[0], "rollback-probe-0001", true, badRevision.Serial+1, 10); err != nil {
		return err
	}

	var approval domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "rollback-approval-0001",
	}, &approval); err != nil {
		return err
	}
	if approval.Verdict != domain.VerdictReviewRequired || approval.RevisionID != badRevision.ID {
		return ensure(false, "rollback approval did not describe the current revision")
	}

	originalPlan.Version++
	var rollback struct {
		Rollback domain.RollbackFact `json:"rollback"`
		Revision domain.ZoneRevision `json:"revision"`
		Plan     domain.RolloutPlan  `json:"plan"`
	}
	request := map[string]any{
		"expected_version":   originalPlan.Version,
		"request_key":        "rollback-request-0001",
		"target_revision_id": oldRevision.ID,
		"serial":             5003,
		"reason":             "restore last known good zone after failed release",
		"approval_id":        approval.ID,
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/rollbacks", request, &rollback); err != nil {
		return err
	}
	if rollback.Plan.State != domain.PlanDraft || rollback.Revision.Serial != 5003 {
		return ensure(false, "rollback did not create a new higher serial draft revision")
	}
	if rollback.Revision.Rollback == nil ||
		rollback.Revision.Rollback.TargetRevisionID != oldRevision.ID ||
		rollback.Revision.Rollback.SourceRevisionID != badRevision.ID ||
		rollback.Rollback.NewRevisionID != rollback.Revision.ID {
		return ensure(false, "rollback lineage or approval relation was incomplete")
	}
	if len(rollback.Rollback.SupersededLeaseIDs) != 1 {
		return ensure(false, "rollback did not record the superseded lease")
	}

	var replay struct {
		Rollback domain.RollbackFact `json:"rollback"`
		Revision domain.ZoneRevision `json:"revision"`
		Plan     domain.RolloutPlan  `json:"plan"`
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/rollbacks", request, &replay); err != nil {
		return err
	}
	if replay.Rollback.ID != rollback.Rollback.ID || replay.Revision.ID != rollback.Revision.ID {
		return ensure(false, "rollback replay created another fact")
	}

	if err := harness.call(http.MethodPatch, "/v1/leases/"+rollback.Rollback.SupersededLeaseIDs[0]+"/probes", domain.ProbeInput{
		RequestKey:     "rollback-probe-old001",
		Success:        true,
		ObservedSerial: rollback.Revision.Serial,
		LatencyMS:      10,
		ObservedAt:     harness.clock.Now(),
	}, nil); err == nil {
		return ensure(false, "superseded lease accepted a probe")
	}

	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/activate", map[string]any{
		"expected_version": rollback.Plan.Version,
	}, &rollback.Plan); err != nil {
		return err
	}
	newGroup, err := allocate(harness, originalPlan.ID, "rollback-lease-0002", "east", 1)
	if err != nil {
		return err
	}
	if newGroup.RevisionID != rollback.Revision.ID {
		return ensure(false, "new lease did not reference the rollback revision")
	}
	if err := harness.call(http.MethodGet, "/v1/plans/"+originalPlan.ID, nil, &rollback.Plan); err != nil {
		return err
	}
	if _, err := submitProbe(harness, newGroup.LeaseIDs[0], "rollback-probe-new001", true, rollback.Revision.Serial, 10); err != nil {
		return err
	}
	var newAssessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "rollback-assessment-new1",
	}, &newAssessment); err != nil {
		return err
	}
	if newAssessment.RevisionID != rollback.Revision.ID || newAssessment.Verdict != domain.VerdictConsistent {
		return ensure(false, "post-rollback assessment did not use the new revision")
	}

	var completed domain.RolloutPlan
	rollback.Plan.Version++
	if err := harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/complete", map[string]any{
		"expected_version": rollback.Plan.Version,
	}, &completed); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+originalPlan.ID+"/rollbacks", map[string]any{
		"expected_version":   completed.Version,
		"request_key":        "rollback-request-completed",
		"target_revision_id": oldRevision.ID,
		"reason":             "attempt rollback after plan completion",
		"approval_id":        approval.ID,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	var sameTargetPlan domain.RolloutPlan
	var sameOld domain.ZoneRevision
	var sameCurrent domain.ZoneRevision
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "same.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    100,
		ExpectedNodes:       1,
	}, &sameTargetPlan); err != nil {
		return err
	}
	makeRevision := func(serial uint32, ip string, target any) error {
		return harness.call(http.MethodPatch, "/v1/plans/"+sameTargetPlan.ID+"/revisions", domain.RevisionCreateInput{
			Serial:    serial,
			Authority: "ns1.same.example",
			Entries:   []domain.ZoneEntry{{Owner: "same.example", Type: domain.EntryA, Value: ip}},
		}, target)
	}
	if err := makeRevision(6001, "192.0.2.20", &sameOld); err != nil {
		return err
	}
	if err := makeRevision(6002, "192.0.2.21", &sameCurrent); err != nil {
		return err
	}
	if err := makeRevision(6003, "192.0.2.20", nil); err != nil {
		return err
	}
	sameTargetPlan.Version += 3
	if err := harness.call(http.MethodPatch, "/v1/plans/"+sameTargetPlan.ID+"/activate", map[string]any{
		"expected_version": sameTargetPlan.Version,
	}, &sameTargetPlan); err != nil {
		return err
	}
	sameGroup, err := allocate(harness, sameTargetPlan.ID, "rollback-lease-same001", "east", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(harness, sameGroup.LeaseIDs[0], "rollback-probe-same01", true, 6004, 10); err != nil {
		return err
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+sameTargetPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "same-approval-0001",
	}, &review); err != nil {
		return err
	}
	sameTargetPlan.Version++
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "same-content plan did not receive review approval")
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+sameTargetPlan.ID+"/rollbacks", map[string]any{
		"expected_version":   sameTargetPlan.Version,
		"request_key":        "rollback-same-target-01",
		"target_revision_id": sameOld.ID,
		"reason":             "roll back to content identical to current revision",
		"approval_id":        review.ID,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeRollbackNotRequired)); err != nil {
		return err
	}

	err = harness.call(http.MethodPatch, "/v1/plans/"+sameTargetPlan.ID+"/rollbacks", map[string]any{
		"expected_version":   sameTargetPlan.Version,
		"request_key":        "rollback-invalid-approval",
		"target_revision_id": sameCurrent.ID,
		"reason":             "rollback using an approval for another revision",
		"approval_id":        approval.ID,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeApprovalInvalid)); err != nil {
		return err
	}
	return nil
}
