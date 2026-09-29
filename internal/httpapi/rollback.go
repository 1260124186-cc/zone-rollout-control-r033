package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

type rollbackRequest struct {
	ExpectedVersion  uint64  `json:"expected_version"`
	RequestKey       string  `json:"request_key"`
	TargetRevisionID string  `json:"target_revision_id"`
	TargetSerial     *uint32 `json:"target_serial,omitempty"`
	Serial           *uint32 `json:"serial,omitempty"`
	Reason           string  `json:"reason"`
	ApprovalID       string  `json:"approval_id,omitempty"`
}

func (s *Server) rollbackPlan(w http.ResponseWriter, r *http.Request) {
	var request rollbackRequest
	if err := decode(w, r, &request); err != nil {
		sendError(w, r, err)
		return
	}
	if request.ExpectedVersion == 0 {
		sendError(w, r, domain.Validation("expected_version must be greater than zero", nil))
		return
	}
	result, err := s.plans.Rollback(r.Context(), r.PathValue("id"), request.ExpectedVersion, domain.RollbackInput{
		RequestKey:       request.RequestKey,
		TargetRevisionID: request.TargetRevisionID,
		TargetSerial:     request.TargetSerial,
		Serial:           request.Serial,
		Reason:           request.Reason,
		ApprovalID:       request.ApprovalID,
	})
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, result)
}
