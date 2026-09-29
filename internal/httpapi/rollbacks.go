package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) rollbackPlan(w http.ResponseWriter, r *http.Request) {
	var input domain.RollbackInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	result, err := s.rollbacks.Apply(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, result)
}

func (s *Server) latestRollback(w http.ResponseWriter, r *http.Request) {
	result, err := s.rollbacks.Latest(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, result)
}
