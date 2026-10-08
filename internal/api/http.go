package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/model"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/service"
	"github.com/Diudiuseven/support-agent-handoff-go/internal/store"
)

// Handler 暴露 HTTP 接口，便于集成与演示
type Handler struct {
	svc *service.Service
}

func NewHandler(svc *service.Service) *Handler {
	return &Handler{svc: svc}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case err == nil:
		return
	case errors.Is(err, service.ErrPermissionDenied):
		status = http.StatusForbidden
	case errors.Is(err, service.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, service.ErrInvalidState), errors.Is(err, service.ErrAgentUnavailable):
		status = http.StatusConflict
	case errors.Is(err, store.ErrNotFound):
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /sessions", h.startSession)
	mux.HandleFunc("GET /sessions", h.listSessions)
	mux.HandleFunc("GET /sessions/{id}", h.getSession)
	mux.HandleFunc("POST /sessions/{id}/handoff", h.requestHandoff)
	mux.HandleFunc("POST /sessions/{id}/messages", h.sendMessage)
	mux.HandleFunc("GET /sessions/{id}/messages", h.listMessages)
	mux.HandleFunc("POST /sessions/{id}/end", h.endSession)
	mux.HandleFunc("GET /sessions/{id}/audit", h.listAudit)
	mux.HandleFunc("GET /sessions/{id}/handoffs", h.listHandoffs)

	mux.HandleFunc("POST /agents", h.registerAgent)
	mux.HandleFunc("GET /agents", h.listAgents)
	mux.HandleFunc("POST /agents/{id}/online", h.agentOnline)
	mux.HandleFunc("POST /agents/{id}/offline", h.agentOffline)
	mux.HandleFunc("POST /agents/{id}/claim", h.claim)
	mux.HandleFunc("POST /agents/{id}/transfer", h.transfer)
	mux.HandleFunc("POST /agents/{id}/transfer/accept", h.acceptTransfer)
	mux.HandleFunc("POST /agents/{id}/transfer/reject", h.rejectTransfer)

	mux.HandleFunc("GET /queue", h.listQueue)
	mux.HandleFunc("GET /stats", h.stats)
	return mux
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.Stats())
}

func decode(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func (h *Handler) startSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID    string `json:"id"`
		Skill string `json:"skill"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	sess, err := h.svc.StartSession(r.Context(), model.SessionID(req.ID), req.Skill)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sess)
}

func (h *Handler) listSessions(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.ListSessions(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) getSession(w http.ResponseWriter, r *http.Request) {
	sess, err := h.svc.GetSession(r.Context(), model.SessionID(r.PathValue("id")))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sess)
}

func (h *Handler) requestHandoff(w http.ResponseWriter, r *http.Request) {
	sess, err := h.svc.RequestHandoff(r.Context(), model.SessionID(r.PathValue("id")), model.ReasonHandoffRequested)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sess)
}

func (h *Handler) sendMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From string `json:"from"`
		Body string `json:"body"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	msg, err := h.svc.SendMessage(r.Context(), model.SessionID(r.PathValue("id")), req.From, req.Body)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, msg)
}

func (h *Handler) listMessages(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.ListMessages(r.Context(), model.SessionID(r.PathValue("id")))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) endSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ActorID string `json:"actor_id"`
		Role    string `json:"role"`
	}
	_ = decode(r, &req)
	if err := h.svc.EndSession(r.Context(), model.SessionID(r.PathValue("id")), req.ActorID, model.Role(req.Role)); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listAudit(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.ListAudit(r.Context(), model.SessionID(r.PathValue("id")))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) listHandoffs(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.ListHandoffs(r.Context(), model.SessionID(r.PathValue("id")))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) registerAgent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string   `json:"id"`
		Name     string   `json:"name"`
		Role     string   `json:"role"`
		Skills   []string `json:"skills"`
		Capacity int      `json:"capacity"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	a := &model.Agent{
		ID:       model.AgentID(req.ID),
		Name:     req.Name,
		Role:     model.Role(req.Role),
		Skills:   req.Skills,
		Capacity: req.Capacity,
	}
	if err := h.svc.RegisterAgent(r.Context(), a); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (h *Handler) listAgents(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.ListAgents(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) agentOnline(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.AgentOnline(r.Context(), model.AgentID(r.PathValue("id"))); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) agentOffline(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.AgentOffline(r.Context(), model.AgentID(r.PathValue("id"))); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) claim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	handoff, err := h.svc.Claim(r.Context(), model.AgentID(r.PathValue("id")), model.SessionID(req.SessionID))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, handoff)
}

func (h *Handler) transfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
		ToAgent   string `json:"to_agent"`
		ActorID   string `json:"actor_id"`
		Role      string `json:"role"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	actor := req.ActorID
	if actor == "" {
		actor = r.PathValue("id")
	}
	handoff, err := h.svc.Transfer(r.Context(), model.AgentID(actor), model.Role(req.Role),
		model.SessionID(req.SessionID), model.AgentID(req.ToAgent))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, handoff)
}

func (h *Handler) acceptTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := h.svc.AcceptTransfer(r.Context(), model.AgentID(r.PathValue("id")), model.SessionID(req.SessionID)); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) rejectTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := h.svc.RejectTransfer(r.Context(), model.AgentID(r.PathValue("id")), model.SessionID(req.SessionID)); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listQueue(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.ListQueue(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
