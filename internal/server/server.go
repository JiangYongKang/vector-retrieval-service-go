// Package server exposes the engine over a small local HTTP API.
package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/engine"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/errs"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/model"
)

// Server wraps an engine with an HTTP handler.
type Server struct {
	eng *engine.Engine
	mux *http.ServeMux
}

// New creates a Server backed by eng.
func New(eng *engine.Engine) *Server {
	s := &Server{eng: eng, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/records", s.handleWrite)
	s.mux.HandleFunc("POST /v1/delete", s.handleDelete)
	s.mux.HandleFunc("POST /v1/search", s.handleSearch)
	s.mux.HandleFunc("POST /v1/rebuild", s.handleRebuild)
	s.mux.HandleFunc("GET /v1/stats", s.handleStats)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// Handler returns the top-level HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

// ---- request/response types -------------------------------------------------

type recordDTO struct {
	ID     string            `json:"id"`
	Vector []float32         `json:"vector"`
	Meta   map[string]string `json:"meta"`
}

type writeRequest struct {
	Records []recordDTO `json:"records"`
}

type deleteRequest struct {
	IDs []string `json:"ids"`
}

type searchRequest struct {
	Vector []float32         `json:"vector"`
	K      int               `json:"k"`
	Filter map[string]string `json:"filter"`
}

type hitDTO struct {
	ID    string  `json:"id"`
	Score float32 `json:"score"`
}

type searchResponse struct {
	Hits     []hitDTO `json:"hits"`
	IndexGen uint64   `json:"index_gen"`
	Explain  string   `json:"explain"`
}

// ---- handlers ---------------------------------------------------------------

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := decode(r, &req); err != nil {
		writeTypedError(w, err)
		return
	}
	recs := make([]model.Record, len(req.Records))
	for i, d := range req.Records {
		recs[i] = model.Record{ID: d.ID, Vector: d.Vector, Meta: d.Meta}
	}
	if err := s.eng.Write(recs); err != nil {
		writeTypedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"accepted": len(recs)})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req deleteRequest
	if err := decode(r, &req); err != nil {
		writeTypedError(w, err)
		return
	}
	if err := s.eng.Delete(req.IDs...); err != nil {
		writeTypedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": len(req.IDs)})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	var req searchRequest
	if err := decode(r, &req); err != nil {
		writeTypedError(w, err)
		return
	}
	f := model.Filter{Eq: req.Filter}
	hits, err := s.eng.Search(req.Vector, req.K, f)
	if err != nil {
		writeTypedError(w, err)
		return
	}
	out := searchResponse{
		Hits: make([]hitDTO, 0, len(hits)),
		Explain: "ranked by cosine similarity over one immutable point-in-time " +
			"index snapshot; deleted ids absent, updates expose only the new vector; " +
			"filter requires every key=value to match authoritative metadata",
	}
	for _, h := range hits {
		out.Hits = append(out.Hits, hitDTO{ID: h.ID, Score: h.Score})
	}
	out.IndexGen = s.eng.Stats().IndexGen
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRebuild(w http.ResponseWriter, r *http.Request) {
	if err := s.eng.Rebuild(r.Context()); err != nil {
		writeTypedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "compacted_and_checkpointed"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.Stats())
}

// ---- helpers ----------------------------------------------------------------

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errs.New(errs.KindInvalid, "malformed JSON: "+err.Error())
	}
	return nil
}

// writeTypedError maps a typed service error to a distinguishable HTTP
// status and stable error code so clients can tell invalid parameters,
// capacity/backpressure and temporary unavailability apart.
func writeTypedError(w http.ResponseWriter, err error) {
	var e *errs.Error
	if errors.As(err, &e) {
		status := http.StatusInternalServerError
		switch e.Kind {
		case errs.KindInvalid:
			status = http.StatusBadRequest
		case errs.KindCapacity:
			status = http.StatusTooManyRequests
		case errs.KindUnavailable:
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]any{"error": e.Kind.String(), "message": e.Message})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{
		"error": "internal_error", "message": err.Error(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
