package vector

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

// Server 把 Store 暴露为本地 HTTP 接口。
type Server struct {
	store *Store
	mux   *http.ServeMux
}

func NewServer(s *Store) *Server {
	srv := &Server{store: s, mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (srv *Server) Handler() http.Handler { return srv.mux }

func (srv *Server) routes() {
	srv.mux.HandleFunc("GET /healthz", srv.handleHealth)
	srv.mux.HandleFunc("POST /v1/vectors", srv.handleUpsert)
	srv.mux.HandleFunc("POST /v1/vectors/batch", srv.handleBatch)
	srv.mux.HandleFunc("DELETE /v1/vectors/{id}", srv.handleDelete)
	srv.mux.HandleFunc("GET /v1/vectors/{id}", srv.handleGet)
	srv.mux.HandleFunc("POST /v1/search", srv.handleSearch)
	srv.mux.HandleFunc("POST /v1/rebuild", srv.handleRebuild)
	srv.mux.HandleFunc("GET /v1/stats", srv.handleStats)
}

type apiError struct {
	Error struct {
		Code    ErrorCode `json:"code"`
		Message string    `json:"message"`
	} `json:"error"`
}

// statusForCode 将稳定错误码映射为可区分的 HTTP 状态：
// 400 参数非法；507 容量不足；503 索引暂不可用；429 背压；404 不存在。
func statusForCode(c ErrorCode) int {
	switch c {
	case CodeInvalidArgument:
		return http.StatusBadRequest
	case CodeNotFound:
		return http.StatusNotFound
	case CodeCapacityExceeded:
		return http.StatusInsufficientStorage
	case CodeTooBusy:
		return http.StatusTooManyRequests
	case CodeIndexUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeErr(w http.ResponseWriter, err error) {
	var se *ServiceError
	if !errors.As(err, &se) {
		se = newError(CodeIndexUnavailable, "内部错误: %v", err)
	}
	var e apiError
	e.Error.Code = se.Code
	e.Error.Message = se.Message
	log.Printf("[reject] code=%s msg=%q", se.Code, se.Message)
	writeJSON(w, statusForCode(se.Code), e)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, newError(CodeInvalidArgument, "请求体不是合法 JSON: %v", err))
		return false
	}
	return true
}

func (srv *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (srv *Server) handleUpsert(w http.ResponseWriter, r *http.Request) {
	var rec Record
	if !decodeJSON(w, r, &rec) {
		return
	}
	if err := srv.store.Upsert(rec); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ack", "id": rec.ID})
}

type batchRequest struct {
	Records []Record `json:"records"`
}

func (srv *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := srv.store.BatchUpsert(req.Records); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ack",
		"applied": len(req.Records),
	})
}

func (srv *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := srv.store.Delete(id); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, ok := srv.store.Get(id)
	if !ok {
		writeErr(w, newError(CodeNotFound, "记录 %q 不存在", id))
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (srv *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	var req SearchRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	res, err := srv.store.Search(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (srv *Server) handleRebuild(w http.ResponseWriter, r *http.Request) {
	if err := srv.store.Rebuild(context.Background()); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, srv.store.Stats())
}

func (srv *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, srv.store.Stats())
}
