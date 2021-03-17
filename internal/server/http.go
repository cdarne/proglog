package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer creates a HTTP server able to produce and consume a log
func NewHTTPServer(addr string) *http.Server {
	srv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", srv.handleProduce).Methods("POST")
	r.HandleFunc("/", srv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ProduceRequest is the request params
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse is the produce response
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest is the request params
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse is the consume response
type ConsumeResponse struct {
	Record Record `json:"record"`
}
