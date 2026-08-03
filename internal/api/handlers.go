package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

type errorResponse struct {
	Error string `json:"error"`
}

type eventsResponse struct {
	Events []store.Event `json:"events"`
	// Cursor is non-empty when another page exists; pass it back as ?cursor=.
	Cursor string `json:"cursor,omitempty"`
}

type healthResponse struct {
	Status string            `json:"status"` // ok | degraded
	Checks map[string]string `json:"checks"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: map[string]string{"database": "ok", "rpc": "ok"}}
	status := http.StatusOK

	if err := s.store.Ping(ctx); err != nil {
		resp.Status, resp.Checks["database"] = "degraded", err.Error()
		status = http.StatusServiceUnavailable
	}
	if health, err := s.rpc.GetHealth(ctx); err != nil {
		resp.Status, resp.Checks["rpc"] = "degraded", err.Error()
		status = http.StatusServiceUnavailable
	} else if health.Status != "healthy" {
		resp.Status, resp.Checks["rpc"] = "degraded", fmt.Sprintf("rpc reports %q", health.Status)
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	s.serveEvents(w, r, filter)
}

func (s *Server) handleContractEvents(w http.ResponseWriter, r *http.Request) {
	// The contract comes from the path. An explicit contract_id query
	// parameter would silently contradict (or silently agree with) it, so
	// reject it outright rather than guess what the caller meant.
	if r.URL.Query().Has("contract_id") {
		writeError(w, http.StatusBadRequest,
			errors.New("contract_id is taken from the path and must not be passed as a query parameter"))
		return
	}
	filter, err := filterFromQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}
	filter.ContractID = contractID
	s.serveEvents(w, r, filter)
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, filter store.EventFilter) {
	events, cursor, err := s.store.QueryEvents(r.Context(), filter)
	if err != nil {
		s.log.Error("querying events", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("querying events failed"))
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{Events: events, Cursor: cursor})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	event, err := s.store.GetEvent(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Errorf("event %q not found", id))
		return
	}
	if err != nil {
		s.log.Error("loading event", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading event failed"))
		return
	}
	writeJSON(w, http.StatusOK, event)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		s.log.Error("loading stats", "error", err)
		writeError(w, http.StatusInternalServerError, errors.New("loading stats failed"))
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// maxFilterValues caps how many values a single multi-value filter
// parameter (contract_id, type) may carry. Beyond this the request is a
// 400 — a list that long is more like a scan than a filter, and the cap
// keeps the generated `= ANY(...)` list bounded. Documented in the
// README's API reference.
const maxFilterValues = 20

// filterFromQuery parses the shared event-filter query params:
// contract_id, type, topic, from_ledger, to_ledger, cursor, limit.
//
// contract_id and type are multi-value. Values may be given as a single
// comma-separated list (?contract_id=C1,C2) or as repeated parameters
// (?contract_id=C1&contract_id=C2), but mixing the two styles in one
// request is ambiguous and rejected with a 400.
func filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()
	f := store.EventFilter{Cursor: q.Get("cursor")}

	var err error
	if f.ContractID, f.ContractIDs, err = parseMultiContractIDs(q); err != nil {
		return f, err
	}
	if f.Types, err = parseMultiTypes(q); err != nil {
		return f, err
	}

	// topic accepts any JSON value; a bare word like `transfer` is treated
	// as the JSON string "transfer". Matching is exact against the stored
	// topic entries, e.g. topic={"symbol":"transfer"} for XDR-decoded rows.
	if topic := q.Get("topic"); topic != "" {
		if json.Valid([]byte(topic)) {
			f.Topic = json.RawMessage(topic)
		} else {
			quoted, err := json.Marshal(topic)
			if err != nil {
				return f, fmt.Errorf("invalid topic: %w", err)
			}
			f.Topic = quoted
		}
	}

	if f.FromLedger, err = parseLedgerParam(q.Get("from_ledger"), "from_ledger"); err != nil {
		return f, err
	}
	if f.ToLedger, err = parseLedgerParam(q.Get("to_ledger"), "to_ledger"); err != nil {
		return f, err
	}
	if f.FromLedger > 0 && f.ToLedger > 0 && f.FromLedger > f.ToLedger {
		return f, fmt.Errorf("from_ledger %d is after to_ledger %d", f.FromLedger, f.ToLedger)
	}

	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > store.MaxQueryLimit {
			return f, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit)
		}
		f.Limit = limit
	}
	return f, nil
}

// parseMultiContractIDs reads the contract_id parameter. Exactly one ID is
// returned as the scalar ContractID (the historical single-value shape);
// more than one is returned as ContractIDs for a `= ANY(...)` query.
func parseMultiContractIDs(q url.Values) (single string, list []string, err error) {
	parts, err := multiValue(q, "contract_id", func(part string) error {
		if !config.ValidContractID(part) {
			return fmt.Errorf("invalid contract_id %q", part)
		}
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	switch len(parts) {
	case 0:
		return "", nil, nil
	case 1:
		return parts[0], nil, nil
	default:
		return "", parts, nil
	}
}

// parseMultiTypes reads the type parameter (contract|system|diagnostic).
func parseMultiTypes(q url.Values) ([]string, error) {
	return multiValue(q, "type", func(part string) error {
		switch part {
		case "contract", "system", "diagnostic":
			return nil
		default:
			return fmt.Errorf("invalid type %q (want contract|system|diagnostic)", part)
		}
	})
}

// multiValue flattens a query parameter that may appear either as a single
// comma-separated value (?p=a,b) or as repeated parameters (?p=a&p=b) into
// its list of values. Requests that combine both styles are ambiguous and
// rejected. validate runs on every element; an absent parameter (or one
// whose values all split to empty) yields no values at all.
func multiValue(q url.Values, name string, validate func(string) error) ([]string, error) {
	raw, ok := q[name]
	if !ok {
		return nil, nil
	}
	if len(raw) > 1 {
		for _, v := range raw {
			if strings.Contains(v, ",") {
				return nil, fmt.Errorf("ambiguous %s: combine repeated parameters and comma-separated values in one request", name)
			}
		}
	}
	var out []string
	for _, v := range raw {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part == "" {
				continue
			}
			if err := validate(part); err != nil {
				return nil, err
			}
			out = append(out, part)
		}
	}
	if len(out) > maxFilterValues {
		return nil, fmt.Errorf("%s accepts at most %d values, got %d", name, maxFilterValues, len(out))
	}
	return out, nil
}

func parseLedgerParam(raw, name string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
