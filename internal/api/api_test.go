package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

const (
	testContract  = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	testContract2 = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSD"
	testContract3 = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSE"
)

// stubStore returns canned values and records the filter it was queried with.
type stubStore struct {
	store.Store // panic on anything not stubbed below

	events     []store.Event
	nextCursor string
	queryErr   error
	lastFilter store.EventFilter

	event    store.Event
	eventErr error

	stats   store.Stats
	pingErr error
}

func (s *stubStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.lastFilter = f
	return s.events, s.nextCursor, s.queryErr
}

func (s *stubStore) GetEvent(context.Context, string) (store.Event, error) {
	return s.event, s.eventErr
}

func (s *stubStore) Stats(context.Context) (store.Stats, error) { return s.stats, nil }
func (s *stubStore) Ping(context.Context) error                 { return s.pingErr }

type stubRPC struct {
	rpc.Client

	health    rpc.Health
	healthErr error
}

func (s *stubRPC) GetHealth(context.Context) (rpc.Health, error) {
	return s.health, s.healthErr
}

func newTestServer(st *stubStore, rc *stubRPC) *Server {
	if rc == nil {
		rc = &stubRPC{health: rpc.Health{Status: "healthy"}}
	}
	return New(st, rc, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func doGet(t *testing.T, s *Server, path string) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + path)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return resp, body
}

func TestListEvents_ParsesFilters(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, body := doGet(t, s,
		"/events?contract_id="+testContract+`&type=contract&from_ledger=10&to_ledger=20&limit=5&topic={"symbol":"transfer"}`)

	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Equal(t, testContract, st.lastFilter.ContractID)
	assert.Equal(t, []string{"contract"}, st.lastFilter.Types)
	assert.Equal(t, int64(10), st.lastFilter.FromLedger)
	assert.Equal(t, int64(20), st.lastFilter.ToLedger)
	assert.Equal(t, 5, st.lastFilter.Limit)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(st.lastFilter.Topic))
}

func TestListEvents_BareTopicBecomesJSONString(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?topic=transfer")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `"transfer"`, string(st.lastFilter.Topic))
}

func TestListEvents_BadParams(t *testing.T) {
	for _, path := range []string{
		"/events?type=bogus",
		"/events?contract_id=nope",
		"/events?from_ledger=abc",
		"/events?from_ledger=20&to_ledger=10",
		"/events?limit=0",
		"/events?limit=99999",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := doGet(t, newTestServer(&stubStore{}, nil), path)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var e map[string]string
			require.NoError(t, json.Unmarshal(body, &e))
			assert.NotEmpty(t, e["error"])
		})
	}
}

func TestListEvents_MultiValueFilters(t *testing.T) {
	t.Run("comma-separated contract_ids", func(t *testing.T) {
		st := &stubStore{}
		resp, body := doGet(t, newTestServer(st, nil),
			"/events?contract_id="+testContract+","+testContract2)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
		assert.Empty(t, st.lastFilter.ContractID, "multiple IDs go to ContractIDs, not the scalar")
		assert.Equal(t, []string{testContract, testContract2}, st.lastFilter.ContractIDs)
	})

	t.Run("repeated contract_id params", func(t *testing.T) {
		st := &stubStore{}
		resp, _ := doGet(t, newTestServer(st, nil),
			"/events?contract_id="+testContract+"&contract_id="+testContract2)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, []string{testContract, testContract2}, st.lastFilter.ContractIDs)
	})

	t.Run("comma-separated types", func(t *testing.T) {
		st := &stubStore{}
		resp, body := doGet(t, newTestServer(st, nil), "/events?type=contract,system")
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
		assert.Equal(t, []string{"contract", "system"}, st.lastFilter.Types)
	})

	t.Run("repeated type params", func(t *testing.T) {
		st := &stubStore{}
		resp, _ := doGet(t, newTestServer(st, nil), "/events?type=contract&type=diagnostic")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, []string{"contract", "diagnostic"}, st.lastFilter.Types)
	})

	t.Run("single values behave exactly as before", func(t *testing.T) {
		st := &stubStore{}
		resp, _ := doGet(t, newTestServer(st, nil), "/events?contract_id="+testContract+"&type=contract")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testContract, st.lastFilter.ContractID)
		assert.Nil(t, st.lastFilter.ContractIDs)
		assert.Equal(t, []string{"contract"}, st.lastFilter.Types)
	})
}

func TestListEvents_RejectsAmbiguousMixing(t *testing.T) {
	for _, path := range []string{
		"/events?contract_id=" + testContract + "&contract_id=" + testContract2 + "," + testContract3,
		"/events?type=contract&type=system,diagnostic",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := doGet(t, newTestServer(&stubStore{}, nil), path)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, string(body), "ambiguous")
		})
	}
}

func TestListEvents_RejectsInvalidMultiValues(t *testing.T) {
	for _, path := range []string{
		"/events?contract_id=" + testContract + ",nope",
		"/events?type=contract,bogus",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := doGet(t, newTestServer(&stubStore{}, nil), path)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var e map[string]string
			require.NoError(t, json.Unmarshal(body, &e))
			assert.NotEmpty(t, e["error"])
		})
	}
}

func TestListEvents_CapsMultiValueList(t *testing.T) {
	// 21 shape-valid strkeys: more than the documented cap of 20.
	ids := make([]string, 0, 21)
	for i := 0; i < 21; i++ {
		ids = append(ids, testContract[:55]+string(rune('A'+i)))
	}
	resp, body := doGet(t, newTestServer(&stubStore{}, nil),
		"/events?contract_id="+strings.Join(ids, ","))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "at most 20 values")
}

func TestContractEvents_RejectsContractIDParam(t *testing.T) {
	st := &stubStore{}
	resp, body := doGet(t, newTestServer(st, nil),
		"/contracts/"+testContract+"/events?contract_id="+testContract2)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "contract_id is taken from the path")
	assert.Empty(t, st.lastFilter.ContractID, "store must not be queried for a rejected request")
}

func TestListEvents_ReturnsCursor(t *testing.T) {
	st := &stubStore{
		events:     []store.Event{{ID: "e1"}, {ID: "e2"}},
		nextCursor: "e2",
	}
	resp, body := doGet(t, newTestServer(st, nil), "/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Events []store.Event `json:"events"`
		Cursor string        `json:"cursor"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Len(t, out.Events, 2)
	assert.Equal(t, "e2", out.Cursor)
}

func TestGetEvent_NotFound(t *testing.T) {
	st := &stubStore{eventErr: store.ErrNotFound}
	resp, _ := doGet(t, newTestServer(st, nil), "/events/0000000000-0000000000")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestContractEvents_ForcesContractFilter(t *testing.T) {
	st := &stubStore{}
	resp, _ := doGet(t, newTestServer(st, nil), "/contracts/"+testContract+"/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, testContract, st.lastFilter.ContractID)

	resp, _ = doGet(t, newTestServer(st, nil), "/contracts/junk/events")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHealth(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/health")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("db down", func(t *testing.T) {
		st := &stubStore{pingErr: errors.New("connection refused")}
		resp, body := doGet(t, newTestServer(st, nil), "/health")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		assert.Contains(t, string(body), "connection refused")
	})
	t.Run("rpc down", func(t *testing.T) {
		rc := &stubRPC{healthErr: errors.New("rpc unreachable")}
		resp, _ := doGet(t, newTestServer(&stubStore{}, rc), "/health")
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	})
}

func TestStats(t *testing.T) {
	st := &stubStore{stats: store.Stats{TotalEvents: 42, LastIngestedLedger: 999}}
	resp, body := doGet(t, newTestServer(st, nil), "/stats")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got store.Stats
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, int64(42), got.TotalEvents)
	assert.Equal(t, int64(999), got.LastIngestedLedger)
}
