package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

func TestMetrics_ServesRequestDurationHistogram(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	// Generate a sample on a known route before scraping.
	resp, _ := doGet(t, s, "/health")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := doGet(t, s, "/metrics")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	text := string(body)
	assert.Contains(t, text, "http_request_duration_seconds")
	assert.Contains(t, text, `route="/health"`)
	assert.Contains(t, text, `method="GET"`)
	assert.Contains(t, text, `status="200"`)
}

func TestMetrics_ExemptFromRateLimit(t *testing.T) {
	assert.True(t, exemptPaths["/metrics"], "/metrics must stay exempt from the rate limiter")
}

// TestMetrics_ExposesIngestionLagGauge asserts the /metrics endpoint serves
// the ingestion-lag gauge (#237): latest RPC ledger minus last ingested.
func TestMetrics_ExposesIngestionLagGauge(t *testing.T) {
	metrics.IngestionLag.Set(21)
	defer metrics.IngestionLag.Set(0)

	s := newTestServer(&stubStore{}, nil)
	resp, body := doGet(t, s, "/metrics")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "sorotrail_ingestion_lag_ledgers 21")
}

func TestServer_routePattern(t *testing.T) {
	s := &Server{}

	tests := []struct {
		name     string
		setup    func(*http.Request)
		path     string
		expected string
	}{
		{
			name:     "no chi context",
			path:     "/unknown",
			expected: "/unknown",
		},
		{
			name: "matched route returns chi pattern",
			path: "/events/123",
			setup: func(r *http.Request) {
				rctx := chi.NewRouteContext()
				rctx.RoutePatterns = []string{"/events/{id}"}
				*r = *r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
			},
			expected: "/events/{id}",
		},
		{
			name: "unmatched request falls back to raw path",
			path: "/unmatched",
			setup: func(r *http.Request) {
				rctx := chi.NewRouteContext()
				*r = *r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
			},
			expected: "/unmatched",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, tt.path, nil)
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(req)
			}

			assert.Equal(t, tt.expected, s.routePattern(req))
		})
	}
}
