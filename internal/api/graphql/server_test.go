package graphql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every failure on the /graphql and /graphiql surface answers through
// writeErrorEnvelope, so a client can decode any refusal with the same
// {data, errors} type it uses for successful operations. These tests pin
// the two paths that used to answer with plain text via http.Error.
//
// Note this transport deliberately keeps its own envelope rather than the
// REST one: the GraphQL contract fixes the response shape, and switching
// to {"error": ...} would break every existing client.

func TestServeHTTPAnswersMethodNotAllowedInTheEnvelope(t *testing.T) {
	h := newGraphQLTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodPut, "/graphql", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req) // the real entry point mounted at /graphql

	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "GET, POST", rec.Header().Get("Allow"),
		"a client refused on method must be told what is accepted")
	assertGraphQLErrorEnvelope(t, rec)
}

func TestDisabledPlaygroundAnswersInTheEnvelope(t *testing.T) {
	// newGraphQLTestServer builds the handler with the playground off,
	// which is exactly the configuration that must refuse requests.
	h := newGraphQLTestServer(t, &stubStore{})

	rec := httptest.NewRecorder()
	h.PlaygroundHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/graphiql", nil))

	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assertGraphQLErrorEnvelope(t, rec)
}

// assertGraphQLErrorEnvelope checks the body is the one shape every
// writeErrorEnvelope call produces on this transport: data present (empty)
// and exactly one error carrying a message.
func assertGraphQLErrorEnvelope(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	var env struct {
		Data   map[string]any `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env),
		"error responses must be JSON: %s", rec.Body.String())
	assert.NotNil(t, env.Data, "the GraphQL envelope always carries data")
	require.Len(t, env.Errors, 1, "an operation-level failure carries one error")
	assert.NotEmpty(t, env.Errors[0].Message)
}
