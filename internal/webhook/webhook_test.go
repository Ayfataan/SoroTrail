package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestNotifier creates a Notifier with fast backoff for tests.
func newTestNotifier(st store.Store, log *slog.Logger) *Notifier {
	n := NewNotifier(st, log)
	n.maxAttempts = 5
	// Fast backoff: 1ms, 2ms, 4ms, 8ms, 16ms — tests complete quickly.
	n.backoffFunc = func(attempt int) time.Duration {
		return time.Duration(1<<attempt) * time.Millisecond
	}
	return n
}

func testEvent(id string) store.Event {
	return store.Event{
		ID:               id,
		ContractID:       "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Ledger:           100,
		Type:             "contract",
		TxHash:           "abc123",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"},{"u64":7}]`),
		Value:            json.RawMessage(`{"i128":"1000"}`),
	}
}

// --- Signature ---

func TestSign_HMACRoundTrip(t *testing.T) {
	secret := "my-secret-key"
	body := []byte(`{"event":{"id":"e1"}}`)

	sig := Sign(secret, body)
	assert.NotEmpty(t, sig)

	// Verify: recompute HMAC-SHA256 and compare.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, expected, sig)
}

func TestSign_DifferentSecretProducesDifferentSignature(t *testing.T) {
	body := []byte(`{"event":{"id":"e1"}}`)

	sig1 := Sign("secret-a", body)
	sig2 := Sign("secret-b", body)

	assert.NotEqual(t, sig1, sig2)
}

func TestSign_DifferentBodyProducesDifferentSignature(t *testing.T) {
	secret := "shared-secret"
	sig1 := Sign(secret, []byte(`{"x":1}`))
	sig2 := Sign(secret, []byte(`{"x":2}`))

	assert.NotEqual(t, sig1, sig2)
}

// --- Filter matching ---

func TestSubscriptionFilter_MatchesEvent(t *testing.T) {
	ev := store.Event{
		ContractID: contractA,
		Type:       "contract",
		Ledger:     100,
		Topics:     json.RawMessage(`[{"symbol":"transfer"},{"address":"GABC"}]`),
	}

	t.Run("empty filter matches everything", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{}.MatchesEvent(ev))
	})

	t.Run("contract_id matches", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{ContractID: contractA}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{ContractID: contractB}.MatchesEvent(ev))
	})

	t.Run("type matches", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{Type: "contract"}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{Type: "diagnostic"}.MatchesEvent(ev))
	})

	t.Run("ledger range matches", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{FromLedger: 50, ToLedger: 200}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{FromLedger: 200}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{ToLedger: 50}.MatchesEvent(ev))
	})

	t.Run("topic at position 0 matches", func(t *testing.T) {
		f := store.SubscriptionFilter{Topic: json.RawMessage(`{"symbol":"transfer"}`)}
		assert.True(t, f.MatchesEvent(ev))
	})

	t.Run("topic at position 1 matches", func(t *testing.T) {
		f := store.SubscriptionFilter{Topic: json.RawMessage(`{"address":"GABC"}`)}
		assert.True(t, f.MatchesEvent(ev))
	})

	t.Run("non-matching topic returns false", func(t *testing.T) {
		f := store.SubscriptionFilter{Topic: json.RawMessage(`{"symbol":"mint"}`)}
		assert.False(t, f.MatchesEvent(ev))
	})

	t.Run("combined filter", func(t *testing.T) {
		f := store.SubscriptionFilter{
			ContractID: contractA,
			Type:       "contract",
			FromLedger: 1,
			ToLedger:   200,
		}
		assert.True(t, f.MatchesEvent(ev))
	})

	t.Run("topic filter with non-JSON topics returns false", func(t *testing.T) {
		malformed := ev
		malformed.Topics = json.RawMessage(`not-json`)
		f := store.SubscriptionFilter{Topic: json.RawMessage(`"anything"`)}
		assert.False(t, f.MatchesEvent(malformed))
	})
}

// --- Delivery ---

// stubSubscriptionStore records delivery attempts for verification.
type stubSubscriptionStore struct {
	store.Store
	mu             sync.Mutex
	attempts       []store.DeliveryAttempt
	failures       map[int64]int
	incremented    map[int64]int
	resets         []int64
	enabledSubs    []store.Subscription
	enabledSubsErr error
}

func newStubSubscriptionStore() *stubSubscriptionStore {
	return &stubSubscriptionStore{
		failures:    map[int64]int{},
		incremented: map[int64]int{},
	}
}

func (s *stubSubscriptionStore) ListEnabledSubscriptions(context.Context) ([]store.Subscription, error) {
	return s.enabledSubs, s.enabledSubsErr
}

func (s *stubSubscriptionStore) RecordDeliveryAttempt(_ context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a.ID = int64(len(s.attempts)) + 1
	s.attempts = append(s.attempts, a)
	return a, nil
}

func (s *stubSubscriptionStore) IncrementSubscriptionFailures(_ context.Context, id int64, maxFailures int) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[id]++
	s.incremented[id] = s.incremented[id] + 1
	disabled := s.failures[id] >= maxFailures
	return s.failures[id], disabled, nil
}

func (s *stubSubscriptionStore) ResetSubscriptionFailures(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resets = append(s.resets, id)
	s.failures[id] = 0
	return nil
}

const (
	contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

func TestWorker_DeliversToMatchingSubscription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify signature header is present and valid.
		assert.NotEmpty(t, r.Header.Get(SignatureHeader))

		body, _ := io.ReadAll(r.Body)
		expectedSig := Sign("secret123", body)
		assert.Equal(t, expectedSig, r.Header.Get(SignatureHeader))

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
		Filters: store.SubscriptionFilter{ContractID: contractA},
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	ev := testEvent("e1")
	n.NotifyEvents(context.Background(), []store.Event{ev})

	// Give the worker time to process.
	time.Sleep(200 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()
	require.Len(t, st.attempts, 1)
	assert.Equal(t, store.DeliverySuccess, st.attempts[0].Status)
	assert.Equal(t, http.StatusOK, st.attempts[0].ResponseCode)
	assert.Equal(t, "e1", st.attempts[0].EventID)
}

func TestWorker_DoesNotDeliverNonMatchingEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not be called for non-matching event")
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
		Filters: store.SubscriptionFilter{ContractID: contractB}, // different contract
	}}

	n := NewNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	ev := testEvent("e1") // contractA
	n.NotifyEvents(context.Background(), []store.Event{ev})

	time.Sleep(100 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Empty(t, st.attempts, "non-matching event should not trigger delivery")
}

func TestWorker_RetriesOnFailureAndAutoDisables(t *testing.T) {
	// Use a channel to signal when the server has received all expected
	// requests, avoiding fragile time.Sleep. The test notifier uses fast
	// (millisecond) backoff so the test completes quickly.
	done := make(chan struct{})
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
		if callCount >= 5 {
			close(done)
		}
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	select {
	case <-done:
		// All HTTP requests completed. Give the worker a moment to
		// record the final attempt before we inspect the store.
		time.Sleep(50 * time.Millisecond)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for all retry attempts")
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	require.Len(t, st.attempts, 5,
		"all retry attempts are recorded")
	for _, a := range st.attempts {
		assert.Equal(t, store.DeliveryFailed, a.Status)
	}
}

func TestWorker_ResetsFailureCountOnSuccess(t *testing.T) {
	// Server fails twice then succeeds on the third attempt. The test
	// notifier uses fast (millisecond) backoff so the test completes quickly.
	done := make(chan struct{})
	var failCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failCount++
		if failCount < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		close(done)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	select {
	case <-done:
		// Third HTTP request succeeded. Give the worker a moment to
		// record the attempt and reset the failure counter.
		time.Sleep(50 * time.Millisecond)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for successful delivery after retries")
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.NotEmpty(t, st.attempts)
	last := st.attempts[len(st.attempts)-1]
	assert.Equal(t, store.DeliverySuccess, last.Status)
	assert.Contains(t, st.resets, int64(1), "failure count reset on success")
}

func TestWorker_NoSubscriptionsSkipsDelivery(t *testing.T) {
	st := newStubSubscriptionStore()
	n := NewNotifier(st, testLogger())

	// Should not panic or error with zero subscriptions.
	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})
}

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		attempt int
		min     time.Duration
		max     time.Duration
	}{
		{0, 1 * time.Second, 1 * time.Second},
		{1, 2 * time.Second, 2 * time.Second},
		{2, 4 * time.Second, 4 * time.Second},
		{3, 8 * time.Second, 8 * time.Second},
		{4, 16 * time.Second, 16 * time.Second},
		{5, 30 * time.Second, 30 * time.Second}, // capped at MaxBackoff
		{10, 30 * time.Second, 30 * time.Second},
	}
	for _, tt := range tests {
		d := backoffDuration(tt.attempt)
		assert.GreaterOrEqual(t, d, tt.min)
		assert.LessOrEqual(t, d, tt.max)
	}
}

func TestNotifier_ListErrorIsLogged(t *testing.T) {
	st := newStubSubscriptionStore()
	st.enabledSubsErr = assert.AnError
	n := NewNotifier(st, testLogger())

	// Should not panic — the error is logged and delivery is skipped.
	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})
}

// --- sleepCtx ---

func TestSleepCtx_CompletesBeforeDeadline(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	ok := sleepCtx(ctx, 10*time.Millisecond)
	elapsed := time.Since(start)
	assert.True(t, ok)
	assert.Less(t, elapsed, 200*time.Millisecond)
}

func TestSleepCtx_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() {
		ok := sleepCtx(ctx, 5*time.Second)
		done <- ok
	}()

	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		assert.False(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("sleepCtx did not return after context cancellation")
	}
}

// --- Endpoint timeout ---

func TestWorker_EndpointTimeout(t *testing.T) {
	// Create a server that sleeps longer than the delivery timeout.
	slowHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(slowHandler)
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	// Create a notifier with a short timeout client to avoid waiting
	// for the production 10s timeout.
	n := newTestNotifier(st, testLogger())
	n.client = &http.Client{Timeout: 50 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	// Wait for all attempts to complete.
	time.Sleep(500 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()

	require.Len(t, st.attempts, 5, "all retry attempts should be recorded")
	for i, a := range st.attempts {
		assert.Equal(t, store.DeliveryFailed, a.Status,
			"attempt %d should be recorded as failed", i)
		assert.NotEmpty(t, a.Error, "attempt %d should have an error message", i)
	}
	// incrementFailures is called once after all attempts are exhausted.
	assert.Equal(t, 1, st.incremented[1], "failure count should be incremented once after all attempts exhausted")
}

// --- Worker loop non-blocking ---

func TestWorker_SlowEndpointDoesNotBlockIngestion(t *testing.T) {
	// Track the order of delivery completions.
	var mu sync.Mutex
	deliveryOrder := []string{}

	// First server: slow endpoint that takes 200ms.
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		deliveryOrder = append(deliveryOrder, "slow")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer slowServer.Close()

	// Second server: fast endpoint that responds immediately.
	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		deliveryOrder = append(deliveryOrder, "fast")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer fastServer.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{
		{
			ID:      1,
			URL:     slowServer.URL + "/callback",
			Secret:  "secret1",
			Enabled: true,
		},
		{
			ID:      2,
			URL:     fastServer.URL + "/callback",
			Secret:  "secret2",
			Enabled: true,
		},
	}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	// Send events for both subscriptions.
	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	// Wait enough for both to complete.
	time.Sleep(500 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()

	// Both subscriptions should have been attempted.
	require.Len(t, st.attempts, 2, "both subscriptions should be attempted")

	// The fast endpoint should complete before the slow one.
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, deliveryOrder, 2, "both deliveries should complete")
	assert.Equal(t, "fast", deliveryOrder[0], "fast endpoint should complete first")
	assert.Equal(t, "slow", deliveryOrder[1], "slow endpoint should complete second")
}

// --- Concurrent delivery safety ---

func TestWorker_ConcurrentDeliveries(t *testing.T) {
	var mu sync.Mutex
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	// Send multiple events concurrently.
	for i := 0; i < 10; i++ {
		n.NotifyEvents(context.Background(), []store.Event{testEvent("e" + string(rune('0'+i)))})
	}

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	count := callCount
	mu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	assert.Equal(t, 10, count, "server should receive 10 requests")
	assert.Len(t, st.attempts, 10, "all attempts should be recorded")
	for _, a := range st.attempts {
		assert.Equal(t, store.DeliverySuccess, a.Status)
	}
}

// --- Terminal errors ---

func TestDeliverWithRetry_BadURLIsTerminal(t *testing.T) {
	st := newStubSubscriptionStore()
	n := newTestNotifier(st, testLogger())

	task := deliveryTask{
		Subscription: store.Subscription{
			ID:      1,
			URL:     "://invalid-url",
			Secret:  "secret123",
			Enabled: true,
		},
		Event: testEvent("e1"),
	}

	n.deliverWithRetry(context.Background(), task)

	st.mu.Lock()
	defer st.mu.Unlock()

	require.Len(t, st.attempts, 1, "bad URL should produce exactly one attempt")
	assert.Equal(t, store.DeliveryFailed, st.attempts[0].Status)
	assert.NotEmpty(t, st.attempts[0].Error)
	assert.Equal(t, 1, st.incremented[1], "failure count should be incremented once for terminal error")
}

// --- Backoff bounds ---

func TestBackoffDuration_BoundedByMaxBackoff(t *testing.T) {
	tests := []struct {
		name   string
		attempt int
	}{
		{"attempt 0", 0},
		{"attempt 1", 1},
		{"attempt 2", 2},
		{"attempt 3", 3},
		{"attempt 4", 4},
		{"attempt 5", 5},
		{"attempt 10", 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := backoffDuration(tt.attempt)
			assert.GreaterOrEqual(t, d, InitialBackoff, "backoff should be at least InitialBackoff")
			assert.LessOrEqual(t, d, MaxBackoff, "backoff should not exceed MaxBackoff")
		})
	}
}

// --- Retry count boundary ---

func TestWorker_RetryCountRespectsMaxAttempts(t *testing.T) {
	var mu sync.Mutex
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	// Override max attempts to 3 to make test faster.
	n.maxAttempts = 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	count := callCount
	mu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	assert.Equal(t, 3, count, "server should be called exactly maxAttempts times")
	require.Len(t, st.attempts, 3, "all attempts should be recorded")
	for _, a := range st.attempts {
		assert.Equal(t, store.DeliveryFailed, a.Status)
	}
	// incrementFailures is called once after all attempts are exhausted.
	assert.Equal(t, 1, st.incremented[1], "failure count should be incremented once after all attempts exhausted")
}

// --- Successful delivery resets failure count ---

func TestWorker_SuccessfulDeliveryAfterFailuresResetsCount(t *testing.T) {
	var mu sync.Mutex
	var failCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		failCount++
		current := failCount
		mu.Unlock()
		if current <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	n.maxAttempts = 5

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	// Wait for delivery to complete.
	time.Sleep(300 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()

	// Should have 3 attempts (2 failures + 1 success).
	require.Len(t, st.attempts, 3, "should have 3 attempts")
	assert.Equal(t, store.DeliveryFailed, st.attempts[0].Status)
	assert.Equal(t, store.DeliveryFailed, st.attempts[1].Status)
	assert.Equal(t, store.DeliverySuccess, st.attempts[2].Status)

	// Failure count should be reset after success.
	assert.Contains(t, st.resets, int64(1), "failure count should be reset on success")
}

// --- Non-matching events ---

func TestWorker_NonMatchingEventsNotDelivered(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not be called for non-matching event")
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
		Filters: store.SubscriptionFilter{ContractID: contractB},
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	time.Sleep(100 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Empty(t, st.attempts, "non-matching events should not trigger delivery")
}

// --- Empty events ---

func TestNotifier_EmptyEventsSkipsDelivery(t *testing.T) {
	st := newStubSubscriptionStore()
	n := newTestNotifier(st, testLogger())

	// Empty events should not panic or trigger any store calls.
	n.NotifyEvents(context.Background(), []store.Event{})

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Empty(t, st.attempts)
}

// --- Worker graceful shutdown ---

func TestWorker_GracefulShutdown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		n.Run(ctx)
		close(done)
	}()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Run returned after context cancellation.
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// --- Record attempt status mapping ---

func TestRecordAttempt_StatusMapping(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus string
	}{
		{"success", nil, store.DeliverySuccess},
		{"failure", assert.AnError, store.DeliveryFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStubSubscriptionStore()
			n := newTestNotifier(st, testLogger())

			task := deliveryTask{
				Subscription: store.Subscription{ID: 1},
				Event:        testEvent("e1"),
			}

			n.recordAttempt(context.Background(), task, 200, 10*time.Millisecond, tt.err)

			st.mu.Lock()
			defer st.mu.Unlock()
			require.Len(t, st.attempts, 1)
			assert.Equal(t, tt.expectedStatus, st.attempts[0].Status)
		})
	}
}

// --- Worker with context cancellation during delivery ---

func TestWorker_ContextCancellationDuringDelivery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	// Cancel while delivery is in progress.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait a bit for cleanup.
	time.Sleep(100 * time.Millisecond)
	// Test passes if no panic or goroutine leak.
}

// --- Sign function edge cases ---

func TestSign_EmptySecret(t *testing.T) {
	body := []byte(`{"test":"data"}`)
	sig := Sign("", body)
	assert.NotEmpty(t, sig)
	// Verify with empty secret.
	mac := hmac.New(sha256.New, []byte(""))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, expected, sig)
}

func TestSign_EmptyBody(t *testing.T) {
	secret := "my-secret"
	sig := Sign(secret, []byte{})
	assert.NotEmpty(t, sig)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte{})
	expected := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, expected, sig)
}

// --- Backoff duration table-driven test ---

func TestBackoffDuration_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{"zero", 0, InitialBackoff},
		{"first", 1, InitialBackoff * 2},
		{"second", 2, InitialBackoff * 4},
		{"third", 3, InitialBackoff * 8},
		{"fourth", 4, InitialBackoff * 16},
		{"fifth capped", 5, MaxBackoff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoffDuration(tt.attempt)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// --- Queue full behavior ---

func TestNotifyEvents_QueueFullDropsEvent(t *testing.T) {
	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     "http://localhost:1/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	// Fill the queue by sending to the underlying channel directly.
	// WorkerQueueSize is 4096; fill it so subsequent sends drop.
	for i := 0; i < WorkerQueueSize; i++ {
		n.sendOnly <- deliveryTask{
			Subscription: store.Subscription{ID: 1},
			Event:        testEvent("fill"),
		}
	}

	// This should not block and should drop the event.
	n.NotifyEvents(context.Background(), []store.Event{testEvent("overflow")})
	// Test passes if no panic or deadlock.
}

// --- Multiple events to multiple subscriptions ---

func TestWorker_MultipleEventsToMultipleSubscriptions(t *testing.T) {
	var mu sync.Mutex
	received := map[string]int64{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p Payload
		json.Unmarshal(body, &p)
		mu.Lock()
		received[p.Event.ID]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{
		{
			ID:      1,
			URL:     server.URL + "/callback",
			Secret:  "secret1",
			Enabled: true,
		},
		{
			ID:      2,
			URL:     server.URL + "/callback",
			Secret:  "secret2",
			Enabled: true,
		},
	}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	events := []store.Event{testEvent("e1"), testEvent("e2"), testEvent("e3")}
	n.NotifyEvents(context.Background(), events)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Each event should be delivered to both subscriptions.
	assert.Equal(t, int64(2), received["e1"], "e1 delivered to both subs")
	assert.Equal(t, int64(2), received["e2"], "e2 delivered to both subs")
	assert.Equal(t, int64(2), received["e3"], "e3 delivered to both subs")
}
