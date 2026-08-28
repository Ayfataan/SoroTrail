// Package store persists contract events and ingestion state in Postgres.
//
// The primary abstraction is the [Store] interface: the ingester, auditor,
// and API layer depend on it rather than on Postgres directly, so
// alternative backends can be contributed by implementing the interface.
//
// Sentinel errors:
//   - [ErrNotFound] – returned by lookups that match no rows ([Store.GetEvent],
//     [Store.GetIngestionState], [Store.GetAuditState], [Store.ListOpenFindingsByRange]).
//   - [ErrReplayLocked] – returned when another replay holds the advisory lock.
package store

import (
	"context"
	"encoding/json"
	"time"
)

// Event is a Soroban contract event as persisted by SoroTrail.
//
// Fields are populated by the ingester from Stellar RPC getEvents responses
// and stored durably in Postgres. The ingester upserts events by [Event.ID],
// so re-scans and restarts never produce duplicates.
//
// The raw XDR fields ([Event.RawTopicXDR], [Event.RawValueXDR]) preserve
// the RPC's base64-encoded XDR so that an improved decoder can re-derive
// [Event.Topics] and [Event.Value] later without an RPC round-trip (see
// internal/replay). They are excluded from the JSON API representation.
type Event struct {
	// ID is the RPC's TOID-based event identifier. IDs are zero-padded, so
	// their lexicographic order matches chronological order — pagination and
	// cursors rely on this.
	ID string `json:"id"`

	// ContractID is the Stellar contract address that emitted the event,
	// e.g. "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC".
	ContractID string `json:"contract_id"`

	// Ledger is the ledger sequence number in which the event was emitted.
	Ledger int64 `json:"ledger"`

	// Type is the event type as reported by the RPC: "contract", "system",
	// or "diagnostic".
	Type string `json:"type"`

	// TxHash is the transaction hash that produced the event.
	TxHash string `json:"tx_hash"`

	// TxIndex is the zero-based index of the transaction within the ledger.
	TxIndex int32 `json:"tx_index"`

	// OpIndex is the zero-based index of the operation within the transaction.
	OpIndex int32 `json:"op_index"`

	// InSuccessfulCall indicates whether the event was emitted during a
	// successful contract invocation. When false the event was part of a
	// failed execution.
	InSuccessfulCall bool `json:"in_successful_call"`

	// Topics is the event's topics array, stored as JSON. When the RPC
	// supports xdrFormat: "json" the decoded JSON is stored verbatim;
	// otherwise the base64 XDR is decoded locally into shapes like
	// {"symbol":"transfer"}, {"u64":42}, {"address":"C..."}.
	Topics json.RawMessage `json:"topics"`

	// Value is the event's value, stored as JSON. The same decoding
	// strategy as [Event.Topics] applies.
	Value json.RawMessage `json:"value"`

	// CreatedAt is the time the row was inserted into the database.
	CreatedAt time.Time `json:"created_at"`

	// RawTopicXDR keeps the base64-encoded topic XDR the RPC delivered.
	// Populated when the RPC returns XDR rather than decoded JSON; empty
	// when the RPC already returned JSON or for rows ingested before raw
	// XDR storage was added. Not part of the API representation.
	RawTopicXDR []string `json:"-"`

	// RawValueXDR keeps the base64-encoded value XDR the RPC delivered.
	// Same semantics as [Event.RawTopicXDR]. Not part of the API
	// representation.
	RawValueXDR string `json:"-"`
}

// DecodedEvent is one event's replayable payload: the raw XDR inputs plus
// the decoded columns currently stored for it. It is used by the replay
// engine (internal/replay) to re-derive Topics and Value from the raw XDR.
type DecodedEvent struct {
	// ID is the event's TOID-based identifier (same as [Event.ID]).
	ID string

	// ContractID is the Stellar contract address that emitted the event.
	ContractID string

	// Ledger is the ledger sequence number in which the event was emitted.
	Ledger int64

	// RawTopicXDR is the base64-encoded topic XDR from the RPC. Empty when
	// the event was ingested with decoded JSON or before raw XDR storage.
	RawTopicXDR []string

	// RawValueXDR is the base64-encoded value XDR the RPC delivered.
	// Same semantics as [DecodedEvent.RawTopicXDR].
	RawValueXDR string

	// Topics is the currently-stored decoded topics, as JSON.
	Topics json.RawMessage

	// Value is the currently-stored decoded value, as JSON.
	Value json.RawMessage
}

// HasRawXDR reports whether the event carries enough raw XDR to be
// replayed. Rows without it are skipped (and counted) rather than treated
// as errors by the replay engine.
func (d DecodedEvent) HasRawXDR() bool {
	return len(d.RawTopicXDR) > 0 || d.RawValueXDR != ""
}

// ReplayState is the single persisted progress row for the replay tool.
// It records which range is being replayed and how far through it the tool
// has progressed. LastEventID is the last event whose rewrite committed;
// replay resumes at the first event after it.
//
// The state row is a singleton (id = 1) in the replay_state table.
// CompletedAt being non-nil indicates the run finished its whole range
// (see [ReplayState.Done]).
type ReplayState struct {
	// FromLedger is the inclusive start of the ledger range being replayed.
	FromLedger int64

	// ToLedger is the inclusive end of the ledger range being replayed.
	ToLedger int64

	// LastEventID is the ID of the last event whose rewrite committed.
	// The next batch starts after this ID.
	LastEventID string

	// Processed is the total number of events examined so far.
	Processed int64

	// Changed is the number of events whose decoded columns actually
	// differed from what was stored.
	Changed int64

	// Skipped is the number of events skipped because they lack raw XDR.
	Skipped int64

	// StartedAt records when the current replay run was initialized.
	StartedAt time.Time

	// UpdatedAt records when progress was last persisted.
	UpdatedAt time.Time

	// CompletedAt is non-nil when the run finished its whole range. Nil
	// means the run is still in progress or was interrupted.
	CompletedAt *time.Time
}

// Done reports whether the recorded run finished its whole range.
func (s ReplayState) Done() bool { return s.CompletedAt != nil }

// EventFilter narrows a [Store.QueryEvents] call. Zero values mean "no
// constraint" — all filters are optional and combinable.
//
// A zero [EventFilter] returns all events in ascending ID order with the
// default page size (see [DefaultQueryLimit]).
type EventFilter struct {
	// ContractID, when non-empty, restricts results to events from this
	// contract address.
	ContractID string

	// Type, when non-empty, restricts results to events of this type
	// ("contract", "system", or "diagnostic").
	Type string

	// Topic matches events whose topics JSON array contains this JSON value
	// at any position (Postgres jsonb containment). A bare word is treated
	// as a JSON string by the API layer.
	Topic json.RawMessage

	// FromLedger is the inclusive lower bound on the event's ledger. 0
	// means no lower bound.
	FromLedger int64

	// ToLedger is the inclusive upper bound on the event's ledger. 0 means
	// no upper bound.
	ToLedger int64

	// Cursor is the ID of the last event from the previous page. The next
	// page starts after (ascending) or before (descending) this cursor. An
	// empty string starts from the beginning (ascending) or end (descending).
	Cursor string

	// Limit is the maximum number of events to return. Capped at
	// [MaxQueryLimit]. When 0, [DefaultQueryLimit] is used.
	Limit int

	// Order is "asc" or "desc". Defaults to "asc" (oldest-first) for
	// backward compatibility.
	Order string
}

// IngestionState tracks how far ingestion has progressed. There is a
// single row (id = 1) in the ingestion_state table that the ingester
// updates after each successful poll.
type IngestionState struct {
	// LastIngestedLedger is the highest ledger sequence number that has
	// been fully ingested. The ingester resumes from this ledger on
	// startup.
	LastIngestedLedger int64

	// LastCursor is the opaque RPC cursor from the last getEvents call.
	// Used to resume ingestion without re-fetching already-seen events.
	LastCursor string

	// UpdatedAt records when this state was last written.
	UpdatedAt time.Time
}

// AuditState tracks how far the background auditor has verified stored
// ranges against the RPC. There is a single row (id = 1) in the
// audit_state table.
//
// VerifiedThroughLedger is the inclusive highest ledger whose stored
// events have been proven to match a fresh getEvents fetch. When the
// auditor is disabled (AUDIT_ENABLED=false), this stays at 0.
type AuditState struct {
	// VerifiedThroughLedger is the inclusive highest ledger whose events
	// have been verified against the RPC. 0 means no ledger has been
	// verified yet.
	VerifiedThroughLedger int64

	// UpdatedAt records when this state was last written.
	UpdatedAt time.Time
}

// LedgerCensus is one row of a per-ledger census over a contiguous
// range. It represents the stored event count (and optionally the full
// ID list) for a single ledger that contains at least one event.
//
// Ledgers with zero stored events are omitted from the census.
type LedgerCensus struct {
	// Ledger is the ledger sequence number.
	Ledger int64

	// Count is the number of stored events in this ledger.
	Count int

	// IDs is the lexicographically sorted list of stored event IDs in the
	// ledger. Populated only when the census is requested with idsOnly=true
	// (the full-diff path). Empty in a count-only census (the cheap
	// "all good" sweep).
	IDs []string
}

// Finding statuses the auditor records in audit_findings. A finding
// progresses through these states as the auditor attempts repair.
const (
	// FindingOpen indicates a newly detected mismatch that has not yet
	// been repaired.
	FindingOpen = "open"
	// FindingRepaired indicates the mismatch was corrected by
	// re-ingesting the affected range.
	FindingRepaired = "repaired"
	// FindingUnverifiable indicates the finding's ledger range aged out
	// of the RPC's retention window before repair could succeed. The
	// finding cannot be verified or repaired.
	FindingUnverifiable = "unverifiable"
	// FindingUnrecoverable indicates the RPC kept returning different
	// events for the same range across AUDIT_MAX_REPAIR_ATTEMPTS
	// iterations. The finding remains visible so operators can
	// investigate.
	FindingUnrecoverable = "unrecoverable"
)

// AuditFinding is one outstanding mismatch the auditor found between the
// store and the RPC. The range [FromLedger, ToLedger] is closed (both
// inclusive) and is bounded by AUDIT_FINDING_MAX_LEDGERS so a single
// finding is a tractable repair target.
//
// The auditor creates findings with status [FindingOpen] and transitions
// them through [FindingRepaired], [FindingUnverifiable], or
// [FindingUnrecoverable] as repair is attempted.
type AuditFinding struct {
	// ID is the auto-assigned primary key (set by [Store.RecordAuditFinding]).
	ID int64

	// FromLedger is the inclusive start of the ledger range where the
	// mismatch was detected.
	FromLedger int64

	// ToLedger is the inclusive end of the ledger range where the mismatch
	// was detected.
	ToLedger int64

	// ExpectedCount is the RPC event count for [FromLedger, ToLedger].
	ExpectedCount int

	// ActualCount is the stored event count before repair was attempted.
	ActualCount int

	// MissingIDs lists event IDs the RPC reported but the store was
	// missing (may be empty when the mismatch was an extra orphan rather
	// than a gap).
	MissingIDs []string

	// Status is one of [FindingOpen], [FindingRepaired],
	// [FindingUnverifiable], or [FindingUnrecoverable].
	Status string

	// Attempts is the number of repair iterations so far.
	Attempts int

	// LastAttemptedAt records when the last repair attempt was made. Zero
	// value means no attempt has been made yet.
	LastAttemptedAt time.Time

	// LastError contains the error message from the most recent failed
	// repair attempt, if any.
	LastError string

	// CreatedAt records when the finding was first recorded.
	CreatedAt time.Time
}

// Stats summarizes what the indexer has stored so far. Returned by
// [Store.Stats].
type Stats struct {
	// TotalEvents is the total number of stored events across all
	// contracts.
	TotalEvents int64 `json:"total_events"`

	// LastIngestedLedger is the highest ledger the ingester has processed.
	// 0 means ingestion has not started.
	LastIngestedLedger int64 `json:"last_ingested_ledger"`

	// VerifiedThroughLedger is the inclusive highest ledger whose stored
	// events have been confirmed to match a fresh RPC fetch by the
	// auditor. 0 means no ledger has been verified yet (auditor disabled
	// or no passes completed).
	VerifiedThroughLedger int64 `json:"verified_through_ledger"`

	// ContractCount is the number of distinct contracts with at least one
	// stored event.
	ContractCount int64 `json:"contract_count"`

	// WatchedContracts is the number of contracts in the watched_contracts
	// table. 0 means all contracts are being ingested (no filter).
	WatchedContracts int64 `json:"watched_contracts"`

	// Auditor holds audit-specific counters. Populated only when the audit
	// package is active; omitted from JSON when the auditor is nil.
	Auditor AuditStats `json:"auditor,omitempty"`
}

// AuditStats is a JSON-friendly view of audit.Metrics. Defined here so
// json.Marshal sees concrete field tags (avoiding mapstructure work in
// the API layer).
type AuditStats struct {
	// PassesRun is the number of audit passes completed.
	PassesRun uint64 `json:"passes_run"`

	// LedgersChecked is the total number of ledgers verified against the
	// RPC across all passes.
	LedgersChecked uint64 `json:"ledgers_checked"`

	// FindingsOpened is the total number of audit findings created.
	FindingsOpened uint64 `json:"findings_opened"`

	// FindingsRepaired is the total number of findings resolved by
	// successful re-ingestion.
	FindingsRepaired uint64 `json:"findings_repaired"`

	// FindingsUnverifiable is the number of findings that aged out of the
	// RPC's retention window before repair.
	FindingsUnverifiable uint64 `json:"findings_unverifiable"`

	// FindingsUnrecoverable is the number of findings where the RPC kept
	// returning inconsistent data across repair attempts.
	FindingsUnrecoverable uint64 `json:"findings_unrecoverable"`

	// RPCRequests is the total number of RPC requests made by the auditor.
	RPCRequests uint64 `json:"rpc_requests"`
}

// ReplayBatch is one transactional unit of replay work: the rewritten
// decoded columns for a batch of events, every dependent table re-derived
// from them, and the progress marker to advance.
//
// Contributors: derivation order is part of this type's contract.
// Dependent tables are derived from the *new* Events decoding, never from
// what is currently in the database, so a replay is a pure function of
// the raw XDR. When a dependent table lands (e.g. token_events from a
// SEP-41 decoder) add it as a field here and write it in
// [Postgres.CommitReplayBatch] after Events — that keeps the order in
// one place instead of spread across call sites.
type ReplayBatch struct {
	// Events are the events-table rewrites, applied first because every
	// dependent table reads from them.
	Events []EventDecoding

	// State is the progress marker, written last and in the same
	// transaction, so committed progress can never run ahead of committed
	// rewrites.
	State ReplayState
}

// EventDecoding is a freshly decoded events row, keyed by event ID. It
// contains only the columns that the replay engine rewrites.
type EventDecoding struct {
	// ID is the event's TOID-based identifier (same as [Event.ID]).
	ID string

	// Topics is the newly decoded topics JSON.
	Topics json.RawMessage

	// Value is the newly decoded value JSON.
	Value json.RawMessage
}

// Store is the persistence boundary. The ingester, auditor, and API depend
// on this interface, never on Postgres directly, so alternative backends
// can be contributed by implementing it.
//
// Replay-specific persistence is deliberately not part of this interface —
// it lives on [*Postgres] and is consumed through the narrower interface in
// internal/replay, so backends that don't need a maintenance replay tool
// aren't forced to implement one.
//
// All methods accept a [context.Context] and return an error as their last
// return value. Methods that look up a single row return [ErrNotFound] when
// no matching row exists.
type Store interface {
	// UpsertEvents inserts events idempotently (duplicates by [Event.ID]
	// are ignored) and returns the number of newly inserted rows. Passing
	// an empty slice returns (0, nil) without touching the database.
	//
	// This is the primary write path for ingestion. For auditor repairs
	// that need orphan deletion, see [Store.ReplaceEventsInRange].
	UpsertEvents(ctx context.Context, events []Event) (int64, error)

	// ReplaceEventsInRange atomically makes the ledger range
	// [fromLedger, toLedger] in the store exactly match events: orphans
	// (rows in that range no longer appearing in events) are deleted,
	// missing events are inserted, and same-ID rows are updated so
	// topic/value mutations on the RPC side are corrected.
	//
	// Designed for targeted repair from the auditor; ingest uses
	// [Store.UpsertEvents] instead because it is cheaper (no orphans to
	// delete). Raw XDR already stored for a surviving event is preserved
	// when the incoming event carries none, so a repair never costs a
	// row its replayability.
	//
	// The entire operation runs in a single Postgres transaction.
	ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error

	// GetEvent returns the event with the given id, or [ErrNotFound] if
	// no event with that ID exists.
	GetEvent(ctx context.Context, id string) (Event, error)

	// QueryEvents returns a page of events matching the given filter,
	// ordered by ID in the direction specified by [EventFilter.Order]
	// (default ascending = oldest-first).
	//
	// The second return value is an opaque cursor: the ID of the last
	// event in the page, to be passed as [EventFilter.Cursor] for the
	// next page. An empty cursor means there are no more results.
	//
	// Page size is [EventFilter.Limit], capped at [MaxQueryLimit]
	// (default [DefaultQueryLimit]). A zero limit uses the default.
	//
	// A zero-value [EventFilter] returns all events in ascending ID order.
	QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error)

	// LedgerRangeCensus returns one [LedgerCensus] row per ledger in the
	// inclusive [fromLedger, toLedger] range that contains at least one
	// stored event, in ascending ledger order. Ledgers with zero events
	// are omitted.
	//
	// When idsOnly is false the response contains only counts (the cheap
	// path used for the common "all good" verify sweep). When idsOnly is
	// true each [LedgerCensus.IDs] is populated with the lexicographically
	// sorted list of stored event IDs in that ledger (used to diff a
	// ledger whose count disagrees with the RPC).
	//
	// Returns an empty slice (not an error) when no ledgers in the range
	// contain events.
	LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error)

	// GetIngestionState returns the current ingestion progress, or
	// [ErrNotFound] if no state has been saved yet (fresh database).
	GetIngestionState(ctx context.Context) (IngestionState, error)

	// SaveIngestionState upserts the singleton ingestion state row,
	// replacing all fields unconditionally.
	SaveIngestionState(ctx context.Context, s IngestionState) error

	// GetAuditState returns the current audit high-water mark, or
	// [ErrNotFound] if no state has been saved yet (fresh database or
	// auditor disabled).
	GetAuditState(ctx context.Context) (AuditState, error)

	// SaveAuditState upserts the singleton audit state row, replacing all
	// fields unconditionally.
	SaveAuditState(ctx context.Context, s AuditState) error

	// SaveAuditStateIfGreater atomically sets verified_through_ledger only
	// when the provided ledger is strictly greater than the stored value.
	// Returns the post-write [AuditState] whether or not it was modified.
	//
	// The atomic WHERE clause ensures concurrent auditors can't regress
	// the high-water mark even if they race.
	SaveAuditStateIfGreater(ctx context.Context, ledger int64) (AuditState, error)

	// ListWatchedContracts returns the contract IDs in the
	// watched_contracts table, sorted lexicographically. An empty slice
	// means all contracts are being ingested (no filter).
	ListWatchedContracts(ctx context.Context) ([]string, error)

	// AddWatchedContract inserts a contract ID into the watched_contracts
	// table. Re-adding an existing contract is a no-op (ON CONFLICT DO
	// NOTHING). The contractID must be a valid Stellar contract address
	// (starts with "C").
	AddWatchedContract(ctx context.Context, contractID string) error

	// RecordAuditFinding persists a new finding with status
	// [FindingOpen] and returns it with its assigned [AuditFinding.ID]
	// and [AuditFinding.CreatedAt] populated by the database.
	RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error)

	// UpdateAuditFinding overwrites the mutable fields (status, attempts,
	// last_attempted_at, last_error, missing_ids) of an existing finding.
	// The finding is identified by [AuditFinding.ID].
	UpdateAuditFinding(ctx context.Context, f AuditFinding) error

	// ListOpenFindingsByRange returns the most recent open or
	// unrecoverable finding whose range overlaps [fromLedger, toLedger],
	// or [ErrNotFound] if no live finding spans the range.
	//
	// The auditor uses this to detect an in-progress repair for a range
	// before recording a new finding.
	ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (AuditFinding, error)

	// Stats returns an aggregate summary of the stored data: total events,
	// last ingested ledger, verified-through ledger, contract counts, and
	// audit counters.
	Stats(ctx context.Context) (Stats, error)

	// Ping checks connectivity to the underlying Postgres database. Returns
	// nil on success, or an error if the database is unreachable.
	Ping(ctx context.Context) error
}
