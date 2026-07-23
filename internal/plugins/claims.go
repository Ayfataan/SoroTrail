// Package plugins loads and runs protocol-specific event decoders written
// as WebAssembly modules. Plugins are untrusted code; this package isolates
// them via wazero, enforces per-call time and memory budgets, and persists
// lossless fallback to the raw event when a plugin misbehaves.
//
// Two-tier model: a Manager reads a directory of .wasm files at startup,
// validates each module's ABI surface (a fixed set of exports), and runs
// matching plugins against every event between generic ScVal decoding and
// persistence. A buggy plugin must never stall ingestion or touch the
// database — the host owns all calls.
//
// ABI v1 is the long-lived commitment. Increment ABIVersion whenever the
// protocol below changes incompatibly.
package plugins

// ABIVersion is the version of the plugin ABI this host implements. A
// plugin that reports a higher number on startup is rejected with a
// clear error.
const ABIVersion uint32 = 1

// PluginMetadata identifies a plugin. Reported once at startup via the
// plugin_metadata export.
type PluginMetadata struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	ABIVersion uint32 `json:"abi_version"`
}

// Claims are the (contract ID, event symbol) pairs the plugin wants to
// see. Each list may be empty: an empty list means "no filter on this
// dimension" (wildcard); both empty is a wildcard match.
//
// Reported once at startup via the declare_claims export.
type Claims struct {
	Contracts []string `json:"contracts"`
	Topics    []string `json:"topics"`
}

// matches reports whether the (contractID, topicSymbol) pair is claimed.
// Either argument being "" means "no value on that dimension", which
// matches a wildcard plugin (empty claim list).
func (c Claims) matches(contractID, topicSymbol string) bool {
	if !matchField(c.Contracts, contractID) {
		return false
	}
	if !matchField(c.Topics, topicSymbol) {
		return false
	}
	return true
}

func matchField(claims []string, value string) bool {
	if len(claims) == 0 {
		return true
	}
	for _, c := range claims {
		if c == value {
			return true
		}
	}
	return false
}

// EventSymbolFromTopics extracts the conventional Soroban event symbol
// from the first topic if it's a JSON object shaped like
// {"symbol":"<name>"} (the scValToGo emission for ScValTypeScvSymbol).
// Returns "" when the topic isn't a symbol, which means no topic-filtered
// plugin can match.
func EventSymbolFromTopics(topicsJSON []byte) string {
	if len(topicsJSON) == 0 || topicsJSON[0] != '[' {
		return ""
	}
	const sym = `"symbol":"`
	i := bytesIndex(topicsJSON, []byte(sym))
	if i < 0 {
		return ""
	}
	start := i + len(sym)
	if start >= len(topicsJSON) {
		return ""
	}
	end := start
	for end < len(topicsJSON) && topicsJSON[end] != '"' {
		if topicsJSON[end] == '\\' && end+1 < len(topicsJSON) {
			end += 2
			continue
		}
		end++
	}
	if end > start {
		return string(topicsJSON[start:end])
	}
	return ""
}

// bytesEqual is the helper called by bytesIndex. Kept local so we
// don't pull in bytes just for one hot-path call.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// bytesIndex is a tiny byte-slice substring search for `sub` inside `s`.
// Used on the hot path of every event to find the conventional Soroban
// event symbol key, so it must not allocate.
func bytesIndex(s, sub []byte) int {
	if len(sub) == 0 {
		return 0
	}
	if len(sub) > len(s) {
		return -1
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i] != sub[0] {
			continue
		}
		if bytesEqual(s[i:i+len(sub)], sub) {
			return i
		}
	}
	return -1
}
