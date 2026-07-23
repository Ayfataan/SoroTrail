package plugins

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/tetratelabs/wazero"
)

// LogSeverity values the plugin passes as host_log's first uint32.
const (
	LogDebug uint32 = 0
	LogInfo  uint32 = 1
	LogWarn  uint32 = 2
	LogError uint32 = 3
)

// LogSink receives log messages a plugin emits via host_log. The host
// owns naming — plugin authors cannot impersonate other plugins.
type LogSink interface {
	PluginLog(pluginName string, severity uint32, msg string)
}

// compileWASM compiles .wasm bytes into a CompiledModule. Compile is the
// expensive step; subsequent InstantiateModule calls are cheap, which is
// why the manager compiles once at startup.
func compileWASM(ctx context.Context, rt wazero.Runtime, file string, r io.Reader) (wazero.CompiledModule, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%s: read: %w", file, err)
	}
	cm, err := rt.CompileModule(ctx, bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: compile: %w", file, err)
	}
	return cm, nil
}

// memoryPages returns the page count needed for `mib` mebibytes at the
// wazero standard 64KiB page size. Centralized so callers share the math;
// wazero's RuntimeConfig.WithMemoryLimitPages takes this value.
func memoryPages(mib int) uint32 {
	if mib <= 0 {
		mib = DefaultLimits.MemoryMiB
	}
	return uint32(mib) * 1024 * 1024 / 65536
}

// runtimeConfig returns a RuntimeConfig with memory-limit-pages set
// from our Limits.MemoryMiB. wazero v1.12 puts the memory limit on the
// runtime (not on ModuleConfig), so per-plugin limits are not possible
// in v1 — one runtime-wide cap is shared by all plugin modules.
func runtimeConfig(limits Limits) wazero.RuntimeConfig {
	rc := wazero.NewRuntimeConfig()
	if limits.MemoryMiB > 0 {
		rc = rc.WithMemoryLimitPages(memoryPages(limits.MemoryMiB))
	}
	return rc
}

// parseMetadata returns the absolute minimum subset of fields a plugin
// must report, validating that the JSON is sane. Used as a guard before
// instantiating worker modules so we don't keep bad metadata around.
func parseMetadata(body []byte) (PluginMetadata, error) {
	var meta PluginMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return meta, fmt.Errorf("plugin_metadata json: %w", err)
	}
	if meta.Name == "" {
		return meta, errors.New("plugin_metadata.name is empty")
	}
	if meta.ABIVersion == 0 {
		meta.ABIVersion = 1 // tolerate plugins that don't echo it
	}
	return meta, nil
}

// parseClaims returns the plugin's contract/topic filter lists. Empty
// body is treated as wildcards on both dimensions.
func parseClaims(body []byte) (Claims, error) {
	var claims Claims
	if len(body) == 0 {
		return claims, nil
	}
	if err := json.Unmarshal(body, &claims); err != nil {
		return claims, fmt.Errorf("declare_claims json: %w", err)
	}
	return claims, nil
}
