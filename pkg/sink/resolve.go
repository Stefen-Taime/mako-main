package sink

import (
	"os"
	"time"

	"github.com/Stefen-Taime/mako/pkg/vault"
)

// ═══════════════════════════════════════════
// Secret Resolution Chain
// ═══════════════════════════════════════════

// vaultClient is the singleton Vault client shared across all sinks.
// It is nil when Vault is not configured (VAULT_ADDR not set).
var vaultClient *vault.Client

// vaultInitErr stores any error from Vault initialization.
var vaultInitErr error

// vaultOnce ensures the Vault client is initialized exactly once.
var vaultOnce initOnce

// initOnce is a simple sync.Once replacement that tracks whether init ran.
type initOnce struct {
	done bool
}

func (o *initOnce) do(f func()) {
	if !o.done {
		f()
		o.done = true
	}
}

// InitVault initializes the shared Vault client from environment variables.
// This is called lazily on the first Resolve() that might need Vault,
// or explicitly during pipeline startup.
// Returns nil if Vault is not configured.
func InitVault() (*vault.Client, error) {
	vaultOnce.do(func() {
		vaultClient, vaultInitErr = vault.New()
	})
	return vaultClient, vaultInitErr
}

// InitVaultWithTTL initializes the Vault client and sets the cache TTL
// from the pipeline-level vault.ttl configuration.
func InitVaultWithTTL(ttlStr string) (*vault.Client, error) {
	client, err := InitVault()
	if err != nil {
		return nil, err
	}
	if client != nil && ttlStr != "" {
		if d, err := time.ParseDuration(ttlStr); err == nil {
			client.SetTTL(d)
		}
	}
	return client, nil
}

// SetVaultClient sets the Vault client explicitly (for testing).
func SetVaultClient(c *vault.Client) {
	vaultClient = c
	vaultOnce.done = true
}

// ResetVault resets the Vault singleton (for testing).
func ResetVault() {
	vaultClient = nil
	vaultInitErr = nil
	vaultOnce.done = false
}

// Resolve returns the value for a configuration key using the resolution chain:
//  1. Explicit YAML config value (cfg[key])
//  2. Environment variable (envKey)
//  3. Vault secret (if vault_path is set in cfg, or if vaultPath is provided)
//  4. Default value
//
// This is the shared resolution function that ALL sinks should use.
// It replaces the previous envOrConfig() function.
func Resolve(cfg map[string]any, key, envKey, defaultVal string) string {
	// 1. Explicit config value
	if cfg != nil {
		if v, ok := cfg[key].(string); ok && v != "" {
			return v
		}
	}

	// 2. Environment variable
	if envKey != "" {
		if v := os.Getenv(envKey); v != "" {
			return v
		}
	}

	// 3. Vault (if configured)
	if vaultClient != nil && cfg != nil {
		if vaultPath, ok := cfg["vault_path"].(string); ok && vaultPath != "" {
			if v := vaultClient.Get(vaultPath, key); v != "" {
				return v
			}
		}
	}

	// 4. Default
	return defaultVal
}
