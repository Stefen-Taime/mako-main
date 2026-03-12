// Package vault provides an optional HashiCorp Vault client for secret resolution.
//
// The client is completely optional: if VAULT_ADDR is not set, New() returns nil
// and all operations are no-ops. This ensures zero impact on existing pipelines
// that don't use Vault.
//
// Authentication methods (via standard Vault env vars):
//   - Token:      VAULT_TOKEN
//   - AppRole:    VAULT_ROLE_ID + VAULT_SECRET_ID
//   - Kubernetes: VAULT_K8S_ROLE (reads service account token from /var/run/secrets/...)
//
// Usage:
//
//	client, err := vault.New()
//	if err != nil { ... }
//	// client may be nil if VAULT_ADDR is not set
//	val := vault.Get(client, "secret/data/mako/postgres", "password")
package vault

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
)

// Client wraps the HashiCorp Vault API client.
// It is safe for concurrent use and shared across all sinks.
type Client struct {
	client   *vaultapi.Client
	cache    sync.Map // path -> *cacheEntry
	ttl      time.Duration
	authType string // "token", "approle", "k8s" — for logging
}

// cacheEntry holds a cached set of secrets from a single Vault path.
type cacheEntry struct {
	data      map[string]any
	fetchedAt time.Time
}

// New creates a Vault client from environment variables.
// Returns (nil, nil) if VAULT_ADDR is not set — Vault is completely optional.
//
// Supported env vars:
//   - VAULT_ADDR:      Vault server URL (required to enable Vault)
//   - VAULT_TOKEN:     Token auth
//   - VAULT_ROLE_ID + VAULT_SECRET_ID: AppRole auth
//   - VAULT_K8S_ROLE:  Kubernetes auth
//   - VAULT_NAMESPACE: Vault namespace (enterprise)
//   - VAULT_CACERT:    Custom CA cert for TLS
func New() (*Client, error) {
	addr := os.Getenv("VAULT_ADDR")
	if addr == "" {
		return nil, nil // Vault not configured — completely transparent
	}

	cfg := vaultapi.DefaultConfig()
	cfg.Address = addr

	// VAULT_CACERT is handled automatically by DefaultConfig()

	apiClient, err := vaultapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("vault client: %w", err)
	}

	// Namespace (Vault Enterprise)
	if ns := os.Getenv("VAULT_NAMESPACE"); ns != "" {
		apiClient.SetNamespace(ns)
	}

	c := &Client{
		client: apiClient,
		ttl:    5 * time.Minute, // default cache TTL
	}

	// Authenticate
	if err := c.authenticate(); err != nil {
		return nil, fmt.Errorf("vault auth: %w", err)
	}

	fmt.Fprintf(os.Stderr, "[vault] connected to %s (auth: %s)\n", addr, c.authType)
	return c, nil
}

// SetTTL overrides the default cache TTL (5 minutes).
func (c *Client) SetTTL(ttl time.Duration) {
	if ttl > 0 {
		c.ttl = ttl
	}
}

// authenticate tries authentication methods in order:
// 1. Token (VAULT_TOKEN)
// 2. AppRole (VAULT_ROLE_ID + VAULT_SECRET_ID)
// 3. Kubernetes (VAULT_K8S_ROLE)
func (c *Client) authenticate() error {
	// 1. Token auth — simplest, VAULT_TOKEN is read by the SDK automatically
	if token := os.Getenv("VAULT_TOKEN"); token != "" {
		c.client.SetToken(token)
		c.authType = "token"
		return nil
	}

	// 2. AppRole auth
	roleID := os.Getenv("VAULT_ROLE_ID")
	secretID := os.Getenv("VAULT_SECRET_ID")
	if roleID != "" && secretID != "" {
		secret, err := c.client.Logical().Write("auth/approle/login", map[string]any{
			"role_id":   roleID,
			"secret_id": secretID,
		})
		if err != nil {
			return fmt.Errorf("approle login: %w", err)
		}
		if secret == nil || secret.Auth == nil {
			return fmt.Errorf("approle login: empty auth response")
		}
		c.client.SetToken(secret.Auth.ClientToken)
		c.authType = "approle"
		return nil
	}

	// 3. Kubernetes auth
	k8sRole := os.Getenv("VAULT_K8S_ROLE")
	if k8sRole != "" {
		jwt, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		if err != nil {
			return fmt.Errorf("k8s read service account token: %w", err)
		}
		secret, err := c.client.Logical().Write("auth/kubernetes/login", map[string]any{
			"role": k8sRole,
			"jwt":  string(jwt),
		})
		if err != nil {
			return fmt.Errorf("k8s login: %w", err)
		}
		if secret == nil || secret.Auth == nil {
			return fmt.Errorf("k8s login: empty auth response")
		}
		c.client.SetToken(secret.Auth.ClientToken)
		c.authType = "k8s"
		return nil
	}

	return fmt.Errorf("no auth method configured (set VAULT_TOKEN, VAULT_ROLE_ID+VAULT_SECRET_ID, or VAULT_K8S_ROLE)")
}

// Get reads a single key from a Vault path.
// Uses the cache with the configured TTL.
// Returns empty string if the key is not found or Vault is not configured.
func (c *Client) Get(path, key string) string {
	if c == nil {
		return ""
	}
	data := c.getSecrets(path)
	if data == nil {
		return ""
	}
	if v, ok := data[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// getSecrets fetches and caches all key/value pairs from a Vault path.
// The path should be the full logical path (e.g., "secret/data/mako/postgres").
func (c *Client) getSecrets(path string) map[string]any {
	// Check cache first
	if entry, ok := c.cache.Load(path); ok {
		ce := entry.(*cacheEntry)
		if time.Since(ce.fetchedAt) < c.ttl {
			return ce.data
		}
		// Cache expired — will re-fetch below
	}

	// Fetch from Vault
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	secret, err := c.client.Logical().ReadWithContext(ctx, path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[vault] error reading %s: %v\n", path, err)
		return nil
	}
	if secret == nil || secret.Data == nil {
		return nil
	}

	// KV v2 stores the actual data under the "data" sub-key.
	// KV v1 stores it directly in secret.Data.
	data := secret.Data
	if subData, ok := data["data"].(map[string]any); ok {
		data = subData
	}

	// Cache the result
	c.cache.Store(path, &cacheEntry{
		data:      data,
		fetchedAt: time.Now(),
	})

	keyCount := len(data)
	fmt.Fprintf(os.Stderr, "[vault] loaded secrets from %s (%d keys, ttl=%s)\n",
		path, keyCount, c.ttl)

	return data
}

// InvalidateCache removes a cached path so the next Get() re-fetches.
func (c *Client) InvalidateCache(path string) {
	if c != nil {
		c.cache.Delete(path)
	}
}

// AuthType returns the authentication method used ("token", "approle", "k8s").
func (c *Client) AuthType() string {
	if c == nil {
		return ""
	}
	return c.authType
}

// TTL returns the current cache TTL.
func (c *Client) TTL() time.Duration {
	if c == nil {
		return 0
	}
	return c.ttl
}
