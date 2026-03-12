// Package duckdbext provides shared DuckDB extension helpers
// used by both source and sink packages.
package duckdbext

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/Stefen-Taime/mako/pkg/vault"
)

// CloudConfig holds credentials for S3, GCS, and Azure Blob Storage
// that DuckDB needs to access remote files via httpfs.
type CloudConfig struct {
	// S3
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
	S3Endpoint        string
	S3URLStyle        string // "path" or "vhost"

	// GCS (HMAC keys for S3-compatible access)
	GCSHMACAccessID string // HMAC access ID (like AWS access key)
	GCSHMACSecret   string // HMAC secret (like AWS secret key)

	// Azure
	AzureAccountName string
	AzureAccountKey  string
	AzureConnectionString string
}

// CloudConfigFromMap builds a CloudConfig from a YAML config map,
// falling back to environment variables.
func CloudConfigFromMap(cfg map[string]any) CloudConfig {
	return CloudConfig{
		// S3
		S3AccessKeyID:     resolve(cfg, "s3_access_key_id", "AWS_ACCESS_KEY_ID", ""),
		S3SecretAccessKey: resolve(cfg, "s3_secret_access_key", "AWS_SECRET_ACCESS_KEY", ""),
		S3Region:          resolve(cfg, "s3_region", "AWS_REGION", "us-east-1"),
		S3Endpoint:        resolve(cfg, "s3_endpoint", "AWS_ENDPOINT_URL", ""),
		S3URLStyle:        resolve(cfg, "s3_url_style", "", "path"),

		// GCS (HMAC keys)
		GCSHMACAccessID: resolve(cfg, "gcs_hmac_access_id", "GCS_HMAC_ACCESS_ID", ""),
		GCSHMACSecret:   resolve(cfg, "gcs_hmac_secret", "GCS_HMAC_SECRET", ""),

		// Azure
		AzureAccountName:      resolve(cfg, "azure_account_name", "AZURE_STORAGE_ACCOUNT", ""),
		AzureAccountKey:       resolve(cfg, "azure_account_key", "AZURE_STORAGE_KEY", ""),
		AzureConnectionString: resolve(cfg, "azure_connection_string", "AZURE_STORAGE_CONNECTION_STRING", ""),
	}
}

// CloudConfigWithVault builds a CloudConfig like CloudConfigFromMap, but also
// checks Vault for GCS HMAC credentials. If the Vault secret contains
// "hmac_access_id" and "hmac_secret" fields, they are used for DuckDB's
// GCS authentication via the S3-compatible API.
func CloudConfigWithVault(cfg map[string]any, vc *vault.Client) CloudConfig {
	cc := CloudConfigFromMap(cfg)

	// If GCS HMAC keys are already set via config or env, no need to check Vault
	if cc.GCSHMACAccessID != "" {
		return cc
	}

	// Try Vault
	if vc == nil {
		return cc
	}

	vaultPath := resolve(cfg, "vault_path", "", "")
	if vaultPath == "" {
		return cc
	}

	accessID := vc.Get(vaultPath, "hmac_access_id")
	secret := vc.Get(vaultPath, "hmac_secret")
	if accessID != "" && secret != "" {
		cc.GCSHMACAccessID = accessID
		cc.GCSHMACSecret = secret
		fmt.Fprintf(os.Stderr, "[duckdb] loaded GCS HMAC credentials from Vault (%s)\n", vaultPath)
	}

	return cc
}

// NeedsHTTPFS returns true if the given path references a remote cloud store.
func NeedsHTTPFS(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasPrefix(lower, "s3://") ||
		strings.HasPrefix(lower, "s3a://") ||
		strings.HasPrefix(lower, "gs://") ||
		strings.HasPrefix(lower, "gcs://") ||
		strings.HasPrefix(lower, "az://") ||
		strings.HasPrefix(lower, "azure://") ||
		strings.HasPrefix(lower, "abfss://") ||
		strings.HasPrefix(lower, "https://storage.googleapis.com") ||
		strings.HasPrefix(lower, "https://") && strings.Contains(lower, ".blob.core.windows.net") ||
		strings.HasPrefix(lower, "https://") && strings.Contains(lower, ".s3.amazonaws.com")
}

// NeedsHTTPFSQuery returns true if a SQL query likely references remote paths.
// This is a heuristic check for common patterns like read_parquet('s3://...').
func NeedsHTTPFSQuery(query string) bool {
	lower := strings.ToLower(query)
	return strings.Contains(lower, "s3://") ||
		strings.Contains(lower, "s3a://") ||
		strings.Contains(lower, "gs://") ||
		strings.Contains(lower, "gcs://") ||
		strings.Contains(lower, "az://") ||
		strings.Contains(lower, "azure://") ||
		strings.Contains(lower, "abfss://") ||
		strings.Contains(lower, ".blob.core.windows.net") ||
		strings.Contains(lower, ".s3.amazonaws.com") ||
		strings.Contains(lower, "storage.googleapis.com")
}

// LoadCloudExtensions installs and loads the httpfs extension, then configures
// cloud credentials (S3, GCS, Azure) if provided. This should be called in
// Open() after the DuckDB connection is established.
//
// The label parameter is used for log messages (e.g. "source", "sink").
func LoadCloudExtensions(ctx context.Context, db *sql.DB, cc CloudConfig, label string) error {
	// Install and load httpfs
	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return fmt.Errorf("duckdb %s: httpfs extension: %w", label, err)
	}
	fmt.Fprintf(os.Stderr, "[duckdb] %s: loaded httpfs extension\n", label)

	// S3 credentials
	if cc.S3AccessKeyID != "" {
		setCmds := []string{
			fmt.Sprintf("SET s3_access_key_id = '%s'", escapeSQLString(cc.S3AccessKeyID)),
			fmt.Sprintf("SET s3_secret_access_key = '%s'", escapeSQLString(cc.S3SecretAccessKey)),
			fmt.Sprintf("SET s3_region = '%s'", escapeSQLString(cc.S3Region)),
		}
		if cc.S3Endpoint != "" {
			setCmds = append(setCmds, fmt.Sprintf("SET s3_endpoint = '%s'", escapeSQLString(cc.S3Endpoint)))
			setCmds = append(setCmds, fmt.Sprintf("SET s3_url_style = '%s'", escapeSQLString(cc.S3URLStyle)))
		}
		for _, cmd := range setCmds {
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				return fmt.Errorf("duckdb %s: s3 config: %w", label, err)
			}
		}
		fmt.Fprintf(os.Stderr, "[duckdb] %s: configured S3 credentials (region=%s)\n", label, cc.S3Region)
	}

	// GCS credentials (HMAC keys via CREATE SECRET)
	if cc.GCSHMACAccessID != "" {
		cmd := fmt.Sprintf(
			"CREATE SECRET gcs_secret (TYPE GCS, KEY_ID '%s', SECRET '%s')",
			escapeSQLString(cc.GCSHMACAccessID),
			escapeSQLString(cc.GCSHMACSecret),
		)
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			fmt.Fprintf(os.Stderr, "[duckdb] %s: gcs secret config: %v (continuing)\n", label, err)
		} else {
			fmt.Fprintf(os.Stderr, "[duckdb] %s: configured GCS credentials (HMAC)\n", label)
		}
	}

	// Azure credentials
	if cc.AzureConnectionString != "" {
		cmd := fmt.Sprintf("SET azure_storage_connection_string = '%s'", escapeSQLString(cc.AzureConnectionString))
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			return fmt.Errorf("duckdb %s: azure config: %w", label, err)
		}
		fmt.Fprintf(os.Stderr, "[duckdb] %s: configured Azure credentials (connection string)\n", label)
	} else if cc.AzureAccountName != "" {
		setCmds := []string{
			fmt.Sprintf("SET azure_account_name = '%s'", escapeSQLString(cc.AzureAccountName)),
		}
		if cc.AzureAccountKey != "" {
			setCmds = append(setCmds, fmt.Sprintf("SET azure_account_key = '%s'", escapeSQLString(cc.AzureAccountKey)))
		}
		for _, cmd := range setCmds {
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				return fmt.Errorf("duckdb %s: azure config: %w", label, err)
			}
		}
		fmt.Fprintf(os.Stderr, "[duckdb] %s: configured Azure credentials (account=%s)\n", label, cc.AzureAccountName)
	}

	return nil
}

// escapeSQLString escapes single quotes for use in DuckDB SET commands.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// resolve reads a value from the config map, then falls back to an
// environment variable, then to a default value.
func resolve(cfg map[string]any, key, envKey, defaultVal string) string {
	if cfg != nil {
		if v, ok := cfg[key].(string); ok && v != "" {
			return v
		}
	}
	if envKey != "" {
		if v := os.Getenv(envKey); v != "" {
			return v
		}
	}
	return defaultVal
}

// ═══════════════════════════════════════════
// GCS ADC proxy — signed URL rewriting
// ═══════════════════════════════════════════

// gcsPathRe matches gs://bucket/path patterns inside SQL strings (single-quoted).
// It captures the full gs://... URL between quotes.
var gcsPathRe = regexp.MustCompile(`'(gs://[^']+)'`)

// NeedsGCSSignedURLs returns true if a query references gs:// paths and
// no direct credentials are configured (meaning DuckDB httpfs can't auth natively).
func NeedsGCSSignedURLs(query string, cc CloudConfig) bool {
	if cc.GCSHMACAccessID != "" {
		return false // HMAC keys available — DuckDB can handle it via CREATE SECRET
	}
	lower := strings.ToLower(query)
	return strings.Contains(lower, "gs://") || strings.Contains(lower, "gcs://")
}

// RewriteGCSToSignedURLs replaces gs://bucket/path patterns in a SQL query
// with short-lived signed HTTPS URLs generated via the Go GCS SDK (ADC).
// This allows DuckDB httpfs to read GCS objects without a service account key,
// using any ADC credential type (authorized_user, workload identity, etc.).
//
// Glob patterns (e.g., gs://bucket/dir/*.parquet) are expanded by listing
// matching objects and replacing the single glob path with a list of signed URLs.
//
// Returns the rewritten query and a cleanup function (currently a no-op).
func RewriteGCSToSignedURLs(ctx context.Context, query string) (string, error) {
	matches := gcsPathRe.FindAllStringSubmatchIndex(query, -1)
	if len(matches) == 0 {
		return query, nil
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("gcs signed url: create client: %w", err)
	}
	defer client.Close()

	// Process matches in reverse order so replacement indices stay valid.
	result := query
	for i := len(matches) - 1; i >= 0; i-- {
		// submatch group 1 = the gs:// URL without quotes
		gsURL := result[matches[i][2]:matches[i][3]]

		signedURLs, err := resolveGCSPath(ctx, client, gsURL)
		if err != nil {
			return "", fmt.Errorf("gcs signed url: %w", err)
		}

		var replacement string
		if len(signedURLs) == 1 {
			replacement = "'" + signedURLs[0] + "'"
		} else {
			// Multiple files from glob expansion → DuckDB list syntax
			quoted := make([]string, len(signedURLs))
			for j, u := range signedURLs {
				quoted[j] = "'" + u + "'"
			}
			replacement = "[" + strings.Join(quoted, ", ") + "]"
		}

		// Replace the full match (including quotes)
		result = result[:matches[i][0]] + replacement + result[matches[i][1]:]
	}

	fmt.Fprintf(os.Stderr, "[duckdb] rewrote gs:// paths to signed URLs (ADC proxy mode)\n")
	return result, nil
}

// resolveGCSPath handles a single gs://bucket/path, expanding globs if needed,
// and returns signed URLs for each matching object.
func resolveGCSPath(ctx context.Context, client *storage.Client, gsURL string) ([]string, error) {
	bucket, path, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, err
	}

	bkt := client.Bucket(bucket)

	// Check if the path contains a glob pattern.
	if strings.ContainsAny(path, "*?[") {
		return expandGlobAndSign(ctx, bkt, bucket, path)
	}

	// Single object — sign directly.
	url, err := signObject(ctx, bkt, bucket, path)
	if err != nil {
		return nil, err
	}
	return []string{url}, nil
}

// parseGCSURL extracts bucket and object path from gs://bucket/path or gcs://bucket/path.
func parseGCSURL(gsURL string) (bucket, path string, err error) {
	u := gsURL
	u = strings.TrimPrefix(u, "gs://")
	u = strings.TrimPrefix(u, "gcs://")

	idx := strings.IndexByte(u, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("invalid GCS path %q: no object path", gsURL)
	}
	return u[:idx], u[idx+1:], nil
}

// expandGlobAndSign lists objects matching a glob pattern and signs each one.
func expandGlobAndSign(ctx context.Context, bkt *storage.BucketHandle, bucket, pattern string) ([]string, error) {
	// Extract the prefix before the first glob character for efficient listing.
	prefix := pattern
	for i, c := range pattern {
		if c == '*' || c == '?' || c == '[' {
			prefix = pattern[:i]
			break
		}
	}

	// Convert glob to regex for matching.
	globRegex := globToRegex(pattern)
	re, err := regexp.Compile("^" + globRegex + "$")
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
	}

	var urls []string
	it := bkt.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs list gs://%s/%s: %w", bucket, pattern, err)
		}

		if re.MatchString(attrs.Name) {
			url, err := signObject(ctx, bkt, bucket, attrs.Name)
			if err != nil {
				return nil, err
			}
			urls = append(urls, url)
		}
	}

	if len(urls) == 0 {
		return nil, fmt.Errorf("no objects matched gs://%s/%s", bucket, pattern)
	}

	fmt.Fprintf(os.Stderr, "[duckdb] expanded gs://%s/%s → %d objects\n", bucket, pattern, len(urls))
	return urls, nil
}

// signObject generates a signed URL for a single GCS object (15 min TTL).
func signObject(ctx context.Context, bkt *storage.BucketHandle, bucket, object string) (string, error) {
	url, err := bkt.SignedURL(object, &storage.SignedURLOptions{
		Method:  "GET",
		Expires: time.Now().Add(15 * time.Minute),
	})
	if err != nil {
		return "", fmt.Errorf("sign gs://%s/%s: %w", bucket, object, err)
	}
	return url, nil
}

// globToRegex converts a simple glob pattern to a regex.
// Supports *, ?, and basic character classes.
func globToRegex(glob string) string {
	var b strings.Builder
	for i := 0; i < len(glob); i++ {
		switch glob[i] {
		case '*':
			// ** matches across path separators (zero or more segments), * does not
			if i+1 < len(glob) && glob[i+1] == '*' {
				i++ // skip second *
				// If followed by /, consume it — ** already covers the separator
				if i+1 < len(glob) && glob[i+1] == '/' {
					i++ // skip /
					b.WriteString("(.+/)?")
				} else {
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '.', '+', '^', '$', '|', '(', ')', '{', '}':
			b.WriteByte('\\')
			b.WriteByte(glob[i])
		case '[':
			// Pass through character classes as-is
			b.WriteByte('[')
		case ']':
			b.WriteByte(']')
		default:
			b.WriteByte(glob[i])
		}
	}
	return b.String()
}
