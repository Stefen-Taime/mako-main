package source

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/sink"
	"golang.org/x/time/rate"
)

// ═══════════════════════════════════════════
// HTTP/API Source
// ═══════════════════════════════════════════

// HTTPSource polls REST APIs, handles pagination, authentication, and rate limiting.
// Supports one-shot fetch and periodic polling.
//
// YAML configuration:
//
//	source:
//	  type: http
//	  config:
//	    url: https://api.example.com/v1/users
//	    method: GET
//	    auth_type: bearer
//	    auth_token: ${API_TOKEN}
//	    response_type: json
//	    data_path: data.results
//	    pagination_type: offset
//	    pagination_limit: 100
//	    poll_interval: 5m
//	    rate_limit_rps: 10
//	    timeout: 30s
type HTTPSource struct {
	// Endpoint
	url         string
	method      string
	headers     map[string]string
	body        string
	contentType string

	// Authentication
	authType      string // none, bearer, basic, api_key, oauth2
	authToken     string
	authUser      string
	authPassword  string
	apiKeyHeader  string
	apiKeyValue   string
	oauth2URL     string
	oauth2ID      string
	oauth2Secret      string
	oauth2ContentType string // "form" (default, RFC 6749) or "json"
	oauth2Token       string // cached OAuth2 token
	oauth2Expiry  time.Time

	// Response parsing
	responseType string // json, jsonl, csv
	dataPath     string // dot-notation path to records array

	// Pagination
	paginationType      string // none, offset, cursor, next_url, page
	paginationLimitParam  string
	paginationLimit       int
	paginationOffsetParam string
	paginationCursorParam string
	paginationCursorPath  string
	paginationNextURLPath string
	paginationPageParam   string
	paginationTotalPath   string
	paginationHasMorePath string

	// Polling
	pollInterval        time.Duration
	pollIncrementalParam string
	pollIncrementalPath  string
	lastIncrementalValue string

	// Rate limiting & retries
	rateLimiter *rate.Limiter
	timeout     time.Duration
	maxRetries  int

	// Runtime
	config  map[string]any
	client  *http.Client
	eventCh chan *pipeline.Event
	lag     atomic.Int64
	closed  atomic.Bool
	wg      sync.WaitGroup
}

// NewHTTPSource creates an HTTP source from config.
func NewHTTPSource(cfg map[string]any) *HTTPSource {
	srcURL := sink.Resolve(cfg, "url", "HTTP_SOURCE_URL", "")
	method := strings.ToUpper(strFromConfig(cfg, "method", "GET"))
	body := strFromConfig(cfg, "body", "")
	contentType := strFromConfig(cfg, "content_type", "application/json")

	// Parse headers
	headers := make(map[string]string)
	if h, ok := cfg["headers"].(map[string]any); ok {
		for k, v := range h {
			if s, ok := v.(string); ok {
				headers[k] = s
			}
		}
	}

	// Authentication
	authType := strFromConfig(cfg, "auth_type", "none")
	authToken := sink.Resolve(cfg, "auth_token", "HTTP_AUTH_TOKEN", "")
	authUser := sink.Resolve(cfg, "auth_user", "HTTP_AUTH_USER", "")
	authPassword := sink.Resolve(cfg, "auth_password", "HTTP_AUTH_PASSWORD", "")
	apiKeyHeader := strFromConfig(cfg, "api_key_header", "X-API-Key")
	apiKeyValue := sink.Resolve(cfg, "api_key_value", "HTTP_API_KEY", "")
	oauth2URL := strFromConfig(cfg, "oauth2_token_url", "")
	oauth2ID := sink.Resolve(cfg, "oauth2_client_id", "OAUTH_CLIENT_ID", "")
	oauth2Secret := sink.Resolve(cfg, "oauth2_client_secret", "OAUTH_CLIENT_SECRET", "")
	oauth2ContentType := strFromConfig(cfg, "oauth2_content_type", "form")

	// Response parsing
	responseType := strFromConfig(cfg, "response_type", "json")
	dataPath := strFromConfig(cfg, "data_path", "")

	// Pagination
	paginationType := strFromConfig(cfg, "pagination_type", "none")
	paginationLimitParam := strFromConfig(cfg, "pagination_limit_param", "limit")
	paginationLimit := intFromConfig(cfg, "pagination_limit", 100)
	paginationOffsetParam := strFromConfig(cfg, "pagination_offset_param", "offset")
	paginationCursorParam := strFromConfig(cfg, "pagination_cursor_param", "cursor")
	paginationCursorPath := strFromConfig(cfg, "pagination_cursor_path", "")
	paginationNextURLPath := strFromConfig(cfg, "pagination_next_url_path", "")
	paginationPageParam := strFromConfig(cfg, "pagination_page_param", "page")
	paginationTotalPath := strFromConfig(cfg, "pagination_total_path", "")
	paginationHasMorePath := strFromConfig(cfg, "pagination_has_more_path", "")

	// Polling
	pollIntervalStr := strFromConfig(cfg, "poll_interval", "0")
	pollInterval, _ := time.ParseDuration(pollIntervalStr)
	pollIncrementalParam := strFromConfig(cfg, "poll_incremental_param", "")
	pollIncrementalPath := strFromConfig(cfg, "poll_incremental_path", "")

	// Rate limiting
	rps := intFromConfig(cfg, "rate_limit_rps", 10)
	burst := intFromConfig(cfg, "rate_limit_burst", 20)
	limiter := rate.NewLimiter(rate.Limit(rps), burst)

	// Timeout & retries
	timeoutStr := strFromConfig(cfg, "timeout", "30s")
	timeout, _ := time.ParseDuration(timeoutStr)
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	maxRetries := intFromConfig(cfg, "max_retries", 3)

	return &HTTPSource{
		url:                   srcURL,
		method:                method,
		headers:               headers,
		body:                  body,
		contentType:           contentType,
		authType:              authType,
		authToken:             authToken,
		authUser:              authUser,
		authPassword:          authPassword,
		apiKeyHeader:          apiKeyHeader,
		apiKeyValue:           apiKeyValue,
		oauth2URL:             oauth2URL,
		oauth2ID:              oauth2ID,
		oauth2Secret:          oauth2Secret,
		oauth2ContentType:     oauth2ContentType,
		responseType:          responseType,
		dataPath:              dataPath,
		paginationType:        paginationType,
		paginationLimitParam:  paginationLimitParam,
		paginationLimit:       paginationLimit,
		paginationOffsetParam: paginationOffsetParam,
		paginationCursorParam: paginationCursorParam,
		paginationCursorPath:  paginationCursorPath,
		paginationNextURLPath: paginationNextURLPath,
		paginationPageParam:   paginationPageParam,
		paginationTotalPath:   paginationTotalPath,
		paginationHasMorePath: paginationHasMorePath,
		pollInterval:          pollInterval,
		pollIncrementalParam:  pollIncrementalParam,
		pollIncrementalPath:   pollIncrementalPath,
		rateLimiter:           limiter,
		timeout:               timeout,
		maxRetries:            maxRetries,
		config:                cfg,
		client:                &http.Client{Timeout: timeout},
		eventCh:               make(chan *pipeline.Event, 1000),
	}
}

// URL returns the configured URL (exported for testing).
func (s *HTTPSource) URL() string { return s.url }

// Method returns the configured HTTP method (exported for testing).
func (s *HTTPSource) Method() string { return s.method }

// AuthType returns the configured auth type (exported for testing).
func (s *HTTPSource) AuthType() string { return s.authType }

// ResponseType returns the configured response type (exported for testing).
func (s *HTTPSource) ResponseType() string { return s.responseType }

// DataPath returns the configured data path (exported for testing).
func (s *HTTPSource) DataPath() string { return s.dataPath }

// PaginationType returns the configured pagination type (exported for testing).
func (s *HTTPSource) PaginationType() string { return s.paginationType }

// PollInterval returns the configured poll interval (exported for testing).
func (s *HTTPSource) PollInterval() time.Duration { return s.pollInterval }

// RateLimiter returns the rate limiter (exported for testing).
func (s *HTTPSource) RateLimiter() *rate.Limiter { return s.rateLimiter }

// SetClient sets a custom HTTP client (for testing).
func (s *HTTPSource) SetClient(c *http.Client) { s.client = c }

// Open validates the HTTP source configuration.
func (s *HTTPSource) Open(ctx context.Context) error {
	if s.url == "" {
		return fmt.Errorf("http source: url is required (set config.url)")
	}
	if _, err := url.Parse(s.url); err != nil {
		return fmt.Errorf("http source: invalid url: %w", err)
	}

	// Verify URL is reachable (network-level check only).
	// We only check that the server responds — HTTP status codes (4xx, 5xx)
	// are handled by the Read() loop with retries, not here.
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodHead, s.url, nil)
	if err != nil {
		return fmt.Errorf("http source: invalid url %q: %w", s.url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http source: url unreachable %q: %w", s.url, err)
	}
	resp.Body.Close()

	fmt.Fprintf(os.Stderr, "[http] source: %s %s (auth=%s, pagination=%s, poll=%s)\n",
		s.method, s.url, s.authType, s.paginationType, s.pollInterval)
	return nil
}

// Read starts the HTTP source and returns the event channel.
func (s *HTTPSource) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	s.wg.Add(1)
	go s.readLoop(ctx)
	return s.eventCh, nil
}

// readLoop handles one-shot or periodic polling.
func (s *HTTPSource) readLoop(ctx context.Context) {
	defer s.wg.Done()
	defer func() {
		if s.closed.CompareAndSwap(false, true) {
			close(s.eventCh)
		}
	}()

	var offset int64

	// First fetch
	if err := s.fetchAndEmit(ctx, &offset); err != nil {
		fmt.Fprintf(os.Stderr, "[http] fetch error: %v\n", err)
	}

	// If no polling, we're done
	if s.pollInterval <= 0 {
		return
	}

	// Polling loop
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.fetchAndEmit(ctx, &offset); err != nil {
				fmt.Fprintf(os.Stderr, "[http] poll error: %v\n", err)
			}
		}
	}
}

// fetchAndEmit fetches all pages and emits events.
func (s *HTTPSource) fetchAndEmit(ctx context.Context, offset *int64) error {
	records, err := s.fetchAllPages(ctx)
	if err != nil {
		return err
	}

	for _, record := range records {
		// Track incremental value for next poll
		if s.pollIncrementalPath != "" {
			if v, ok := getNestedValue(record, s.pollIncrementalPath); ok {
				if sv, ok := v.(string); ok {
					s.lastIncrementalValue = sv
				} else {
					s.lastIncrementalValue = fmt.Sprintf("%v", v)
				}
			}
		}

		// Extract key (use "id" field if present)
		key := ""
		if v, ok := record["id"]; ok {
			key = fmt.Sprintf("%v", v)
		}

		event := &pipeline.Event{
			Key:       []byte(key),
			Value:     record,
			Timestamp: time.Now(),
			Topic:     s.url,
			Offset:    *offset,
		}

		select {
		case s.eventCh <- event:
			*offset++
			s.lag.Store(int64(len(s.eventCh)))
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

// fetchAllPages fetches all pages using the configured pagination strategy.
func (s *HTTPSource) fetchAllPages(ctx context.Context) ([]map[string]any, error) {
	switch s.paginationType {
	case "offset":
		return s.fetchPaginatedOffset(ctx)
	case "cursor":
		return s.fetchPaginatedCursor(ctx)
	case "next_url":
		return s.fetchPaginatedNextURL(ctx)
	case "page":
		return s.fetchPaginatedPage(ctx)
	default:
		// No pagination — single fetch
		return s.fetchSinglePage(ctx, s.url)
	}
}

// fetchPaginatedOffset implements offset-based pagination.
func (s *HTTPSource) fetchPaginatedOffset(ctx context.Context) ([]map[string]any, error) {
	var allRecords []map[string]any
	offset := 0

	for {
		pageURL := s.buildPageURL(map[string]string{
			s.paginationOffsetParam: strconv.Itoa(offset),
			s.paginationLimitParam:  strconv.Itoa(s.paginationLimit),
		})

		records, fullBody, err := s.fetchPageWithBody(ctx, pageURL)
		if err != nil {
			return allRecords, err
		}

		allRecords = append(allRecords, records...)

		if len(records) < s.paginationLimit {
			break
		}

		// Check total if available
		if s.paginationTotalPath != "" && fullBody != nil {
			if total, ok := getNestedValue(fullBody, s.paginationTotalPath); ok {
				totalVal := toInt(total)
				if totalVal > 0 && offset+s.paginationLimit >= totalVal {
					break
				}
			}
		}

		offset += s.paginationLimit
	}

	return allRecords, nil
}

// fetchPaginatedCursor implements cursor-based pagination.
func (s *HTTPSource) fetchPaginatedCursor(ctx context.Context) ([]map[string]any, error) {
	var allRecords []map[string]any
	cursor := ""

	for {
		params := map[string]string{
			s.paginationLimitParam: strconv.Itoa(s.paginationLimit),
		}
		if cursor != "" {
			params[s.paginationCursorParam] = cursor
		}

		pageURL := s.buildPageURL(params)
		records, fullBody, err := s.fetchPageWithBody(ctx, pageURL)
		if err != nil {
			return allRecords, err
		}

		allRecords = append(allRecords, records...)

		if len(records) == 0 {
			break
		}

		// Extract next cursor
		if s.paginationCursorPath != "" && fullBody != nil {
			if nextCursor, ok := getNestedValue(fullBody, s.paginationCursorPath); ok {
				newCursor := fmt.Sprintf("%v", nextCursor)
				if newCursor == "" || newCursor == "<nil>" {
					break
				}
				cursor = newCursor
			} else {
				break
			}
		} else {
			break
		}
	}

	return allRecords, nil
}

// fetchPaginatedNextURL implements next-URL pagination.
func (s *HTTPSource) fetchPaginatedNextURL(ctx context.Context) ([]map[string]any, error) {
	var allRecords []map[string]any
	currentURL := s.url

	for currentURL != "" {
		records, fullBody, err := s.fetchPageWithBody(ctx, currentURL)
		if err != nil {
			return allRecords, err
		}

		allRecords = append(allRecords, records...)

		// Extract next URL
		if s.paginationNextURLPath != "" && fullBody != nil {
			if nextURL, ok := getNestedValue(fullBody, s.paginationNextURLPath); ok {
				currentURL = fmt.Sprintf("%v", nextURL)
				if currentURL == "<nil>" || currentURL == "" {
					break
				}
			} else {
				break
			}
		} else {
			break
		}
	}

	return allRecords, nil
}

// fetchPaginatedPage implements page-number pagination.
func (s *HTTPSource) fetchPaginatedPage(ctx context.Context) ([]map[string]any, error) {
	var allRecords []map[string]any
	page := 1

	for {
		pageURL := s.buildPageURL(map[string]string{
			s.paginationPageParam:  strconv.Itoa(page),
			s.paginationLimitParam: strconv.Itoa(s.paginationLimit),
		})

		records, fullBody, err := s.fetchPageWithBody(ctx, pageURL)
		if err != nil {
			return allRecords, err
		}

		allRecords = append(allRecords, records...)

		if len(records) == 0 {
			break
		}

		// Check has_more
		if s.paginationHasMorePath != "" && fullBody != nil {
			if hasMore, ok := getNestedValue(fullBody, s.paginationHasMorePath); ok {
				if bv, ok := hasMore.(bool); ok && !bv {
					break
				}
			}
		}

		if len(records) < s.paginationLimit {
			break
		}

		page++
	}

	return allRecords, nil
}

// fetchSinglePage fetches a single URL and parses records.
func (s *HTTPSource) fetchSinglePage(ctx context.Context, fetchURL string) ([]map[string]any, error) {
	records, _, err := s.fetchPageWithBody(ctx, fetchURL)
	return records, err
}

// fetchPageWithBody fetches a URL and returns parsed records plus the raw response body.
func (s *HTTPSource) fetchPageWithBody(ctx context.Context, fetchURL string) ([]map[string]any, map[string]any, error) {
	// Add incremental param if polling
	if s.pollIncrementalParam != "" && s.lastIncrementalValue != "" {
		u, err := url.Parse(fetchURL)
		if err == nil {
			q := u.Query()
			q.Set(s.pollIncrementalParam, s.lastIncrementalValue)
			u.RawQuery = q.Encode()
			fetchURL = u.String()
		}
	}

	body, err := s.doRequestWithRetry(ctx, fetchURL)
	if err != nil {
		return nil, nil, err
	}

	records, fullBody, err := s.ParseResponse(body)
	if err != nil {
		return nil, nil, fmt.Errorf("parse response from %s: %w", fetchURL, err)
	}

	return records, fullBody, nil
}

// doRequestWithRetry performs an HTTP request with rate limiting and retries.
func (s *HTTPSource) doRequestWithRetry(ctx context.Context, fetchURL string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		// Rate limiting
		if err := s.rateLimiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter: %w", err)
		}

		body, statusCode, err := s.doRequest(ctx, fetchURL)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, err
			}
			// Retry on network errors
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}

		// Success
		if statusCode >= 200 && statusCode < 300 {
			return body, nil
		}

		// Don't retry client errors (except 429)
		if statusCode >= 400 && statusCode < 500 && statusCode != 429 {
			return nil, fmt.Errorf("http %d from %s: %s", statusCode, fetchURL, string(body))
		}

		// Retry on 429 and 5xx
		lastErr = fmt.Errorf("http %d from %s", statusCode, fetchURL)

		if statusCode == 429 {
			// Check Retry-After header
			retryAfter := 5 * time.Second
			if attempt < s.maxRetries {
				select {
				case <-time.After(retryAfter):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			continue
		}

		// Exponential backoff for 5xx
		backoff := time.Duration(1<<uint(attempt)) * time.Second
		if attempt < s.maxRetries {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return nil, fmt.Errorf("all retries exhausted: %w", lastErr)
}

// doRequest performs a single HTTP request.
func (s *HTTPSource) doRequest(ctx context.Context, fetchURL string) ([]byte, int, error) {
	var bodyReader io.Reader
	if s.body != "" && s.method == "POST" {
		bodyReader = strings.NewReader(s.body)
	}

	req, err := http.NewRequestWithContext(ctx, s.method, fetchURL, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	// Set headers
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	if s.body != "" && s.method == "POST" {
		req.Header.Set("Content-Type", s.contentType)
	}

	// Apply authentication
	if err := s.applyAuth(ctx, req); err != nil {
		return nil, 0, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read body: %w", err)
	}

	return body, resp.StatusCode, nil
}

// applyAuth adds authentication to the HTTP request based on auth_type.
func (s *HTTPSource) applyAuth(ctx context.Context, req *http.Request) error {
	switch s.authType {
	case "bearer":
		if s.authToken != "" {
			req.Header.Set("Authorization", "Bearer "+s.authToken)
		}
	case "basic":
		if s.authUser != "" {
			encoded := base64.StdEncoding.EncodeToString([]byte(s.authUser + ":" + s.authPassword))
			req.Header.Set("Authorization", "Basic "+encoded)
		}
	case "api_key":
		if s.apiKeyValue != "" {
			req.Header.Set(s.apiKeyHeader, s.apiKeyValue)
		}
	case "oauth2":
		token, err := s.getOAuth2Token(ctx)
		if err != nil {
			return fmt.Errorf("oauth2: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

// getOAuth2Token fetches or returns a cached OAuth2 token.
func (s *HTTPSource) getOAuth2Token(ctx context.Context) (string, error) {
	// Return cached token if still valid
	if s.oauth2Token != "" && time.Now().Before(s.oauth2Expiry) {
		return s.oauth2Token, nil
	}

	// Fetch new token using client credentials grant
	var bodyReader io.Reader
	var contentType string

	if s.oauth2ContentType == "json" {
		payload, err := json.Marshal(map[string]string{
			"grant_type":    "client_credentials",
			"client_id":     s.oauth2ID,
			"client_secret": s.oauth2Secret,
		})
		if err != nil {
			return "", fmt.Errorf("marshal oauth2 json body: %w", err)
		}
		bodyReader = bytes.NewReader(payload)
		contentType = "application/json"
	} else {
		data := url.Values{
			"grant_type":    {"client_credentials"},
			"client_id":     {s.oauth2ID},
			"client_secret": {s.oauth2Secret},
		}
		bodyReader = strings.NewReader(data.Encode())
		contentType = "application/x-www-form-urlencoded"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.oauth2URL, bodyReader)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", err
	}

	s.oauth2Token = tokenResp.AccessToken
	if tokenResp.ExpiresIn > 0 {
		s.oauth2Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	} else {
		s.oauth2Expiry = time.Now().Add(time.Hour)
	}

	return s.oauth2Token, nil
}

// ═══════════════════════════════════════════
// Response Parsing
// ═══════════════════════════════════════════

// ParseResponse extracts records from the HTTP response body.
// Exported for testing.
func (s *HTTPSource) ParseResponse(body []byte) ([]map[string]any, map[string]any, error) {
	switch s.responseType {
	case "jsonl":
		return s.parseJSONL(body)
	case "csv":
		return s.parseCSV(body)
	default:
		return s.parseJSON(body)
	}
}

// parseJSON extracts records from a JSON response.
func (s *HTTPSource) parseJSON(body []byte) ([]map[string]any, map[string]any, error) {
	// First try to parse as an object
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err == nil {
		// If data_path is specified, navigate to the array
		if s.dataPath != "" {
			val, ok := getNestedValue(obj, s.dataPath)
			if !ok {
				return nil, obj, nil
			}
			if arr, ok := val.([]any); ok {
				records := make([]map[string]any, 0, len(arr))
				for _, item := range arr {
					if rec, ok := item.(map[string]any); ok {
						records = append(records, rec)
					}
				}
				return records, obj, nil
			}
			return nil, obj, nil
		}
		// Single object, wrap in array
		return []map[string]any{obj}, obj, nil
	}

	// Try to parse as an array
	var arr []map[string]any
	if err := json.Unmarshal(body, &arr); err != nil {
		return nil, nil, fmt.Errorf("json parse: %w", err)
	}
	return arr, nil, nil
}

// parseJSONL parses newline-delimited JSON.
func (s *HTTPSource) parseJSONL(body []byte) ([]map[string]any, map[string]any, error) {
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		records = append(records, rec)
	}
	return records, nil, scanner.Err()
}

// parseCSV parses CSV response with header row.
func (s *HTTPSource) parseCSV(body []byte) ([]map[string]any, map[string]any, error) {
	reader := csv.NewReader(bytes.NewReader(body))
	reader.LazyQuotes = true

	allRecords, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("csv parse: %w", err)
	}

	if len(allRecords) < 2 {
		return nil, nil, nil
	}

	headers := allRecords[0]
	records := make([]map[string]any, 0, len(allRecords)-1)

	for _, row := range allRecords[1:] {
		rec := make(map[string]any, len(headers))
		for i, header := range headers {
			if i < len(row) {
				rec[header] = row[i]
			}
		}
		records = append(records, rec)
	}

	return records, nil, nil
}

// ═══════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════

// buildPageURL adds pagination query parameters to the base URL.
func (s *HTTPSource) buildPageURL(params map[string]string) string {
	u, err := url.Parse(s.url)
	if err != nil {
		return s.url
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// getNestedValue navigates a dot-separated path in a map.
// e.g., "data.results" on {"data":{"results":[...]}} returns the array.
func getNestedValue(obj map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = obj

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			val, ok := v[part]
			if !ok {
				return nil, false
			}
			current = val
		default:
			return nil, false
		}
	}

	return current, true
}

// toInt converts an interface value to int.
func toInt(v any) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case string:
		n, _ := strconv.Atoi(val)
		return n
	}
	return 0
}

// Close stops the HTTP source.
func (s *HTTPSource) Close() error {
	s.wg.Wait()
	return nil
}

// Lag returns the event channel backlog.
func (s *HTTPSource) Lag() int64 {
	return s.lag.Load()
}

// GetNestedValue is exported for testing.
func GetNestedValue(obj map[string]any, path string) (any, bool) {
	return getNestedValue(obj, path)
}

// Ensure HTTPSource implements pipeline.Source at compile time.
var _ pipeline.Source = (*HTTPSource)(nil)
