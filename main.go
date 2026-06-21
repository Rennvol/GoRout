package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// truncStr truncates a string to max runes (not bytes), appending "..." if cut.
func truncStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

// ==================== API Keys ====================

type APIKey struct {
	Key       string    `json:"key"`
	Label     string    `json:"label"`
	CreatedAt time.Time `json:"created_at"`
	LastUsed  time.Time `json:"last_used"`
}

// ==================== Provider ====================

type Provider struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	BaseURL     string    `json:"base_url"`
	APIKeys     []string  `json:"api_keys"`     // multiple tokens for rotation
	Enabled     bool      `json:"enabled"`
	Prefix      string    `json:"prefix"`        // USER-DEFINED, e.g. "or", "openai", "ant"
	Models      []string  `json:"models"`        // cached from fetch
	ModelsFetch time.Time `json:"models_fetch"`  // last fetch time
	rrCounter   uint64    `json:"-"`             // round-robin counter (in-memory only)
}

// RotateKey returns the next API key for this provider using round-robin.
// First call picks key 0, next picks key 1, ... wraps around. Thread-safe.
func (p *Provider) RotateKey() string {
	if len(p.APIKeys) == 0 {
		return ""
	}
	if len(p.APIKeys) == 1 {
		return p.APIKeys[0]
	}
	idx := atomic.AddUint64(&p.rrCounter, 1) - 1
	return p.APIKeys[int(idx%uint64(len(p.APIKeys)))]
}

// MaskKeys returns masked preview of all keys (for display).
func (p *Provider) MaskKeys() []string {
	out := make([]string, len(p.APIKeys))
	for i, k := range p.APIKeys {
		out[i] = maskKey(k)
	}
	return out
}

type ModelEntry struct {
	ID       string `json:"id"`       // prefix/original-model-name
	Provider string `json:"provider"` // provider ID
	Prefix   string `json:"prefix"`   // user-defined prefix
}

// ==================== Config ====================

type Settings struct {
	Port     int    `json:"port"`
	LogLevel string `json:"log_level"`
}

// ==================== Usage Tracking ====================

// ProviderUsage tracks one provider's lifetime stats.
type ProviderUsage struct {
	Requests      uint64 `json:"requests"`
	InputTokens   uint64 `json:"input_tokens"`
	OutputTokens  uint64 `json:"output_tokens"`
	TotalTokens   uint64 `json:"total_tokens"`
	Errors        uint64 `json:"errors"`
	LastRequestAt string `json:"last_request_at,omitempty"`
}

// ModelUsage tracks one model's stats within a provider.
type ModelUsage struct {
	Provider     string `json:"provider"`
	Model        string `json:"model"`
	Requests     uint64 `json:"requests"`
	InputTokens  uint64 `json:"input_tokens"`
	OutputTokens uint64 `json:"output_tokens"`
	TotalTokens  uint64 `json:"total_tokens"`
}

// Usage holds aggregate stats. ByProvider keyed by id; ByModel keyed by "provider|model".
type Usage struct {
	StartedAt   string                    `json:"started_at"`
	LastFlushAt string                    `json:"last_flush_at,omitempty"`
	Total       ProviderUsage             `json:"total"`
	ByProvider  map[string]*ProviderUsage `json:"by_provider"`
	ByModel     map[string]*ModelUsage    `json:"by_model"`
}

func newUsage() Usage {
	return Usage{
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
		ByProvider: make(map[string]*ProviderUsage),
		ByModel:    make(map[string]*ModelUsage),
	}
}

// Record adds one request's stats. Thread-safe.
func (u *Usage) Record(provider, model string, inTok, outTok uint64, isErr bool) {
	now := time.Now().UTC().Format(time.RFC3339)
	atomic.AddUint64(&u.Total.Requests, 1)
	atomic.AddUint64(&u.Total.InputTokens, inTok)
	atomic.AddUint64(&u.Total.OutputTokens, outTok)
	atomic.AddUint64(&u.Total.TotalTokens, inTok+outTok)
	if isErr {
		atomic.AddUint64(&u.Total.Errors, 1)
	}

	p, ok := u.ByProvider[provider]
	if !ok {
		p = &ProviderUsage{}
		u.ByProvider[provider] = p
	}
	atomic.AddUint64(&p.Requests, 1)
	atomic.AddUint64(&p.InputTokens, inTok)
	atomic.AddUint64(&p.OutputTokens, outTok)
	atomic.AddUint64(&p.TotalTokens, inTok+outTok)
	if isErr {
		atomic.AddUint64(&p.Errors, 1)
	}
	p.LastRequestAt = now

	if model != "" {
		key := provider + "|" + model
		m, ok := u.ByModel[key]
		if !ok {
			m = &ModelUsage{Provider: provider, Model: model}
			u.ByModel[key] = m
		}
		atomic.AddUint64(&m.Requests, 1)
		atomic.AddUint64(&m.InputTokens, inTok)
		atomic.AddUint64(&m.OutputTokens, outTok)
		atomic.AddUint64(&m.TotalTokens, inTok+outTok)
	}
}

type Config struct {
	mu         sync.RWMutex
	Providers  []Provider `json:"providers"`
	APIKeys    []APIKey   `json:"api_keys"`
	Settings   Settings   `json:"settings"`
	Usage      Usage      `json:"usage"`
	configPath string
	usagePath  string
	httpClient *http.Client
}

func NewConfig(path string) *Config {
	usagePath := filepath.Join(filepath.Dir(path), "usage.json")
	c := &Config{
		configPath: path,
		usagePath:  usagePath,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		Settings:   Settings{Port: 9988, LogLevel: "info"},
	}
	c.load()
	c.loadUsage()
	go c.flushLoop()
	return c
}

// loadUsage reads usage.json if present. Best-effort: missing file = empty stats.
func (c *Config) loadUsage() {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := os.ReadFile(c.usagePath)
	if err != nil {
		c.Usage = newUsage()
		return
	}
	if err := json.Unmarshal(data, &c.Usage); err != nil {
		c.Usage = newUsage()
		return
	}
	// Ensure maps aren't nil
	if c.Usage.ByProvider == nil {
		c.Usage.ByProvider = make(map[string]*ProviderUsage)
	}
	if c.Usage.ByModel == nil {
		c.Usage.ByModel = make(map[string]*ModelUsage)
	}
}

// flushLoop periodically writes usage.json. 10s interval keeps disk IO low
// while still surviving crashes with at most 10s of loss.
func (c *Config) flushLoop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		c.saveUsage()
	}
}

func (c *Config) saveUsage() {
	c.mu.RLock()
	usage := c.Usage
	usage.LastFlushAt = time.Now().UTC().Format(time.RFC3339)
	c.mu.RUnlock()

	data, err := json.MarshalIndent(usage, "", "  ")
	if err != nil {
		return
	}
	tmp := c.usagePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return
	}
	os.Rename(tmp, c.usagePath)
}

func (c *Config) load() {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := os.ReadFile(c.configPath)
	if err != nil {
		c.saveLocked()
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err == nil {
		json.Unmarshal(data, c)

		// Migration: old schema had "api_key" (single string) per provider.
		// New schema has "api_keys" ([]string). Move old value if present.
		if provsRaw, ok := raw["providers"]; ok {
			var provs []json.RawMessage
			if err := json.Unmarshal(provsRaw, &provs); err == nil {
				for i := range c.Providers {
					if i >= len(provs) {
						break
					}
					var pmap map[string]any
					if err := json.Unmarshal(provs[i], &pmap); err != nil {
						continue
					}
					if c.Providers[i].APIKeys == nil {
						if v, ok := pmap["api_key"].(string); ok && v != "" {
							c.Providers[i].APIKeys = []string{v}
						} else if arr, ok := pmap["api_keys"].([]any); ok {
							for _, k := range arr {
								if s, ok := k.(string); ok && s != "" {
									c.Providers[i].APIKeys = append(c.Providers[i].APIKeys, s)
								}
							}
						}
					}
				}
			}
		}
	} else {
		json.Unmarshal(data, c)
	}

	if c.Settings.Port == 0 {
		c.Settings.Port = 9988
	}
}

func (c *Config) save() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saveLocked()
}

func (c *Config) saveLocked() {
	data, _ := json.MarshalIndent(c, "", "  ")
	os.WriteFile(c.configPath, data, 0600)
}

func (c *Config) EnabledProviders() []Provider {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []Provider
	for _, p := range c.Providers {
		if p.Enabled {
			out = append(out, p)
		}
	}
	return out
}

// ==================== Model Fetching ====================

// FetchModels fetches /models from a provider and caches them
func (c *Config) FetchModels(prov *Provider) error {
	modelURL := strings.TrimRight(prov.BaseURL, "/") + "/models"

	req, err := http.NewRequest("GET", modelURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	firstKey := ""
	if len(prov.APIKeys) > 0 {
		firstKey = prov.APIKeys[0]
	}
	req.Header.Set("Authorization", "Bearer "+firstKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", modelURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		// Ollama format
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	var modelIDs []string
	for _, m := range result.Data {
		modelIDs = append(modelIDs, m.ID)
	}
	for _, m := range result.Models {
		modelIDs = append(modelIDs, m.Name)
	}

	if len(modelIDs) == 0 {
		return fmt.Errorf("no models found")
	}

	c.mu.Lock()
	for i := range c.Providers {
		if c.Providers[i].ID == prov.ID {
			c.Providers[i].Models = modelIDs
			c.Providers[i].ModelsFetch = time.Now()
			break
		}
	}
	c.saveLocked()
	c.mu.Unlock()

	return nil
}

func (c *Config) FetchAllModels() map[string]string {
	results := make(map[string]string)
	providers := c.EnabledProviders()
	for i := range providers {
		prov := &providers[i]
		err := c.FetchModels(prov)
		if err != nil {
			results[prov.ID] = "ERROR: " + err.Error()
		} else {
			c.mu.RLock()
			count := 0
			for _, p := range c.Providers {
				if p.ID == prov.ID {
					count = len(p.Models)
					break
				}
			}
			c.mu.RUnlock()
			results[prov.ID] = fmt.Sprintf("OK (%d models)", count)
		}
	}
	return results
}

// GetAllModels returns all models with prefix applied
func (c *Config) GetAllModels() []ModelEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var entries []ModelEntry
	for _, p := range c.Providers {
		if !p.Enabled {
			continue
		}
		for _, m := range p.Models {
			entries = append(entries, ModelEntry{
				ID:       p.Prefix + "/" + m,
				Provider: p.ID,
				Prefix:   p.Prefix,
			})
		}
	}
	return entries
}

// ResolveModel parses "prefix/model" → returns provider + original model name
func (c *Config) ResolveModel(modelName string) (*Provider, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// First pass: longest-prefix match wins (avoids "or" eating "openrouter/...").
	var matches []Provider
	for _, p := range c.Providers {
		if !p.Enabled {
			continue
		}
		prefixSlash := p.Prefix + "/"
		if strings.HasPrefix(modelName, prefixSlash) {
			matches = append(matches, p)
		}
	}
	if len(matches) > 0 {
		best := matches[0]
		for _, m := range matches[1:] {
			if len(m.Prefix) > len(best.Prefix) {
				best = m
			}
		}
		originalModel := strings.TrimPrefix(modelName, best.Prefix+"/")
		return &best, originalModel
	}
	return nil, modelName
}

// ==================== API Key Methods ====================

func (c *Config) GenerateKey(label string) (*APIKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, k := range c.APIKeys {
		if strings.EqualFold(k.Label, label) {
			return nil, fmt.Errorf("label '%s' already exists", label)
		}
	}

	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	key := "gr_" + hex.EncodeToString(keyBytes)

	apiKey := APIKey{Key: key, Label: label, CreatedAt: time.Now()}
	c.APIKeys = append(c.APIKeys, apiKey)
	c.saveLocked()
	return &apiKey, nil
}

func (c *Config) ListKeys() []APIKey {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]APIKey, len(c.APIKeys))
	copy(out, c.APIKeys)
	return out
}

func (c *Config) ViewKey(label string) (*APIKey, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for i := range c.APIKeys {
		if strings.EqualFold(c.APIKeys[i].Label, label) {
			return &c.APIKeys[i], nil
		}
	}
	return nil, fmt.Errorf("label '%s' not found", label)
}

func (c *Config) DeleteKey(label string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := -1
	for i, k := range c.APIKeys {
		if strings.EqualFold(k.Label, label) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("label '%s' not found", label)
	}
	c.APIKeys = append(c.APIKeys[:idx], c.APIKeys[idx+1:]...)
	c.saveLocked()
	return nil
}

func (c *Config) ValidateKey(key string) bool {
	c.mu.RLock()
	found := false
	for i := range c.APIKeys {
		if c.APIKeys[i].Key == key {
			found = true
			break
		}
	}
	c.mu.RUnlock()

	if found {
		go func() {
			c.mu.Lock()
			for i := range c.APIKeys {
				if c.APIKeys[i].Key == key {
					c.APIKeys[i].LastUsed = time.Now()
					break
				}
			}
			c.saveLocked()
			c.mu.Unlock()
		}()
	}
	return found
}

func maskKey(key string) string {
	if len(key) <= 10 {
		return key[:3] + "***"
	}
	return key[:4] + "..." + key[len(key)-4:]
}

// ==================== HTTP Proxy ====================

type Proxy struct {
	config *Config
	client *http.Client
	logger *log.Logger
}

func NewProxy(cfg *Config) *Proxy {
	return &Proxy{
		config: cfg,
		client: &http.Client{Timeout: 0},
		logger: log.New(os.Stdout, "[gorout] ", log.LstdFlags),
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}

	// Internal API
	if strings.HasPrefix(r.URL.Path, "/api/") {
		p.handleInternalAPI(w, r)
		return
	}

	// Health
	if r.URL.Path == "/" && r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"version":   "1.0.0",
			"name":      "GoRout",
			"port":      p.config.Settings.Port,
			"providers": len(p.config.EnabledProviders()),
		})
		return
	}

	// AI Proxy
	if !strings.HasPrefix(r.URL.Path, "/v1/") && !strings.HasPrefix(r.URL.Path, "/openai/") {
		http.NotFound(w, r)
		return
	}

	// Auth
	auth := r.Header.Get("Authorization")
	bearerToken := strings.TrimPrefix(auth, "Bearer ")
	if !p.config.ValidateKey(bearerToken) {
		p.jsonError(w, "Unauthorized: valid API key required", 401)
		return
	}

	// Read body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		p.jsonError(w, "read body: "+err.Error(), 500)
		return
	}

	var bodyMap map[string]any
	json.Unmarshal(bodyBytes, &bodyMap)
	modelName, _ := bodyMap["model"].(string)

	// Resolve provider from model prefix
	var target *Provider
	actualModel := modelName

	if modelName != "" {
		prov, resolvedModel := p.config.ResolveModel(modelName)
		if prov != nil {
			target = prov
			actualModel = resolvedModel
			bodyMap["model"] = actualModel
		}
	}

	if target == nil {
		providers := p.config.EnabledProviders()
		if len(providers) == 0 {
			p.jsonError(w, "No enabled providers", 503)
			return
		}
		target = &providers[0]
	}

	// Re-encode body
	newBody, _ := json.Marshal(bodyMap)

	// Resolve streaming intent (OpenAI "stream": true, or "stream_options.include_usage").
	isStreaming := false
	if s, ok := bodyMap["stream"].(bool); ok && s {
		isStreaming = true
	} else if so, ok := bodyMap["stream_options"].(map[string]any); ok {
		if v, ok := so["include_usage"].(bool); ok && v {
			isStreaming = true
		}
	}

	// Rewrite path
	path := r.URL.Path
	path = strings.TrimPrefix(path, "/v1")
	path = strings.TrimPrefix(path, "/openai")
	if path == "" {
		path = "/"
	}

	targetURL := target.BaseURL + path
	p.logger.Printf("PROXY %s %s -> %s (model: %s → %s)", r.Method, r.URL.Path, targetURL, modelName, actualModel)

	req, err := http.NewRequest(r.Method, targetURL, bytes.NewReader(newBody))
	if err != nil {
		p.jsonError(w, err.Error(), 500)
		return
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Authorization", "Bearer "+target.RotateKey())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Del("Host")
	req.ContentLength = int64(len(newBody))

	// Failover: try current key, then rotate to next on retryable errors.
	// Retryable = 401 (bad key), 429 (rate-limited), 5xx (server error).
	// Network errors count as retryable too.
	resp, err := p.doWithFailover(req, target)
	if err != nil {
		p.recordUsage(target.ID, actualModel, bodyMap, 0, 0, true)
		p.jsonError(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)

	// Buffer the upstream body so we can parse usage after streaming it to the client.
	// OpenAI sends a `usage` chunk in the final SSE event for stream=true.
	isErr := resp.StatusCode >= 400
	if isStreaming {
		// Tee: copy to client + collect into a buffer for usage parsing.
		var buf bytes.Buffer
		tee := io.TeeReader(resp.Body, &buf)
		io.Copy(w, tee)
		inTok, outTok, modelSeen := parseSSEUsage(&buf, actualModel)
		if modelSeen == "" {
			modelSeen = actualModel
		}
		p.recordUsage(target.ID, modelSeen, bodyMap, inTok, outTok, isErr)
	} else {
		body, _ := io.ReadAll(resp.Body)
		w.Write(body)
		inTok, outTok, modelSeen := parseJSONUsage(body)
		if modelSeen == "" {
			modelSeen = actualModel
		}
		p.recordUsage(target.ID, modelSeen, bodyMap, inTok, outTok, isErr)
	}
}

// recordUsage logs one request's usage. Tokens may be 0 (e.g. on error or
// non-OpenAI providers that omit the usage field).
func (p *Proxy) recordUsage(provider, model string, bodyMap map[string]any, inTok, outTok uint64, isErr bool) {
	p.config.Usage.Record(provider, model, inTok, outTok, isErr)
}

// doWithFailover sends the request, rotating keys on retryable errors.
// Strategy:
//   - Make request with current key.
//   - If response is 401/429/5xx, mark it as retried and try the next key.
//   - If body is network error, retry once with the next key.
//   - Give up after trying every key (loop limited to N keys).
//   - On final failure, return the LAST response / error so caller can stream it.
//
// Returns (response, error). Caller MUST close resp.Body when error==nil.
func (p *Proxy) doWithFailover(req *http.Request, target *Provider) (*http.Response, error) {
	// The body has been marshalled already; we need to re-create the reader for each attempt
	// because net/http drains the body after a send.
	bodyBytes := []byte(nil)
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
		req.Body.Close()
	}

	nKeys := len(target.APIKeys)
	if nKeys == 0 {
		return nil, fmt.Errorf("provider %s has no API keys", target.ID)
	}

	startIdx := int(atomic.AddUint64(&target.rrCounter, 1) % uint64(nKeys))

	var lastErr error
	var lastResp *http.Response
	for attempt := 0; attempt < nKeys; attempt++ {
		idx := (startIdx + attempt) % nKeys
		key := target.APIKeys[idx]

		// Rebuild request with fresh body reader
		req2, err := http.NewRequest(req.Method, req.URL.String(), bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		req2.Header = req.Header.Clone()
		req2.Header.Set("Authorization", "Bearer "+key)
		req2.ContentLength = int64(len(bodyBytes))

		resp, err := p.client.Do(req2)
		if err != nil {
			lastErr = err
			p.logger.Printf("FAILOVER: provider=%s key=%d network err=%v (trying next)", target.ID, idx+1, err)
			continue
		}

		// 2xx = success, return immediately
		if resp.StatusCode < 400 {
			if attempt > 0 {
				p.logger.Printf("FAILOVER: provider=%s recovered on key=%d (status=%d)", target.ID, idx+1, resp.StatusCode)
			}
			return resp, nil
		}

		// Non-2xx: hold onto the latest response. Decide if we should retry.
		retryable := resp.StatusCode == 401 || resp.StatusCode == 429 || resp.StatusCode >= 500
		if !retryable {
			// 4xx other than 401/429 — client error, not a key problem. Return immediately.
			return resp, nil
		}

		// Drain + close this attempt's body so the connection can be reused.
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// If this was the last key we can try, give up and return a synthesized error.
		if attempt == nKeys-1 {
			return nil, fmt.Errorf("all %d key(s) exhausted for provider %s (last status: %d)", nKeys, target.ID, resp.StatusCode)
		}
		lastResp = resp
		p.logger.Printf("FAILOVER: provider=%s key=%d status=%d retrying", target.ID, idx+1, resp.StatusCode)
	}

	// All attempts failed at the network layer
	if lastResp != nil {
		return nil, fmt.Errorf("upstream error after %d attempts: %d", nKeys, lastResp.StatusCode)
	}
	return nil, fmt.Errorf("upstream unreachable: %v", lastErr)
}

// parseJSONUsage extracts usage from a non-streaming OpenAI response.
// Returns (input, output, model). Missing usage yields (0, 0, "").
func parseJSONUsage(body []byte) (uint64, uint64, string) {
	var resp struct {
		Model string `json:"model"`
		Usage struct {
			PromptTokens     uint64 `json:"prompt_tokens"`
			CompletionTokens uint64 `json:"completion_tokens"`
			TotalTokens      uint64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, 0, ""
	}
	return resp.Usage.PromptTokens, resp.Usage.CompletionTokens, resp.Model
}

// parseSSEUsage walks an SSE buffer and pulls the final usage chunk (if any).
// Streamed responses emit one "data: {...}" per line; usage appears in the
// chunk whose JSON has a non-null `usage` field.
func parseSSEUsage(buf *bytes.Buffer, fallbackModel string) (uint64, uint64, string) {
	scanner := bufio.NewScanner(buf)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var (
		lastIn, lastOut uint64
		modelSeen       string
	)
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := bytes.TrimPrefix(line, []byte("data: "))
		if bytes.Equal(payload, []byte("[DONE]")) {
			break
		}
		var chunk struct {
			Model  string `json:"model"`
			Usage  *struct {
				PromptTokens     uint64 `json:"prompt_tokens"`
				CompletionTokens uint64 `json:"completion_tokens"`
				TotalTokens      uint64 `json:"total_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(payload, &chunk); err != nil {
			continue
		}
		if chunk.Model != "" {
			modelSeen = chunk.Model
		}
		if chunk.Usage != nil {
			lastIn = chunk.Usage.PromptTokens
			lastOut = chunk.Usage.CompletionTokens
		}
	}
	return lastIn, lastOut, modelSeen
}

func (p *Proxy) handleInternalAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api")

	switch {
	case path == "/providers" && r.Method == "GET":
		// Sanitize: mask keys in API response so we don't leak secrets over HTTP
		out := make([]map[string]any, 0, len(p.config.Providers))
		for _, pr := range p.config.Providers {
			out = append(out, map[string]any{
				"id":          pr.ID,
				"name":        pr.Name,
				"base_url":    pr.BaseURL,
				"enabled":     pr.Enabled,
				"prefix":      pr.Prefix,
				"models":      pr.Models,
				"models_fetch": pr.ModelsFetch,
				"key_count":   len(pr.APIKeys),
				"api_keys":    pr.MaskKeys(),
			})
		}
		json.NewEncoder(w).Encode(out)

	case path == "/providers" && r.Method == "POST":
		var prov Provider
		json.NewDecoder(r.Body).Decode(&prov)
		if prov.ID == "" {
			prov.ID = prov.Name
		}
		p.config.Providers = append(p.config.Providers, prov)
		p.config.save()
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": prov.ID})

	case path == "/providers/keys" && r.Method == "POST":
		// Add an API key to an existing provider: {"id": "openrouter", "api_key": "sk-or-..."}
		var req struct {
			ID     string `json:"id"`
			APIKey string `json:"api_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			p.jsonError(w, "bad json: "+err.Error(), 400)
			return
		}
		p.config.mu.Lock()
		defer p.config.mu.Unlock()
		for i := range p.config.Providers {
			if p.config.Providers[i].ID == req.ID {
				p.config.Providers[i].APIKeys = append(p.config.Providers[i].APIKeys, req.APIKey)
				count := len(p.config.Providers[i].APIKeys)
				p.config.saveLocked()
				json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": req.ID, "key_count": count})
				return
			}
		}
		p.jsonError(w, "provider not found: "+req.ID, 404)

	case path == "/usage" && r.Method == "GET":
		json.NewEncoder(w).Encode(p.config.Usage)

	case path == "/usage/reset" && r.Method == "POST":
		p.config.Usage = newUsage()
		p.config.saveUsage()
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "reset_at": time.Now().UTC()})

	case path == "/models" && r.Method == "GET":
		json.NewEncoder(w).Encode(p.config.GetAllModels())

	case path == "/models/refresh" && r.Method == "POST":
		results := p.config.FetchAllModels()
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "results": results})

	case path == "/settings" && r.Method == "GET":
		json.NewEncoder(w).Encode(p.config.Settings)

	case path == "/settings" && r.Method == "PUT":
		var s Settings
		json.NewDecoder(r.Body).Decode(&s)
		if s.Port > 0 {
			p.config.Settings.Port = s.Port
		}
		p.config.save()
		json.NewEncoder(w).Encode(p.config.Settings)

	case strings.HasPrefix(path, "/providers/") && r.Method == "DELETE":
		id := strings.TrimPrefix(path, "/providers/")
		var filtered []Provider
		for _, pr := range p.config.Providers {
			if pr.ID != id {
				filtered = append(filtered, pr)
			}
		}
		p.config.Providers = filtered
		p.config.save()
		json.NewEncoder(w).Encode(map[string]any{"ok": true})

	default:
		json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
	}
}

func (p *Proxy) jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

// ==================== CLI ====================

func usage() {
	fmt.Println(`GoRout — lightweight AI API proxy with model routing

Usage:
  gorout start                    Start proxy server
  gorout stop                     Stop running server
  gorout status                   Show server status
  gorout config                   Show config
  gorout list-providers           List all providers (with token count)
  gorout add-provider             Add AI provider (interactive, supports multi-token rotation)
  gorout add-key <provider>       Add another API key to existing provider
  gorout remove-key <provider>    Remove an API key from provider (by index)
  gorout usage                    Show usage stats (total + per-provider + per-model)
  gorout usage-reset              Reset all usage counters
  gorout fetch-models             Fetch models from all providers
  gorout fetch-models-one <id>    Fetch models from one provider
  gorout list-models              List all models with prefixes
  gorout list-models <provider>   List models for one provider
  gorout test-model <provider>/<model>  Test one model (1 request, prints tokens)
  gorout generate-key --label X   Generate API key
  gorout list-keys                List all API keys (masked)
  gorout view <label>             Show full API key
  gorout delete-key <label>       Delete API key
  gorout version                  Show version

Env:
  GOROUT_PORT     Override port (default: 9988)
  GOROUT_HOME     Config directory (default: ~/.gorout)`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(0)
	}

	home := os.Getenv("GOROUT_HOME")
	if home == "" {
		home = filepath.Join(os.Getenv("HOME"), ".gorout")
	}
	os.MkdirAll(home, 0700)

	configPath := filepath.Join(home, "config.json")
	cfg := NewConfig(configPath)

	if envPort := os.Getenv("GOROUT_PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil && p > 0 {
			cfg.Settings.Port = p
		}
	}

	cmd := os.Args[1]

	switch cmd {
	case "start":
		port := cfg.Settings.Port
		addr := fmt.Sprintf(":%d", port)
		proxy := NewProxy(cfg)

		pidPath := filepath.Join(home, "server.pid")
		os.WriteFile(pidPath, []byte(fmt.Sprintf("%d", os.Getpid())), 0600)

		fmt.Printf("🚀 GoRout listening on http://localhost:%d\n", port)
		fmt.Printf("    Config: %s\n", configPath)
		fmt.Printf("    Providers: %d\n", len(cfg.EnabledProviders()))
		fmt.Printf("    API Keys:  %d\n", len(cfg.APIKeys))
		fmt.Printf("    Models:    %d\n", len(cfg.GetAllModels()))
		fmt.Printf("    PID: %d\n\n", os.Getpid())

		// Open log file for failover / proxy diagnostics
		logPath := filepath.Join(home, "server.log")
		logFile, logErr := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if logErr != nil {
			log.Printf("⚠️  Cannot open %s: %v (using stdout)", logPath, logErr)
		} else {
			mw := io.MultiWriter(os.Stdout, logFile)
			log.SetOutput(mw)
			// Also redirect the proxy's logger (used in ServeHTTP / doWithFailover)
			proxy.logger = log.New(mw, "[gorout] ", log.LstdFlags)
		}

		// Auto-fetch models on startup
		go func() {
			time.Sleep(2 * time.Second)
			results := cfg.FetchAllModels()
			for id, result := range results {
				log.Printf("[models] %s: %s", id, result)
			}
		}()

		log.Fatal(http.ListenAndServe(addr, proxy))

	case "stop":
		pidPath := filepath.Join(home, "server.pid")
		data, err := os.ReadFile(pidPath)
		if err != nil {
			fmt.Println("❌ Server not running (no PID file)")
			os.Exit(1)
		}
		pid, _ := strconv.Atoi(string(data))
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err != nil {
			fmt.Printf("❌ Server (PID %d) not running — stale PID\n", pid)
			os.Remove(pidPath)
			os.Exit(1)
		}
		proc, _ := os.FindProcess(pid)
		proc.Kill()
		os.Remove(pidPath)
		fmt.Printf("✅ Server (PID %d) stopped\n", pid)

	case "status":
		pidPath := filepath.Join(home, "server.pid")
		data, err := os.ReadFile(pidPath)
		if err != nil {
			fmt.Println("❌ Server not running")
			os.Exit(1)
		}
		pid, _ := strconv.Atoi(string(data))
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err != nil {
			fmt.Printf("❌ Server (PID %d) not running — stale PID\n", pid)
			os.Remove(pidPath)
			os.Exit(1)
		}
		fmt.Printf("✅ Server running (PID %d)\n", pid)
		fmt.Printf("    Providers: %d enabled / %d total\n", len(cfg.EnabledProviders()), len(cfg.Providers))
		fmt.Printf("    API Keys:  %d\n", len(cfg.APIKeys))
		fmt.Printf("    Models:    %d\n", len(cfg.GetAllModels()))
		fmt.Printf("    Port: %d\n", cfg.Settings.Port)

	case "config":
		fmt.Printf("Config: %s\n\n", configPath)
		data, _ := os.ReadFile(configPath)
		fmt.Printf("%s\n", data)

	case "add-provider":
		fmt.Print("Provider name: ")
		var name string
		fmt.Scanln(&name)
		fmt.Print("Base URL (e.g. https://api.openai.com/v1): ")
		var baseURL string
		fmt.Scanln(&baseURL)
		fmt.Print("API Key 1: ")
		var apiKey1 string
		fmt.Scanln(&apiKey1)
		fmt.Print("Prefix (custom, e.g. or, openai, ant): ")
		var prefix string
		fmt.Scanln(&prefix)
		if prefix == "" {
			prefix = name
		}

		// Optional: ask for extra tokens for rotation
		var apiKeys []string
		if apiKey1 != "" {
			apiKeys = append(apiKeys, apiKey1)
		}
		fmt.Print("Add another API key for rotation? (y/N): ")
		var ans string
		fmt.Scanln(&ans)
		for strings.EqualFold(ans, "y") || strings.EqualFold(ans, "yes") {
			fmt.Print("API Key: ")
			var k string
			fmt.Scanln(&k)
			if k != "" {
				apiKeys = append(apiKeys, k)
			}
			fmt.Print("Add another? (y/N): ")
			fmt.Scanln(&ans)
		}

		prov := Provider{
			ID:      name,
			Name:    name,
			BaseURL: baseURL,
			APIKeys: apiKeys,
			Enabled: true,
			Prefix:  prefix,
		}
		cfg.Providers = append(cfg.Providers, prov)
		cfg.save()
		fmt.Printf("✅ Provider '%s' added (prefix: %s, %d API key(s) for rotation)\n", name, prefix, len(apiKeys))
		fmt.Printf("   Models will be: %s/<model-name>\n", prefix)
		fmt.Printf("   Run 'gorout fetch-models' to get available models\n")
		if len(apiKeys) > 1 {
			fmt.Printf("   Tokens will rotate round-robin across %d keys\n", len(apiKeys))
		}

	case "list-providers":
		if len(cfg.Providers) == 0 {
			fmt.Println("No providers configured.")
			fmt.Println("Add one with: gorout add-provider")
			os.Exit(0)
		}
		fmt.Printf("Providers (%d):\n\n", len(cfg.Providers))
		fmt.Printf("  %-5s %-20s %-40s %-12s %s\n", "ON", "ID", "BASE URL", "PREFIX", "KEYS")
		fmt.Println("  " + strings.Repeat("-", 95))
		for _, p := range cfg.Providers {
			onOff := "✗"
			if p.Enabled {
				onOff = "✓"
			}
			base := p.BaseURL
			if len(base) > 38 {
				base = base[:35] + "..."
			}
			masked := p.MaskKeys()
			fmt.Printf("  %-5s %-20s %-40s %-12s %d token(s): %s\n",
				onOff, p.ID, base, p.Prefix, len(p.APIKeys), strings.Join(masked, ", "))
		}
		fmt.Printf("\nTotal models cached: %d\n", len(cfg.GetAllModels()))

	case "fetch-models":
		fmt.Println("🔄 Fetching models from all providers...")
		results := cfg.FetchAllModels()
		for id, result := range results {
			fmt.Printf("  %s: %s\n", id, result)
		}

	case "fetch-models-one":
		if len(os.Args) < 3 {
			fmt.Println("❌ Usage: gorout fetch-models-one <provider-id>")
			os.Exit(1)
		}
		provID := os.Args[2]
		var prov *Provider
		for i := range cfg.Providers {
			if cfg.Providers[i].ID == provID {
				prov = &cfg.Providers[i]
				break
			}
		}
		if prov == nil {
			fmt.Printf("❌ Provider '%s' not found\n", provID)
			os.Exit(1)
		}
		fmt.Printf("🔄 Fetching models from provider '%s' (%s)...\n", prov.ID, prov.BaseURL)
		if err := cfg.FetchModels(prov); err != nil {
			fmt.Printf("❌ %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ Cached %d models for '%s'\n", len(prov.Models), prov.ID)

	case "list-models":
		// Optional filter: list-models <provider-id>
		filter := ""
		if len(os.Args) >= 3 {
			filter = os.Args[2]
		}
		models := cfg.GetAllModels()
		if filter != "" {
			filtered := models[:0]
			for _, m := range models {
				if m.Provider == filter {
					filtered = append(filtered, m)
				}
			}
			models = filtered
		}
		if len(models) == 0 {
			if filter != "" {
				fmt.Printf("No models cached for provider '%s'. Run: gorout fetch-models-one %s\n", filter, filter)
			} else {
				fmt.Println("No models cached. Run: gorout fetch-models")
			}
			os.Exit(0)
		}
		fmt.Printf("Models (%d", len(models))
		if filter != "" {
			fmt.Printf(", provider=%s", filter)
		}
		fmt.Println("):\n")
		fmt.Printf("  %-55s %-15s %s\n", "MODEL ID", "PROVIDER", "PREFIX")
		fmt.Println("  " + strings.Repeat("-", 85))
		for _, m := range models {
			fmt.Printf("  %-55s %-15s %s\n", m.ID, m.Provider, m.Prefix)
		}

	case "test-model":
		if len(os.Args) < 3 {
			fmt.Println("❌ Usage: gorout test-model <provider>/<model> [--prompt \"text\"] [--max-tokens N]")
			fmt.Println("   Examples:")
			fmt.Println("     gorout test-model openai/gpt-4o-mini")
			fmt.Println("     gorout test-model nine/duadua --prompt \"hello\" --max-tokens 50")
			os.Exit(1)
		}
		modelArg := os.Args[2]
		prompt := "Say 'ok' in exactly 2 words."
		maxTokens := 20
		stream := false
		for i := 3; i < len(os.Args); i++ {
			arg := os.Args[i]
			switch {
			case arg == "--prompt" && i+1 < len(os.Args):
				prompt = os.Args[i+1]
				i++
			case arg == "--max-tokens" && i+1 < len(os.Args):
				if n, err := strconv.Atoi(os.Args[i+1]); err == nil {
					maxTokens = n
				}
				i++
			case arg == "--stream":
				stream = true
			}
		}

		prov, resolvedModel := cfg.ResolveModel(modelArg)
		if prov == nil {
			fmt.Printf("❌ Provider for '%s' not found. Use prefix like 'openai/gpt-4o-mini' or 'ant/claude-sonnet-4-5'\n", modelArg)
			os.Exit(1)
		}
		if len(prov.APIKeys) == 0 {
			fmt.Printf("❌ Provider '%s' has no API keys. Run: gorout add-key %s\n", prov.ID, prov.ID)
			os.Exit(1)
		}

		body := map[string]any{
			"model":      modelArg, // keep the prefix so GoRout can route
			"messages":   []map[string]string{{"role": "user", "content": prompt}},
			"max_tokens": maxTokens,
			"stream":     stream,
		}
		if stream {
			body["stream_options"] = map[string]any{"include_usage": true}
		}
		bodyBytes, _ := json.Marshal(body)

		// Reuse the first available GoRout API key (test-models runs against the local proxy).
		var key string
		if len(cfg.APIKeys) > 0 {
			key = cfg.APIKeys[0].Key
		} else {
			fmt.Println("❌ No gorout API key. Run: gorout generate-key --label test")
			os.Exit(1)
		}

		fmt.Printf("🧪 Testing %s/%s (provider=%s, base=%s)\n", prov.Prefix, resolvedModel, prov.ID, prov.BaseURL)
		fmt.Printf("   prompt:    %q\n", prompt)
		fmt.Printf("   max-tokens:%d   stream:%v\n", maxTokens, stream)
		keyIdx := int(atomic.LoadUint64(&prov.rrCounter)) % len(prov.APIKeys)
		fmt.Printf("   key idx:   %d/%d (rotates per call)\n", keyIdx, len(prov.APIKeys))
		fmt.Println()

		start := time.Now()
		req, _ := http.NewRequest("POST", "http://127.0.0.1:"+strconv.Itoa(cfg.Settings.Port)+"/v1/chat/completions", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		// Skip GoRout's own auth for this localhost loopback — easier: use the key directly.
		resp, err := http.DefaultClient.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			fmt.Printf("❌ Request failed (%v): %v\n", elapsed, err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)

		fmt.Printf("📥 Response: HTTP %d (%v)\n", resp.StatusCode, elapsed)
		if resp.StatusCode >= 400 {
			fmt.Println("❌ Error body:")
			fmt.Println(string(respBody))
			os.Exit(1)
		}

		if stream {
			// Show first text delta + parse usage
			text := ""
			var inTok, outTok uint64
			var modelSeen string
			for _, line := range strings.Split(string(respBody), "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				payload := strings.TrimPrefix(line, "data: ")
				if payload == "[DONE]" {
					break
				}
				var chunk struct {
					Model string `json:"model"`
					Choices []struct {
						Delta struct {
							Content string `json:"content"`
						} `json:"delta"`
					} `json:"choices"`
					Usage *struct {
						PromptTokens     uint64 `json:"prompt_tokens"`
						CompletionTokens uint64 `json:"completion_tokens"`
						TotalTokens      uint64 `json:"total_tokens"`
					} `json:"usage"`
				}
				if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
					continue
				}
				if chunk.Model != "" {
					modelSeen = chunk.Model
				}
				if len(chunk.Choices) > 0 {
					text += chunk.Choices[0].Delta.Content
				}
				if chunk.Usage != nil {
					inTok = chunk.Usage.PromptTokens
					outTok = chunk.Usage.CompletionTokens
				}
			}
			fmt.Printf("   text:    %s\n", strings.TrimSpace(text))
			fmt.Printf("   model:   %s\n", modelSeen)
			fmt.Printf("   tokens:  in=%d out=%d total=%d\n", inTok, outTok, inTok+outTok)
		} else {
			var r struct {
				Model   string `json:"model"`
				Choices []struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				} `json:"choices"`
				Usage struct {
					PromptTokens     uint64 `json:"prompt_tokens"`
					CompletionTokens uint64 `json:"completion_tokens"`
					TotalTokens      uint64 `json:"total_tokens"`
				} `json:"usage"`
			}
			if err := json.Unmarshal(respBody, &r); err == nil {
				content := ""
				if len(r.Choices) > 0 {
					content = r.Choices[0].Message.Content
				}
				fmt.Printf("   model:   %s\n", r.Model)
				fmt.Printf("   text:    %s\n", strings.TrimSpace(content))
				fmt.Printf("   tokens:  in=%d out=%d total=%d\n", r.Usage.PromptTokens, r.Usage.CompletionTokens, r.Usage.TotalTokens)
			} else {
				fmt.Println(string(respBody))
			}
		}

	case "generate-key":
		label := ""
		for i, arg := range os.Args {
			if arg == "--label" && i+1 < len(os.Args) {
				label = os.Args[i+1]
			}
		}
		if label == "" {
			fmt.Println("❌ Usage: gorout generate-key --label \"my-laptop\"")
			os.Exit(1)
		}
		apiKey, err := cfg.GenerateKey(label)
		if err != nil {
			fmt.Printf("❌ %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ API key generated!\n")
		fmt.Printf("   Label:   %s\n", apiKey.Label)
		fmt.Printf("   Key:     %s\n", apiKey.Key)
		fmt.Printf("   Created: %s\n", apiKey.CreatedAt.Format("2006-01-02 15:04:05"))

	case "list-keys":
		keys := cfg.ListKeys()
		if len(keys) == 0 {
			fmt.Println("No API keys found.")
			os.Exit(0)
		}
		fmt.Printf("API Keys (%d):\n\n", len(keys))
		fmt.Printf("  %-20s %-25s %-20s %s\n", "LABEL", "KEY", "CREATED", "LAST USED")
		fmt.Println("  " + strings.Repeat("-", 90))
		for _, k := range keys {
			used := "never"
			if !k.LastUsed.IsZero() {
				used = k.LastUsed.Format("2006-01-02 15:04")
			}
			fmt.Printf("  %-20s %-25s %-20s %s\n", k.Label, maskKey(k.Key), k.CreatedAt.Format("2006-01-02 15:04"), used)
		}

	case "view":
		if len(os.Args) < 3 {
			fmt.Println("❌ Usage: gorout view <label>")
			os.Exit(1)
		}
		label := os.Args[2]
		apiKey, err := cfg.ViewKey(label)
		if err != nil {
			fmt.Printf("❌ %v\n", err)
			os.Exit(1)
		}
		used := "never"
		if !apiKey.LastUsed.IsZero() {
			used = apiKey.LastUsed.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("API Key Details:\n")
		fmt.Printf("   Label:     %s\n", apiKey.Label)
		fmt.Printf("   Key:       %s\n", apiKey.Key)
		fmt.Printf("   Created:   %s\n", apiKey.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("   Last Used: %s\n", used)

	case "delete-key":
		if len(os.Args) < 3 {
			fmt.Println("❌ Usage: gorout delete-key <label>")
			os.Exit(1)
		}
		label := os.Args[2]
		err := cfg.DeleteKey(label)
		if err != nil {
			fmt.Printf("❌ %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ API key '%s' deleted\n", label)

	case "remove-key":
		if len(os.Args) < 4 {
			fmt.Println("❌ Usage: gorout remove-key <provider-id> <index>")
			os.Exit(1)
		}
		provID := os.Args[2]
		idx, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Printf("❌ Index must be a number, got: %s\n", os.Args[3])
			os.Exit(1)
		}
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		for i := range cfg.Providers {
			if cfg.Providers[i].ID != provID {
				continue
			}
			if idx < 0 || idx >= len(cfg.Providers[i].APIKeys) {
				fmt.Printf("❌ Index %d out of range (have %d key(s))\n", idx, len(cfg.Providers[i].APIKeys))
				os.Exit(1)
			}
			if len(cfg.Providers[i].APIKeys) == 1 {
				fmt.Printf("❌ Cannot remove last key for provider '%s' (need at least 1)\n", provID)
				os.Exit(1)
			}
			cfg.Providers[i].APIKeys = append(cfg.Providers[i].APIKeys[:idx], cfg.Providers[i].APIKeys[idx+1:]...)
			cfg.saveLocked()
			fmt.Printf("✅ Removed key #%d from provider '%s' (%d key(s) left)\n", idx, provID, len(cfg.Providers[i].APIKeys))
			return
		}
		fmt.Printf("❌ Provider '%s' not found\n", provID)
		os.Exit(1)

	case "add-key":
		if len(os.Args) < 3 {
			fmt.Println("❌ Usage: gorout add-key <provider-id>")
			fmt.Println("   Then paste the API key when prompted")
			os.Exit(1)
		}
		provID := os.Args[2]
		fmt.Print("API Key to add: ")
		var newKey string
		fmt.Scanln(&newKey)
		if newKey == "" {
			fmt.Println("❌ Empty key, aborting")
			os.Exit(1)
		}
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		for i := range cfg.Providers {
			if cfg.Providers[i].ID == provID {
				cfg.Providers[i].APIKeys = append(cfg.Providers[i].APIKeys, newKey)
				count := len(cfg.Providers[i].APIKeys)
				cfg.saveLocked()
				fmt.Printf("✅ Key added to '%s' (now %d key(s), will rotate)\n", provID, count)
				return
			}
		}
		fmt.Printf("❌ Provider '%s' not found\n", provID)
		os.Exit(1)

	case "usage-reset":
		cfg.Usage = newUsage()
		cfg.saveUsage()
		fmt.Println("✅ Usage counters reset")

	case "usage":
		if len(cfg.Usage.ByProvider) == 0 && cfg.Usage.Total.Requests == 0 {
			fmt.Println("📊 No usage recorded yet.")
			fmt.Println("   Make a request to a model, then run this again.")
			os.Exit(0)
		}
		fmt.Printf("📊 Usage Stats\n")
		fmt.Printf("   Tracking started: %s\n", cfg.Usage.StartedAt)
		if cfg.Usage.LastFlushAt != "" {
			fmt.Printf("   Last flushed:     %s\n", cfg.Usage.LastFlushAt)
		}
		fmt.Println()
		fmt.Println("  TOTAL")
		fmt.Printf("    Requests:       %d\n", cfg.Usage.Total.Requests)
		fmt.Printf("    Input tokens:   %d\n", cfg.Usage.Total.InputTokens)
		fmt.Printf("    Output tokens:  %d\n", cfg.Usage.Total.OutputTokens)
		fmt.Printf("    Total tokens:   %d\n", cfg.Usage.Total.TotalTokens)
		fmt.Printf("    Errors:         %d\n", cfg.Usage.Total.Errors)
		fmt.Println()
		fmt.Println("  PER PROVIDER")
		// Sort providers by total tokens desc (simple bubble sort, n is small)
		type pp struct {
			id string
			u  *ProviderUsage
		}
		provs := make([]pp, 0, len(cfg.Usage.ByProvider))
		for k, v := range cfg.Usage.ByProvider {
			provs = append(provs, pp{k, v})
		}
		for i := 0; i < len(provs); i++ {
			for j := i + 1; j < len(provs); j++ {
				if provs[j].u.TotalTokens > provs[i].u.TotalTokens {
					provs[i], provs[j] = provs[j], provs[i]
				}
			}
		}
		fmt.Printf("    %-20s %8s %12s %12s %12s %8s\n", "PROVIDER", "REQS", "IN TOK", "OUT TOK", "TOT TOK", "ERRORS")
		fmt.Println("    " + strings.Repeat("-", 80))
		for _, p := range provs {
			fmt.Printf("    %-20s %8d %12d %12d %12d %8d\n",
				p.id, p.u.Requests, p.u.InputTokens, p.u.OutputTokens, p.u.TotalTokens, p.u.Errors)
		}
		fmt.Println()
		fmt.Println("  TOP MODELS (by total tokens)")
		type mm struct {
			k string
			u *ModelUsage
		}
		models := make([]mm, 0, len(cfg.Usage.ByModel))
		for k, v := range cfg.Usage.ByModel {
			models = append(models, mm{k, v})
		}
		// Sort by total tokens desc
		for i := 0; i < len(models); i++ {
			for j := i + 1; j < len(models); j++ {
				if models[j].u.TotalTokens > models[i].u.TotalTokens {
					models[i], models[j] = models[j], models[i]
				}
			}
		}
		limit := 10
		if len(models) < limit {
			limit = len(models)
		}
		fmt.Printf("    %-20s %-32s %6s %12s %12s %12s\n", "PROVIDER", "MODEL", "REQS", "IN TOK", "OUT TOK", "TOT TOK")
		fmt.Println("    " + strings.Repeat("-", 100))
		for i := 0; i < limit; i++ {
			m := models[i].u
			fmt.Printf("    %-20s %-32s %6d %12d %12d %12d\n",
				m.Provider, truncStr(m.Model, 32), m.Requests, m.InputTokens, m.OutputTokens, m.TotalTokens)
		}
		if len(models) > limit {
			fmt.Printf("    ... and %d more model(s)\n", len(models)-limit)
		}

	case "version":
		fmt.Println("GoRout v1.0.0")

	default:
		fmt.Printf("Unknown command: %s\n\n", cmd)
		usage()
		os.Exit(1)
	}
}
