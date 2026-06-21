package main

import (
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

type Config struct {
	mu         sync.RWMutex
	Providers  []Provider `json:"providers"`
	APIKeys    []APIKey   `json:"api_keys"`
	Settings   Settings   `json:"settings"`
	configPath string
	httpClient *http.Client
}

func NewConfig(path string) *Config {
	c := &Config{
		configPath: path,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		Settings:   Settings{Port: 9988, LogLevel: "info"},
	}
	c.load()
	return c
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

	for _, p := range c.Providers {
		if !p.Enabled {
			continue
		}
		prefixSlash := p.Prefix + "/"
		if strings.HasPrefix(modelName, prefixSlash) {
			originalModel := strings.TrimPrefix(modelName, prefixSlash)
			return &p, originalModel
		}
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

	resp, err := p.client.Do(req)
	if err != nil {
		p.jsonError(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
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
  gorout fetch-models             Fetch models from all providers
  gorout list-models              List all models with prefixes
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

	case "list-models":
		models := cfg.GetAllModels()
		if len(models) == 0 {
			fmt.Println("No models cached. Run: gorout fetch-models")
			os.Exit(0)
		}
		fmt.Printf("Models (%d):\n\n", len(models))
		fmt.Printf("  %-55s %-15s\n", "MODEL ID", "PROVIDER")
		fmt.Println("  " + strings.Repeat("-", 70))
		for _, m := range models {
			fmt.Printf("  %-55s %-15s\n", m.ID, m.Provider)
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

	case "version":
		fmt.Println("GoRout v1.0.0")

	default:
		fmt.Printf("Unknown command: %s\n\n", cmd)
		usage()
		os.Exit(1)
	}
}
