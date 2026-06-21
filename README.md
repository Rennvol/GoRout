# GoRout

Lightweight AI API proxy with model routing. Single binary, zero dependencies, written in Go.

## Features

- **Multi-provider routing** — proxy to OpenAI, OpenRouter, Anthropic, Groq, Ollama, any OpenAI-compatible API
- **Custom prefix per provider** — define your own prefix (e.g. `or`, `openai`, `ant`)
- **Model routing** — `prefix/model-name` auto-routes to the correct provider
- **Model fetching** — auto-fetch available models from each provider's `/models` endpoint
- **API key management** — generate, list, view, delete keys with labels
- **Single binary** — no runtime, no dependencies (Go stdlib only)
- **~6 MB RAM idle** — perfect for low-resource servers (ARM, Raspberry Pi, etc.)

## Quick Start

```bash
# Build
go build -o gorout main.go

# Add a provider (interactive)
./gorout add-provider
# Provider name: openrouter
# Base URL: https://openrouter.ai/api/v1
# API Key: sk-or-v1-xxx
# Prefix: or

# Fetch models
./gorout fetch-models

# List models (shows: or/anthropic/claude-sonnet-4, or/openai/gpt-4o, etc.)
./gorout list-models

# Generate API key
./gorout generate-key --label "laptop"

# Start server
./gorout start
```

## Usage

### Proxy

```bash
# With prefix routing — routes to OpenRouter
curl http://localhost:9988/v1/chat/completions \
  -H "Authorization: Bearer gr_xxx" \
  -H "Content-Type: application/json" \
  -d '{"model":"or/anthropic/claude-sonnet-4","messages":[{"role":"user","content":"Hello"}]}'

# Without prefix — falls back to first enabled provider
curl http://localhost:9988/v1/chat/completions \
  -H "Authorization: Bearer gr_xxx" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Hello"}]}'
```

### CLI Commands

| Command | Description |
|---|---|
| `gorout start` | Start proxy server |
| `gorout stop` | Stop server |
| `gorout status` | Show server status |
| `gorout config` | Show config |
| `gorout add-provider` | Add AI provider (interactive) |
| `gorout fetch-models` | Fetch models from all providers |
| `gorout list-models` | List all models with prefixes |
| `gorout generate-key --label X` | Generate API key |
| `gorout list-keys` | List all API keys (masked) |
| `gorout view <label>` | Show full API key |
| `gorout delete-key <label>` | Delete API key |
| `gorout version` | Show version |

### Internal API

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Health check |
| `/api/providers` | GET | List enabled providers |
| `/api/providers` | POST | Add provider |
| `/api/providers/:id` | DELETE | Delete provider |
| `/api/models` | GET | List all models with prefixes |
| `/api/models/refresh` | POST | Fetch models from all providers |
| `/api/settings` | GET / PUT | Get/update settings |

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GOROUT_PORT` | `9988` | Override port |
| `GOROUT_HOME` | `~/.gorout` | Config directory |

## How Prefix Routing Works

```
User sends: model = "or/anthropic/claude-sonnet-4"
                 ↑  ↑
                 |  └── original model name sent to provider
                 └── prefix matches provider "openrouter"

GoRout routes to: openrouter provider
Provider receives: model = "anthropic/claude-sonnet-4"
```

Add multiple providers with different prefixes:
```
or/anthropic/claude-sonnet-4   → OpenRouter
ant/claude-sonnet-4            → Anthropic direct
openai/gpt-4o                  → OpenAI direct
groq/llama-3.1-70b             → Groq
ollama/llama3                  → Local Ollama
```

## Build

```bash
go build -o gorout main.go
```

Binary is static, ~7.6 MB, works on any Linux ARM64/amd64.

## License

MIT
