## Intro
 This module is mostly used to manage users and API keys using Redis (might be moved out in the future).
 There are several components in this module. Let's talk about them one by one.

### Settings and Config

#### Config JSON
The primary config for this module is defined in `auth_settings.json`. To help users get started, a boilerplate is provided in `auth_settings.example.json` which can be used to generate their own `auth_settings.json`.

The current settings structure is something like this

```json
{
  "redis": {
      "host": <host>,
      "port": <port>,
      "db": <db>,
      "password": <password>
    },
  "bind": {
    "host": <bind_host>,
    "port": <bind_port>
  }
}
```
The `redis` part of the config is used to configure the Redis instance that will be used to connect as the primary datastore.
The `bind` part of the config is used to configure the `host` and `port` where the API server will run.

#### Setting Models
The config JSON structure above is not entirely flexible. Everything from the auth_settings.json is loaded in well-defined `Settings Models` defined in `settings_models.py` using `conf.py`.
If any new config needs to be added, users must update the Setting Models first. This will make sure we always have a proper and updated data model for app configuration.

### Redis Utilities
`redis_conn.py` contains utility and helper functions for Redis pool setup and `redis_keys.py` contains functions to generate all the `keys` that are used in Redis across the entire module.

### Server

The main FastAPI server and entry point for this module is `server_entry.py`, this server is started using a custom Gunicorn handler present in `gunicorn_auth_entry_launcher.py`. Doing so provides more flexibility to customize the application and start it using `Pm2`.

### Helpers

#### `helpers.py` and `rate_limiter.py` (API keys + rate limits on routes)

These modules implement FastAPI dependencies (`auth_check`, `rate_limit_auth_check`, etc.) using **`async_limits`** for async-safe fixed-window limits backed by Redis.

**Wiring status (current tree):**

- The **auth HTTP service** (`server_entry.py`) only handles user/API key CRUD in Redis. It imports `data_models` and `redis_keys`, **not** `helpers.py` or `rate_limiter.py`, so it never loads `async_limits` at runtime.
- **`core_api.py`** registers **`PublicRateLimitMiddleware`** (`snapshotter/public_rate_limit.py`) for free routes; it reuses **`rate_limiter.generic_rate_limiter`** and main `app.state.redis_conn` (not the auth Redis). Per-route `Depends(rate_limit_auth_check)` on **`computes/`** is still optional and not wired by default.

The **`async_limits`** dependency is declared in the project `pyproject.toml` (vendored `contrib/async-limits`) so the package resolves for imports and middleware.

#### Other utilities

Additional helpers live alongside the files above (e.g. Redis key helpers in `redis_keys.py`).
