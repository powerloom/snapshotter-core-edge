# mpp-bds-client

Small **Python** harness to call **paid** BDS Core API routes (`/mpp/snapshot/...`) using the same **[pympp](https://mpp.dev/sdk/python/)** client stack as the server: **HTTP 402** → Tempo **charge** → retry with `Authorization` / receipt handling.

## Why Python

- Matches server (`pympp`, `TempoAccount`, `Client`).
- Fewer moving parts than wiring a browser wallet for a quick API smoke test.

Alternatives for agents: **TypeScript** + MPP client, or **[Tempo CLI](https://docs.tempo.xyz/cli/request)** (`tempo request`) for paid HTTP.

## Setup

From the **snapshotter-core-edge** repository root:

```bash
cd tools/mpp-bds-client
poetry install
```

## Environment (client wallet — not `MPP_SECRET_KEY`)

Set a Tempo-capable key for the **payer** (testnet faucet: [Tempo faucet](https://docs.tempo.xyz/quickstart/faucet)):

```bash
export TEMPO_PRIVATE_KEY=0x...   # default for TempoAccount.from_env(); override name via from_env("OTHER_VAR")
```

## Run

Use the environment where **pympp** is installed. Bare `python fetch_paid.py` uses your system Python and will fail with `No module named 'mpp'` unless you installed pympp there.

```bash
poetry run python fetch_paid.py --base-url https://your-host:9003 --path /mpp/snapshot/allTrades
```

Equivalent without Poetry: `pip install 'pympp[tempo]>=0.4.2'` then `python fetch_paid.py`.

`fetch_paid.py` sets pympp’s `DEFAULT_GAS_LIMIT` to **1e6** before requests: stock pympp **100000** is below Tempo AA intrinsic gas when `eth_estimateGas` fails silently.

Fund the wallet with test **pathUSD** (or the currency your server’s `MPP_TEMPO_CURRENCY` expects) on **Tempo testnet** before calling.

## POWER token (later)

Settlement in **POWER** on Powerloom L2 / Ethereum is **not** implemented here. This client uses **Tempo TIP-20** (e.g. pathUSD) per MPP charge. See **ai-coord-docs** `bds-mpp-integration/08-power-token-payments.md` (separate documentation repo).
