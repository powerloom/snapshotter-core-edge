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

Set a Tempo-capable key for the **payer**:

```bash
export TEMPO_PRIVATE_KEY=0x...   # default for TempoAccount.from_env(); override name via from_env("OTHER_VAR")
# Same chain as Core API (default Moderato testnet):
export TEMPO_CHAIN_ID=42431      # or MPP_TEMPO_CHAIN_ID; use 4217 for Tempo mainnet
```

### Fund the payer (required)

MPP charges are paid in **TIP-20** on Tempo (e.g. **pathUSD**). Gas on Tempo is also paid in a **fee token**, not native ETH for these flows. Your payer address must hold **enough of that token** for **both** the transfer and the fee.

1. Derive the payer address from `TEMPO_PRIVATE_KEY` (or check in a block explorer).
2. Use the [Tempo faucet](https://docs.tempo.xyz/quickstart/faucet) and send **pathUSD** (or whatever matches the server’s `MPP_TEMPO_CURRENCY`) **to that exact address**.
3. Ensure the server’s `MPP_TEMPO_CURRENCY` matches the token you funded (same chain / testnet).
4. **Chain ID must match** where you funded: Core API uses **`MPP_TEMPO_CHAIN_ID`** (default **42431** Moderato). If it was unset, pympp previously defaulted to **mainnet (4217)** while balances live on testnet—set **`MPP_TEMPO_CHAIN_ID=42431`** on the server and align **`TEMPO_CHAIN_ID`** / **`MPP_TEMPO_CHAIN_ID`** for `fetch_paid.py`.

If the API returns `insufficient funds … have 0 want …` despite a funded wallet, re-check **chain** (42431 vs 4217) and **token contract** vs `MPP_TEMPO_CURRENCY`.

## Run

Use the environment where **pympp** is installed. Bare `python fetch_paid.py` uses your system Python and will fail with `No module named 'mpp'` unless you installed pympp there.

```bash
poetry run python fetch_paid.py --base-url https://your-host:9003 --path /mpp/snapshot/allTrades
```

Equivalent without Poetry: `pip install 'pympp[tempo]>=0.4.2'` then `python fetch_paid.py`.

`fetch_paid.py` sets pympp’s `DEFAULT_GAS_LIMIT` to **1e6** before requests: stock pympp **100000** is below Tempo AA intrinsic gas when `eth_estimateGas` fails silently.

Successful payments expose **`Payment-Receipt`** on the HTTP response (not the JSON body). The script prints **`payment_reference_tx`** — that value is usually the **Tempo transaction hash** to look up on a block explorer.

### Tempo block explorers (official)

Use the explorer that matches **`TEMPO_CHAIN_ID` / `MPP_TEMPO_CHAIN_ID`** ([connection details](https://docs.tempo.xyz/quickstart/connection-details)):

| Network | Chain ID | Block explorer |
|--------|----------|----------------|
| Tempo mainnet | 4217 | [explore.tempo.xyz](https://explore.tempo.xyz) |
| Tempo testnet (Moderato) | 42431 | [explore.testnet.tempo.xyz](https://explore.testnet.tempo.xyz) |

Default MPP settings here target **Moderato (42431)** — open addresses and tx hashes on **testnet** explorer, not mainnet.

## POWER token (later)

Settlement in **POWER** on Powerloom L2 / Ethereum is **not** implemented here. This client uses **Tempo TIP-20** (e.g. pathUSD) per MPP charge. See **ai-coord-docs** `bds-mpp-integration/08-power-token-payments.md` (separate documentation repo).
