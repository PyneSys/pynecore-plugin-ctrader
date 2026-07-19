# PyneCore cTrader Plugin

cTrader [Open API](https://help.ctrader.com/open-api/) integration for
[PyneCore](https://pynesys.io). A single plugin that works across the many
brokers running cTrader (Pepperstone, IC Markets, FxPro, Spotware, and more),
since they all speak the same Open API.

## Status

Both the **data provider** (`LiveProviderPlugin`) and **live order execution**
(`BrokerPlugin`) are implemented: OAuth2 authentication, symbol mapping,
historical plus live OHLCV, and position-based order routing with server-side
stop-loss / take-profit / trailing stop.

## Demo first

Every cTrader broker offers demo accounts — run your strategy on one before
risking funds. Set `demo = true` and the plugin connects to the demo host
(`demo.ctraderapi.com`) and only sees your demo accounts; `demo = false`
connects to `live.ctraderapi.com` and your live accounts. The OAuth token
is stored per environment, so authorize once for each you use.

## Configuration

Settings live in `workdir/config/plugins/ctrader.toml`, auto-generated from
[`CTraderConfig`](src/pynecore_ctrader/config.py) on first run:

```toml
demo = true            # demo.ctraderapi.com vs. live.ctraderapi.com
client_id = ""         # your cTrader Open API application's client id
client_secret = ""     # ... and its client secret
account_id = ""        # optional ctidTraderAccountId (see Accounts below)
```

`client_id` / `client_secret` come from your **own** cTrader Open API
application, registered on the [Open API portal](https://openapi.ctrader.com)
— there is no shared PyneSys secret and PyneSys never relays the trading
socket. When registering the application, add `http://localhost:8765` as a
redirect URI (or another port, matched with `--port` below).

### Authentication: `pyne ctrader auth`

The plugin ships a CLI command that runs the OAuth2 loopback consent flow:

```bash
pyne ctrader auth              # environment from the config's demo setting
pyne ctrader auth --live       # or force the environment explicitly
```

It opens the consent page in your browser (use `--no-browser` to print the
URL instead, e.g. on a headless server), receives the redirect on
`localhost`, exchanges the code, and stores the refreshable token pair in
the workdir cache — tokens never touch the user-edited TOML file. Run it
once per environment; after that the plugin refreshes tokens on its own.

## Accounts

One access token can grant several trading accounts. The plugin selects
the account to trade on in this order:

1. an explicit `account_id` in the config always wins;
2. otherwise the broker slug from the provider string selects it;
3. otherwise the sole account of the right kind (demo/live) is used.

An ambiguous choice fails at startup with the candidate
`ctidTraderAccountId` values listed — copy the one you want into
`account_id`.

## Symbols

cTrader is a multi-broker platform, so the provider string carries an
optional broker segment before the symbol:

```
ctrader:EURUSD@60                  # sole account decides the broker
ctrader:pepperstoneuk:EURUSD@60    # broker slug selects the account
```

Discovery commands:

```bash
pyne data download ctrader --list-brokers            # your brokers' slugs
pyne data download ctrader:<broker> --list-symbols   # a broker's symbols
```

Symbols use the broker's native cTrader names; Pine scripts written with
TradingView-style symbols keep working through the optional `symbol_map`
translation table in the config.

## Architecture

- **Transport**: Protobuf v2 over a persistent TCP+TLS connection
  (`demo.ctraderapi.com` / `live.ctraderapi.com`, port 5035). The plugin ships a
  thin asyncio client built on the generated Protobuf message classes — it does
  not depend on the Twisted-based official SDK, to fit PyneCore's asyncio event
  loop.
- **Authentication**: OAuth2. A registered cTrader application provides the
  client id and secret; the end user grants account access in the browser and
  the plugin stores the refreshable access token.
- **Market data**: historical trendbars and live spot/trendbar subscriptions.
- **Order model**: position-based with server-side stop-loss / take-profit /
  trailing stop as position attributes. Netting (one-way) accounts use the
  direct execution path; hedging accounts run through PyneCore's one-way
  emulation layer, so Pine one-way semantics hold on both.
- **Push order events**: order state and fills arrive over the
  `ProtoOAExecutionEvent` stream — no polling. Bracket changes go through
  native atomic amends instead of cancel-and-recreate.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
