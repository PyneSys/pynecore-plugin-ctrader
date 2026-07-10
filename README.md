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

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
