# PyneCore cTrader Plugin

cTrader [Open API](https://help.ctrader.com/open-api/) integration for
[PyneCore](https://pynesys.io). A single plugin that works across the many
brokers running cTrader (Pepperstone, IC Markets, FxPro, Spotware, and more),
since they all speak the same Open API.

## Status

Early development. The first milestone is the **data provider**
(`LiveProviderPlugin`): OAuth2 authentication, symbol mapping, and historical
plus live OHLCV. Live order execution (`BrokerPlugin`) follows.

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
- **Order model** (later milestone): position-based with server-side
  stop-loss / take-profit / trailing stop as position attributes; netting
  (one-way) accounts.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
