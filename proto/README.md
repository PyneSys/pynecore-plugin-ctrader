# Vendored cTrader Open API protobuf definitions

These `.proto` files are vendored verbatim from Spotware's official
[`openapi-proto-messages`](https://github.com/spotware/openapi-proto-messages)
repository, pinned at commit `3fd8bddfbe0cfc2ecfda079623dc4e498af11e66`.

| File                               | Role                                                                       |
|------------------------------------|----------------------------------------------------------------------------|
| `OpenApiCommonMessages.proto`      | Transport envelope: `ProtoMessage`, `ProtoErrorRes`, `ProtoHeartbeatEvent`. |
| `OpenApiCommonModelMessages.proto` | Common enums: `ProtoPayloadType`, `ProtoErrorCode`.                        |
| `OpenApiMessages.proto`            | Concrete `ProtoOA*` request/response/event messages.                       |
| `OpenApiModelMessages.proto`       | OpenApi enums and models (`ProtoOAPayloadType`, `ProtoOATrendbar`, ...).   |

## Regenerating the Python modules

The generated modules live in `src/pynecore_ctrader/messages/*_pb2.py` and are
committed, so end users only need the `protobuf` runtime. To regenerate them
(e.g. after bumping the pinned commit above), install the `dev` extra and run
the generator:

```bash
pip install -e ".[dev]"   # provides grpcio-tools
python proto/generate.py
```

The generator compiles the four `.proto` files together and rewrites their
cross-file imports to be package-relative.
