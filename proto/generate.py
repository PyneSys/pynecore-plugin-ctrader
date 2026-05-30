"""Regenerate the vendored cTrader Open API protobuf modules.

Compiles the ``.proto`` files in this directory into
``src/pynecore_ctrader/messages/*_pb2.py`` and rewrites the cross-file imports
to be package-relative, so the generated modules import correctly as
``pynecore_ctrader.messages.*`` (``protoc`` emits top-level ``import X_pb2``
statements that do not resolve inside a package).

The generated modules are committed to the repository, so end users only need
the ``protobuf`` runtime. Regenerate (e.g. after bumping the pinned commit in
``README.md``) with the ``dev`` extra installed::

    pip install -e ".[dev]"   # provides grpcio-tools
    python proto/generate.py
"""
import re
from pathlib import Path

from grpc_tools import protoc

PROTO_DIR = Path(__file__).resolve().parent
PLUGIN_ROOT = PROTO_DIR.parent
OUT_DIR = PLUGIN_ROOT / "src" / "pynecore_ctrader" / "messages"

PROTO_FILES = (
    "OpenApiCommonModelMessages.proto",
    "OpenApiCommonMessages.proto",
    "OpenApiModelMessages.proto",
    "OpenApiMessages.proto",
)

#: Matches the leading ``import OpenApi*_pb2`` that ``protoc`` emits for the
#: sibling generated modules; the trailing `` as X__pb2`` (if any) is preserved.
_CROSS_IMPORT = re.compile(r"^import (OpenApi\w+_pb2)\b", re.MULTILINE)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rc = protoc.main([
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--pyi_out={OUT_DIR}",
        *(str(PROTO_DIR / name) for name in PROTO_FILES),
    ])
    if rc != 0:
        return rc

    for generated in sorted([*OUT_DIR.glob("*_pb2.py"), *OUT_DIR.glob("*_pb2.pyi")]):
        text = generated.read_text()
        fixed = _CROSS_IMPORT.sub(r"from . import \1", text)
        if fixed != text:
            generated.write_text(fixed)
        print(f"  {generated.name}")

    (OUT_DIR / "__init__.py").write_text(
        '"""Generated cTrader Open API protobuf modules.\n\n'
        "Do not edit by hand; regenerate with ``python proto/generate.py``\n"
        '(see ``proto/README.md``).\n"""\n'
    )
    print(f"-> {OUT_DIR.relative_to(PLUGIN_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
