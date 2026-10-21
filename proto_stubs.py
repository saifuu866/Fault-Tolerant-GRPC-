"""Single import point for the generated gRPC stubs.

The files produced by protoc import each other with a flat name
(`import service_pb2`), so the `generated/` directory must be on sys.path.
Every module in this project imports the stubs through here:

    from proto_stubs import service_pb2, service_pb2_grpc
"""
import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
_GENERATED = os.path.join(_ROOT, "generated")

for _path in (_ROOT, _GENERATED):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    import service_pb2            # noqa: E402
    import service_pb2_grpc       # noqa: E402
except ImportError as exc:        # pragma: no cover - setup helper
    raise ImportError(
        "gRPC stubs not found.\n"
        "Run this first:   python generate_protos.py"
    ) from exc

__all__ = ["service_pb2", "service_pb2_grpc"]
