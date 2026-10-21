"""Generates the Python gRPC stubs from proto/service.proto into generated/.

Run:  python generate_protos.py
"""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.join(ROOT, "proto")
OUT_DIR = os.path.join(ROOT, "generated")


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    # make the folder importable as a package too
    init_file = os.path.join(OUT_DIR, "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "w", encoding="utf-8") as handle:
            handle.write("# generated gRPC stubs live here\n")

    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        os.path.join(PROTO_DIR, "service.proto"),
    ]
    print("Running:", " ".join(cmd))
    code = subprocess.call(cmd)
    if code != 0:
        print("\nFAILED. Install the compiler with:  pip install grpcio-tools")
        return code
    print("OK -> generated/service_pb2.py and generated/service_pb2_grpc.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
