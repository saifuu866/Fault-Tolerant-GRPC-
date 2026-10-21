"""One-command launcher: starts the 3 backend servers + the Flask dashboard.

    python run.py                    # everything (Ctrl+C stops all)
    python run.py --servers-only     # only the backends
    python run.py --separate-windows # each server in its own console (Windows)
"""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from servers.server_config import (DASHBOARD_HOST, DASHBOARD_PORT,   # noqa: E402
                                   SERVERS)


def ensure_stubs():
    """Generate the gRPC stubs if they are missing."""
    try:
        import proto_stubs  # noqa: F401
        return True
    except ImportError:
        print("gRPC stubs missing - generating them now...")
        code = subprocess.call([sys.executable,
                                os.path.join(ROOT, "generate_protos.py")])
        return code == 0


def start_servers(separate_windows=False):
    processes = []
    for cfg in SERVERS:
        cmd = [sys.executable, os.path.join(ROOT, "servers", "server.py"),
               "--id", cfg["id"]]
        creationflags = 0
        if separate_windows and os.name == "nt":
            creationflags = subprocess.CREATE_NEW_CONSOLE
        print(f"starting {cfg['id']} on port {cfg['port']}")
        processes.append(subprocess.Popen(cmd, cwd=ROOT, creationflags=creationflags))
    return processes


def stop_servers(processes):
    print("\nstopping backend servers...")
    for process in processes:
        try:
            process.terminate()
        except Exception:
            pass
    for process in processes:
        try:
            process.wait(timeout=5)
        except Exception:
            process.kill()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--servers-only", action="store_true")
    parser.add_argument("--separate-windows", action="store_true")
    args = parser.parse_args()

    if not ensure_stubs():
        print("Could not generate gRPC stubs. Run: pip install -r requirements.txt")
        return 1

    processes = start_servers(args.separate_windows)
    time.sleep(2.0)   # give the gRPC servers time to bind their ports

    try:
        if args.servers_only:
            print("servers running - press Ctrl+C to stop")
            while True:
                time.sleep(1)
        else:
            from dashboard.app import main as dashboard_main
            print(f"\nDashboard: http://{DASHBOARD_HOST}:{DASHBOARD_PORT}\n")
            dashboard_main()
    except KeyboardInterrupt:
        pass
    finally:
        stop_servers(processes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
