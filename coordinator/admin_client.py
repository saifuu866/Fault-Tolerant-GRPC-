"""Tiny helper used by the dashboard / elector to send Admin RPCs."""
import logging

import grpc

from proto_stubs import service_pb2, service_pb2_grpc

LOG = logging.getLogger("admin-client")


def send_admin(address, command, value="", timeout=2.0):
    """Returns (ok: bool, message: str). Never raises."""
    try:
        with grpc.insecure_channel(address) as channel:
            stub = service_pb2_grpc.ComputeServiceStub(channel)
            response = stub.Admin(
                service_pb2.AdminRequest(command=command, value=str(value)),
                timeout=timeout)
            return response.ok, response.message
    except grpc.RpcError as exc:
        message = f"{exc.code().name}: {exc.details()}"
        LOG.warning("admin %s -> %s failed: %s", command, address, message)
        return False, message
    except Exception as exc:                               # pragma: no cover
        LOG.warning("admin %s -> %s failed: %s", command, address, exc)
        return False, str(exc)
