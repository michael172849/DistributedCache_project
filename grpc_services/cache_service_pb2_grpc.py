# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import payload_pb2 as payload__pb2


class CacheServiceStub(object):
    """Missing associated documentation comment in .proto file"""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.setContent = channel.unary_unary(
                '/CacheService/setContent',
                request_serializer=payload__pb2.Request.SerializeToString,
                response_deserializer=payload__pb2.Response.FromString,
                )
        self.getContent = channel.unary_unary(
                '/CacheService/getContent',
                request_serializer=payload__pb2.Request.SerializeToString,
                response_deserializer=payload__pb2.Response.FromString,
                )
        self.invalidate = channel.unary_unary(
                '/CacheService/invalidate',
                request_serializer=payload__pb2.Request.SerializeToString,
                response_deserializer=payload__pb2.Response.FromString,
                )


class CacheServiceServicer(object):
    """Missing associated documentation comment in .proto file"""

    def setContent(self, request, context):
        """Missing associated documentation comment in .proto file"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def getContent(self, request, context):
        """Missing associated documentation comment in .proto file"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def invalidate(self, request, context):
        """Missing associated documentation comment in .proto file"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_CacheServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'setContent': grpc.unary_unary_rpc_method_handler(
                    servicer.setContent,
                    request_deserializer=payload__pb2.Request.FromString,
                    response_serializer=payload__pb2.Response.SerializeToString,
            ),
            'getContent': grpc.unary_unary_rpc_method_handler(
                    servicer.getContent,
                    request_deserializer=payload__pb2.Request.FromString,
                    response_serializer=payload__pb2.Response.SerializeToString,
            ),
            'invalidate': grpc.unary_unary_rpc_method_handler(
                    servicer.invalidate,
                    request_deserializer=payload__pb2.Request.FromString,
                    response_serializer=payload__pb2.Response.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'CacheService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class CacheService(object):
    """Missing associated documentation comment in .proto file"""

    @staticmethod
    def setContent(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheService/setContent',
            payload__pb2.Request.SerializeToString,
            payload__pb2.Response.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def getContent(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheService/getContent',
            payload__pb2.Request.SerializeToString,
            payload__pb2.Response.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def invalidate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheService/invalidate',
            payload__pb2.Request.SerializeToString,
            payload__pb2.Response.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)
