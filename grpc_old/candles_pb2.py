# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: FinamPy/grpc_old/candles.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'FinamPy/grpc_old/candles.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from FinamPy.proto_old import candles_pb2 as FinamPy_dot_proto__old_dot_candles__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1e\x46inamPy/grpc_old/candles.proto\x12\x10grpc.tradeapi.v1\x1a\x1f\x46inamPy/proto_old/candles.proto2\xdc\x01\n\x07\x43\x61ndles\x12`\n\rGetDayCandles\x12\'.proto.tradeapi.v1.GetDayCandlesRequest\x1a&.proto.tradeapi.v1.GetDayCandlesResult\x12o\n\x12GetIntradayCandles\x12,.proto.tradeapi.v1.GetIntradayCandlesRequest\x1a+.proto.tradeapi.v1.GetIntradayCandlesResultB\x19\xaa\x02\x16\x46inam.TradeApi.Grpc.V1b\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'FinamPy.grpc_old.candles_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'\252\002\026Finam.TradeApi.Grpc.V1'
  _globals['_CANDLES']._serialized_start=86
  _globals['_CANDLES']._serialized_end=306
# @@protoc_insertion_point(module_scope)
