from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class ProtoPayloadType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROTO_MESSAGE: _ClassVar[ProtoPayloadType]
    ERROR_RES: _ClassVar[ProtoPayloadType]
    HEARTBEAT_EVENT: _ClassVar[ProtoPayloadType]

class ProtoErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNKNOWN_ERROR: _ClassVar[ProtoErrorCode]
    UNSUPPORTED_MESSAGE: _ClassVar[ProtoErrorCode]
    INVALID_REQUEST: _ClassVar[ProtoErrorCode]
    TIMEOUT_ERROR: _ClassVar[ProtoErrorCode]
    ENTITY_NOT_FOUND: _ClassVar[ProtoErrorCode]
    CANT_ROUTE_REQUEST: _ClassVar[ProtoErrorCode]
    FRAME_TOO_LONG: _ClassVar[ProtoErrorCode]
    MARKET_CLOSED: _ClassVar[ProtoErrorCode]
    CONCURRENT_MODIFICATION: _ClassVar[ProtoErrorCode]
    BLOCKED_PAYLOAD_TYPE: _ClassVar[ProtoErrorCode]
PROTO_MESSAGE: ProtoPayloadType
ERROR_RES: ProtoPayloadType
HEARTBEAT_EVENT: ProtoPayloadType
UNKNOWN_ERROR: ProtoErrorCode
UNSUPPORTED_MESSAGE: ProtoErrorCode
INVALID_REQUEST: ProtoErrorCode
TIMEOUT_ERROR: ProtoErrorCode
ENTITY_NOT_FOUND: ProtoErrorCode
CANT_ROUTE_REQUEST: ProtoErrorCode
FRAME_TOO_LONG: ProtoErrorCode
MARKET_CLOSED: ProtoErrorCode
CONCURRENT_MODIFICATION: ProtoErrorCode
BLOCKED_PAYLOAD_TYPE: ProtoErrorCode
