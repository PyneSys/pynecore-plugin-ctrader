from . import OpenApiCommonModelMessages_pb2 as _OpenApiCommonModelMessages_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProtoMessage(_message.Message):
    __slots__ = ("payloadType", "payload", "clientMsgId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CLIENTMSGID_FIELD_NUMBER: _ClassVar[int]
    payloadType: int
    payload: bytes
    clientMsgId: str
    def __init__(self, payloadType: _Optional[int] = ..., payload: _Optional[bytes] = ..., clientMsgId: _Optional[str] = ...) -> None: ...

class ProtoErrorRes(_message.Message):
    __slots__ = ("payloadType", "errorCode", "description", "maintenanceEndTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    ERRORCODE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MAINTENANCEENDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiCommonModelMessages_pb2.ProtoPayloadType
    errorCode: str
    description: str
    maintenanceEndTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiCommonModelMessages_pb2.ProtoPayloadType, str]] = ..., errorCode: _Optional[str] = ..., description: _Optional[str] = ..., maintenanceEndTimestamp: _Optional[int] = ...) -> None: ...

class ProtoHeartbeatEvent(_message.Message):
    __slots__ = ("payloadType",)
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiCommonModelMessages_pb2.ProtoPayloadType
    def __init__(self, payloadType: _Optional[_Union[_OpenApiCommonModelMessages_pb2.ProtoPayloadType, str]] = ...) -> None: ...
