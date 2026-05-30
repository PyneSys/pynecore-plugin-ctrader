from . import OpenApiModelMessages_pb2 as _OpenApiModelMessages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProtoOAApplicationAuthReq(_message.Message):
    __slots__ = ("payloadType", "clientId", "clientSecret")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CLIENTID_FIELD_NUMBER: _ClassVar[int]
    CLIENTSECRET_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    clientId: str
    clientSecret: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., clientId: _Optional[str] = ..., clientSecret: _Optional[str] = ...) -> None: ...

class ProtoOAApplicationAuthRes(_message.Message):
    __slots__ = ("payloadType",)
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ...) -> None: ...

class ProtoOAAccountAuthReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "accessToken")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ACCESSTOKEN_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    accessToken: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., accessToken: _Optional[str] = ...) -> None: ...

class ProtoOAAccountAuthRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAErrorRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "errorCode", "description", "maintenanceEndTimestamp", "retryAfter")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ERRORCODE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MAINTENANCEENDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    RETRYAFTER_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    errorCode: str
    description: str
    maintenanceEndTimestamp: int
    retryAfter: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., errorCode: _Optional[str] = ..., description: _Optional[str] = ..., maintenanceEndTimestamp: _Optional[int] = ..., retryAfter: _Optional[int] = ...) -> None: ...

class ProtoOAClientDisconnectEvent(_message.Message):
    __slots__ = ("payloadType", "reason")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    reason: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class ProtoOAAccountsTokenInvalidatedEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountIds", "reason")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTIDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountIds: _containers.RepeatedScalarFieldContainer[int]
    reason: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountIds: _Optional[_Iterable[int]] = ..., reason: _Optional[str] = ...) -> None: ...

class ProtoOAVersionReq(_message.Message):
    __slots__ = ("payloadType",)
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ...) -> None: ...

class ProtoOAVersionRes(_message.Message):
    __slots__ = ("payloadType", "version")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    version: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., version: _Optional[str] = ...) -> None: ...

class ProtoOANewOrderReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId", "orderType", "tradeSide", "volume", "limitPrice", "stopPrice", "timeInForce", "expirationTimestamp", "stopLoss", "takeProfit", "comment", "baseSlippagePrice", "slippageInPoints", "label", "positionId", "clientOrderId", "relativeStopLoss", "relativeTakeProfit", "guaranteedStopLoss", "trailingStopLoss", "stopTriggerMethod")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    ORDERTYPE_FIELD_NUMBER: _ClassVar[int]
    TRADESIDE_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    LIMITPRICE_FIELD_NUMBER: _ClassVar[int]
    STOPPRICE_FIELD_NUMBER: _ClassVar[int]
    TIMEINFORCE_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    BASESLIPPAGEPRICE_FIELD_NUMBER: _ClassVar[int]
    SLIPPAGEINPOINTS_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    CLIENTORDERID_FIELD_NUMBER: _ClassVar[int]
    RELATIVESTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    RELATIVETAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    GUARANTEEDSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TRAILINGSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    STOPTRIGGERMETHOD_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: int
    orderType: _OpenApiModelMessages_pb2.ProtoOAOrderType
    tradeSide: _OpenApiModelMessages_pb2.ProtoOATradeSide
    volume: int
    limitPrice: float
    stopPrice: float
    timeInForce: _OpenApiModelMessages_pb2.ProtoOATimeInForce
    expirationTimestamp: int
    stopLoss: float
    takeProfit: float
    comment: str
    baseSlippagePrice: float
    slippageInPoints: int
    label: str
    positionId: int
    clientOrderId: str
    relativeStopLoss: int
    relativeTakeProfit: int
    guaranteedStopLoss: bool
    trailingStopLoss: bool
    stopTriggerMethod: _OpenApiModelMessages_pb2.ProtoOAOrderTriggerMethod
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[int] = ..., orderType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAOrderType, str]] = ..., tradeSide: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATradeSide, str]] = ..., volume: _Optional[int] = ..., limitPrice: _Optional[float] = ..., stopPrice: _Optional[float] = ..., timeInForce: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATimeInForce, str]] = ..., expirationTimestamp: _Optional[int] = ..., stopLoss: _Optional[float] = ..., takeProfit: _Optional[float] = ..., comment: _Optional[str] = ..., baseSlippagePrice: _Optional[float] = ..., slippageInPoints: _Optional[int] = ..., label: _Optional[str] = ..., positionId: _Optional[int] = ..., clientOrderId: _Optional[str] = ..., relativeStopLoss: _Optional[int] = ..., relativeTakeProfit: _Optional[int] = ..., guaranteedStopLoss: bool = ..., trailingStopLoss: bool = ..., stopTriggerMethod: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAOrderTriggerMethod, str]] = ...) -> None: ...

class ProtoOAExecutionEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "executionType", "position", "order", "deal", "bonusDepositWithdraw", "depositWithdraw", "errorCode", "isServerEvent")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONTYPE_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    DEAL_FIELD_NUMBER: _ClassVar[int]
    BONUSDEPOSITWITHDRAW_FIELD_NUMBER: _ClassVar[int]
    DEPOSITWITHDRAW_FIELD_NUMBER: _ClassVar[int]
    ERRORCODE_FIELD_NUMBER: _ClassVar[int]
    ISSERVEREVENT_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    executionType: _OpenApiModelMessages_pb2.ProtoOAExecutionType
    position: _OpenApiModelMessages_pb2.ProtoOAPosition
    order: _OpenApiModelMessages_pb2.ProtoOAOrder
    deal: _OpenApiModelMessages_pb2.ProtoOADeal
    bonusDepositWithdraw: _OpenApiModelMessages_pb2.ProtoOABonusDepositWithdraw
    depositWithdraw: _OpenApiModelMessages_pb2.ProtoOADepositWithdraw
    errorCode: str
    isServerEvent: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., executionType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAExecutionType, str]] = ..., position: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPosition, _Mapping]] = ..., order: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAOrder, _Mapping]] = ..., deal: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOADeal, _Mapping]] = ..., bonusDepositWithdraw: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOABonusDepositWithdraw, _Mapping]] = ..., depositWithdraw: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOADepositWithdraw, _Mapping]] = ..., errorCode: _Optional[str] = ..., isServerEvent: bool = ...) -> None: ...

class ProtoOACancelOrderReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "orderId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    orderId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., orderId: _Optional[int] = ...) -> None: ...

class ProtoOAAmendOrderReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "orderId", "volume", "limitPrice", "stopPrice", "expirationTimestamp", "stopLoss", "takeProfit", "slippageInPoints", "relativeStopLoss", "relativeTakeProfit", "guaranteedStopLoss", "trailingStopLoss", "stopTriggerMethod")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    LIMITPRICE_FIELD_NUMBER: _ClassVar[int]
    STOPPRICE_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    SLIPPAGEINPOINTS_FIELD_NUMBER: _ClassVar[int]
    RELATIVESTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    RELATIVETAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    GUARANTEEDSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TRAILINGSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    STOPTRIGGERMETHOD_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    orderId: int
    volume: int
    limitPrice: float
    stopPrice: float
    expirationTimestamp: int
    stopLoss: float
    takeProfit: float
    slippageInPoints: int
    relativeStopLoss: int
    relativeTakeProfit: int
    guaranteedStopLoss: bool
    trailingStopLoss: bool
    stopTriggerMethod: _OpenApiModelMessages_pb2.ProtoOAOrderTriggerMethod
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., orderId: _Optional[int] = ..., volume: _Optional[int] = ..., limitPrice: _Optional[float] = ..., stopPrice: _Optional[float] = ..., expirationTimestamp: _Optional[int] = ..., stopLoss: _Optional[float] = ..., takeProfit: _Optional[float] = ..., slippageInPoints: _Optional[int] = ..., relativeStopLoss: _Optional[int] = ..., relativeTakeProfit: _Optional[int] = ..., guaranteedStopLoss: bool = ..., trailingStopLoss: bool = ..., stopTriggerMethod: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAOrderTriggerMethod, str]] = ...) -> None: ...

class ProtoOAAmendPositionSLTPReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionId", "stopLoss", "takeProfit", "guaranteedStopLoss", "trailingStopLoss", "stopLossTriggerMethod")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    STOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    GUARANTEEDSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TRAILINGSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    STOPLOSSTRIGGERMETHOD_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionId: int
    stopLoss: float
    takeProfit: float
    guaranteedStopLoss: bool
    trailingStopLoss: bool
    stopLossTriggerMethod: _OpenApiModelMessages_pb2.ProtoOAOrderTriggerMethod
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionId: _Optional[int] = ..., stopLoss: _Optional[float] = ..., takeProfit: _Optional[float] = ..., guaranteedStopLoss: bool = ..., trailingStopLoss: bool = ..., stopLossTriggerMethod: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAOrderTriggerMethod, str]] = ...) -> None: ...

class ProtoOAClosePositionReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionId", "volume")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionId: int
    volume: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionId: _Optional[int] = ..., volume: _Optional[int] = ...) -> None: ...

class ProtoOATrailingSLChangedEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionId", "orderId", "stopPrice", "utcLastUpdateTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    STOPPRICE_FIELD_NUMBER: _ClassVar[int]
    UTCLASTUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionId: int
    orderId: int
    stopPrice: float
    utcLastUpdateTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionId: _Optional[int] = ..., orderId: _Optional[int] = ..., stopPrice: _Optional[float] = ..., utcLastUpdateTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOAAssetListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAAssetListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "asset")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ASSET_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    asset: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAAsset]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., asset: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAAsset, _Mapping]]] = ...) -> None: ...

class ProtoOASymbolsListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "includeArchivedSymbols")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    INCLUDEARCHIVEDSYMBOLS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    includeArchivedSymbols: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., includeArchivedSymbols: bool = ...) -> None: ...

class ProtoOASymbolsListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbol", "archivedSymbol")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    ARCHIVEDSYMBOL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbol: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOALightSymbol]
    archivedSymbol: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAArchivedSymbol]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbol: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOALightSymbol, _Mapping]]] = ..., archivedSymbol: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAArchivedSymbol, _Mapping]]] = ...) -> None: ...

class ProtoOASymbolByIdReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOASymbolByIdRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbol", "archivedSymbol")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    ARCHIVEDSYMBOL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbol: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOASymbol]
    archivedSymbol: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAArchivedSymbol]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbol: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOASymbol, _Mapping]]] = ..., archivedSymbol: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAArchivedSymbol, _Mapping]]] = ...) -> None: ...

class ProtoOASymbolsForConversionReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "firstAssetId", "lastAssetId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    FIRSTASSETID_FIELD_NUMBER: _ClassVar[int]
    LASTASSETID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    firstAssetId: int
    lastAssetId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., firstAssetId: _Optional[int] = ..., lastAssetId: _Optional[int] = ...) -> None: ...

class ProtoOASymbolsForConversionRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbol")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbol: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOALightSymbol]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbol: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOALightSymbol, _Mapping]]] = ...) -> None: ...

class ProtoOASymbolChangedEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOAAssetClassListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAAssetClassListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "assetClass")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ASSETCLASS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    assetClass: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAAssetClass]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., assetClass: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAAssetClass, _Mapping]]] = ...) -> None: ...

class ProtoOATraderReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOATraderRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "trader")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    TRADER_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    trader: _OpenApiModelMessages_pb2.ProtoOATrader
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., trader: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATrader, _Mapping]] = ...) -> None: ...

class ProtoOATraderUpdatedEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "trader")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    TRADER_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    trader: _OpenApiModelMessages_pb2.ProtoOATrader
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., trader: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATrader, _Mapping]] = ...) -> None: ...

class ProtoOAReconcileReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "returnProtectionOrders")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    RETURNPROTECTIONORDERS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    returnProtectionOrders: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., returnProtectionOrders: bool = ...) -> None: ...

class ProtoOAReconcileRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "position", "order")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    position: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAPosition]
    order: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAOrder]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., position: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAPosition, _Mapping]]] = ..., order: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAOrder, _Mapping]]] = ...) -> None: ...

class ProtoOAOrderErrorEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "errorCode", "orderId", "positionId", "description")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ERRORCODE_FIELD_NUMBER: _ClassVar[int]
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    errorCode: str
    orderId: int
    positionId: int
    description: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., errorCode: _Optional[str] = ..., orderId: _Optional[int] = ..., positionId: _Optional[int] = ..., description: _Optional[str] = ...) -> None: ...

class ProtoOADealListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "fromTimestamp", "toTimestamp", "maxRows")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MAXROWS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    fromTimestamp: int
    toTimestamp: int
    maxRows: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ..., maxRows: _Optional[int] = ...) -> None: ...

class ProtoOADealListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "deal", "hasMore")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    DEAL_FIELD_NUMBER: _ClassVar[int]
    HASMORE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    deal: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADeal]
    hasMore: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., deal: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADeal, _Mapping]]] = ..., hasMore: bool = ...) -> None: ...

class ProtoOAOrderListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "fromTimestamp", "toTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    fromTimestamp: int
    toTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOAOrderListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "order", "hasMore")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    HASMORE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    order: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAOrder]
    hasMore: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., order: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAOrder, _Mapping]]] = ..., hasMore: bool = ...) -> None: ...

class ProtoOAExpectedMarginReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId", "volume")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: int
    volume: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[int] = ..., volume: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOAExpectedMarginRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "margin", "moneyDigits")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    MARGIN_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    margin: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAExpectedMargin]
    moneyDigits: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., margin: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAExpectedMargin, _Mapping]]] = ..., moneyDigits: _Optional[int] = ...) -> None: ...

class ProtoOAMarginChangedEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionId", "usedMargin", "moneyDigits")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    USEDMARGIN_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionId: int
    usedMargin: int
    moneyDigits: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionId: _Optional[int] = ..., usedMargin: _Optional[int] = ..., moneyDigits: _Optional[int] = ...) -> None: ...

class ProtoOACashFlowHistoryListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "fromTimestamp", "toTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    fromTimestamp: int
    toTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOACashFlowHistoryListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "depositWithdraw")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    DEPOSITWITHDRAW_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    depositWithdraw: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADepositWithdraw]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., depositWithdraw: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADepositWithdraw, _Mapping]]] = ...) -> None: ...

class ProtoOAGetAccountListByAccessTokenReq(_message.Message):
    __slots__ = ("payloadType", "accessToken")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    ACCESSTOKEN_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    accessToken: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., accessToken: _Optional[str] = ...) -> None: ...

class ProtoOAGetAccountListByAccessTokenRes(_message.Message):
    __slots__ = ("payloadType", "accessToken", "permissionScope", "ctidTraderAccount")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    ACCESSTOKEN_FIELD_NUMBER: _ClassVar[int]
    PERMISSIONSCOPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNT_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    accessToken: str
    permissionScope: _OpenApiModelMessages_pb2.ProtoOAClientPermissionScope
    ctidTraderAccount: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOACtidTraderAccount]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., accessToken: _Optional[str] = ..., permissionScope: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAClientPermissionScope, str]] = ..., ctidTraderAccount: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOACtidTraderAccount, _Mapping]]] = ...) -> None: ...

class ProtoOARefreshTokenReq(_message.Message):
    __slots__ = ("payloadType", "refreshToken")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    REFRESHTOKEN_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    refreshToken: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., refreshToken: _Optional[str] = ...) -> None: ...

class ProtoOARefreshTokenRes(_message.Message):
    __slots__ = ("payloadType", "accessToken", "tokenType", "expiresIn", "refreshToken")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    ACCESSTOKEN_FIELD_NUMBER: _ClassVar[int]
    TOKENTYPE_FIELD_NUMBER: _ClassVar[int]
    EXPIRESIN_FIELD_NUMBER: _ClassVar[int]
    REFRESHTOKEN_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    accessToken: str
    tokenType: str
    expiresIn: int
    refreshToken: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., accessToken: _Optional[str] = ..., tokenType: _Optional[str] = ..., expiresIn: _Optional[int] = ..., refreshToken: _Optional[str] = ...) -> None: ...

class ProtoOASubscribeSpotsReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId", "subscribeToSpotTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIBETOSPOTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: _containers.RepeatedScalarFieldContainer[int]
    subscribeToSpotTimestamp: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[_Iterable[int]] = ..., subscribeToSpotTimestamp: bool = ...) -> None: ...

class ProtoOASubscribeSpotsRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAUnsubscribeSpotsReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOAUnsubscribeSpotsRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOASpotEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId", "bid", "ask", "trendbar", "sessionClose", "timestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    BID_FIELD_NUMBER: _ClassVar[int]
    ASK_FIELD_NUMBER: _ClassVar[int]
    TRENDBAR_FIELD_NUMBER: _ClassVar[int]
    SESSIONCLOSE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: int
    bid: int
    ask: int
    trendbar: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOATrendbar]
    sessionClose: int
    timestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[int] = ..., bid: _Optional[int] = ..., ask: _Optional[int] = ..., trendbar: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOATrendbar, _Mapping]]] = ..., sessionClose: _Optional[int] = ..., timestamp: _Optional[int] = ...) -> None: ...

class ProtoOASubscribeLiveTrendbarReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "period", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    period: _OpenApiModelMessages_pb2.ProtoOATrendbarPeriod
    symbolId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., period: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATrendbarPeriod, str]] = ..., symbolId: _Optional[int] = ...) -> None: ...

class ProtoOASubscribeLiveTrendbarRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAUnsubscribeLiveTrendbarReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "period", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    period: _OpenApiModelMessages_pb2.ProtoOATrendbarPeriod
    symbolId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., period: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATrendbarPeriod, str]] = ..., symbolId: _Optional[int] = ...) -> None: ...

class ProtoOAUnsubscribeLiveTrendbarRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAGetTrendbarsReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "fromTimestamp", "toTimestamp", "period", "symbolId", "count")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    fromTimestamp: int
    toTimestamp: int
    period: _OpenApiModelMessages_pb2.ProtoOATrendbarPeriod
    symbolId: int
    count: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ..., period: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATrendbarPeriod, str]] = ..., symbolId: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class ProtoOAGetTrendbarsRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "period", "timestamp", "trendbar", "symbolId", "hasMore")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRENDBAR_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    HASMORE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    period: _OpenApiModelMessages_pb2.ProtoOATrendbarPeriod
    timestamp: int
    trendbar: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOATrendbar]
    symbolId: int
    hasMore: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., period: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOATrendbarPeriod, str]] = ..., timestamp: _Optional[int] = ..., trendbar: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOATrendbar, _Mapping]]] = ..., symbolId: _Optional[int] = ..., hasMore: bool = ...) -> None: ...

class ProtoOAGetTickDataReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId", "type", "fromTimestamp", "toTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: int
    type: _OpenApiModelMessages_pb2.ProtoOAQuoteType
    fromTimestamp: int
    toTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[int] = ..., type: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAQuoteType, str]] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOAGetTickDataRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "tickData", "hasMore")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    TICKDATA_FIELD_NUMBER: _ClassVar[int]
    HASMORE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    tickData: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOATickData]
    hasMore: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., tickData: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOATickData, _Mapping]]] = ..., hasMore: bool = ...) -> None: ...

class ProtoOAGetCtidProfileByTokenReq(_message.Message):
    __slots__ = ("payloadType", "accessToken")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    ACCESSTOKEN_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    accessToken: str
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., accessToken: _Optional[str] = ...) -> None: ...

class ProtoOAGetCtidProfileByTokenRes(_message.Message):
    __slots__ = ("payloadType", "profile")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    profile: _OpenApiModelMessages_pb2.ProtoOACtidProfile
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., profile: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOACtidProfile, _Mapping]] = ...) -> None: ...

class ProtoOADepthEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId", "newQuotes", "deletedQuotes")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    NEWQUOTES_FIELD_NUMBER: _ClassVar[int]
    DELETEDQUOTES_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: int
    newQuotes: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADepthQuote]
    deletedQuotes: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[int] = ..., newQuotes: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADepthQuote, _Mapping]]] = ..., deletedQuotes: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOASubscribeDepthQuotesReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOASubscribeDepthQuotesRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAUnsubscribeDepthQuotesReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolId: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolId: _Optional[_Iterable[int]] = ...) -> None: ...

class ProtoOAUnsubscribeDepthQuotesRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOASymbolCategoryListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOASymbolCategoryListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "symbolCategory")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLCATEGORY_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    symbolCategory: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOASymbolCategory]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., symbolCategory: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOASymbolCategory, _Mapping]]] = ...) -> None: ...

class ProtoOAAccountLogoutReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAAccountLogoutRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAAccountDisconnectEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAMarginCallListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAMarginCallListRes(_message.Message):
    __slots__ = ("payloadType", "marginCall")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    MARGINCALL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    marginCall: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAMarginCall]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., marginCall: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAMarginCall, _Mapping]]] = ...) -> None: ...

class ProtoOAMarginCallUpdateReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "marginCall")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    MARGINCALL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    marginCall: _OpenApiModelMessages_pb2.ProtoOAMarginCall
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., marginCall: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAMarginCall, _Mapping]] = ...) -> None: ...

class ProtoOAMarginCallUpdateRes(_message.Message):
    __slots__ = ("payloadType",)
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ...) -> None: ...

class ProtoOAMarginCallUpdateEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "marginCall")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    MARGINCALL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    marginCall: _OpenApiModelMessages_pb2.ProtoOAMarginCall
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., marginCall: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAMarginCall, _Mapping]] = ...) -> None: ...

class ProtoOAMarginCallTriggerEvent(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "marginCall")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    MARGINCALL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    marginCall: _OpenApiModelMessages_pb2.ProtoOAMarginCall
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., marginCall: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAMarginCall, _Mapping]] = ...) -> None: ...

class ProtoOAGetDynamicLeverageByIDReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "leverageId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    LEVERAGEID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    leverageId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., leverageId: _Optional[int] = ...) -> None: ...

class ProtoOAGetDynamicLeverageByIDRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "leverage")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    LEVERAGE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    leverage: _OpenApiModelMessages_pb2.ProtoOADynamicLeverage
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., leverage: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOADynamicLeverage, _Mapping]] = ...) -> None: ...

class ProtoOADealListByPositionIdReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionId", "fromTimestamp", "toTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionId: int
    fromTimestamp: int
    toTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionId: _Optional[int] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOADealListByPositionIdRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "deal", "hasMore")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    DEAL_FIELD_NUMBER: _ClassVar[int]
    HASMORE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    deal: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADeal]
    hasMore: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., deal: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADeal, _Mapping]]] = ..., hasMore: bool = ...) -> None: ...

class ProtoOAOrderDetailsReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "orderId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    orderId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., orderId: _Optional[int] = ...) -> None: ...

class ProtoOAOrderDetailsRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "order", "deal")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    DEAL_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    order: _OpenApiModelMessages_pb2.ProtoOAOrder
    deal: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADeal]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., order: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAOrder, _Mapping]] = ..., deal: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADeal, _Mapping]]] = ...) -> None: ...

class ProtoOAOrderListByPositionIdReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionId", "fromTimestamp", "toTimestamp")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    FROMTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TOTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionId: int
    fromTimestamp: int
    toTimestamp: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionId: _Optional[int] = ..., fromTimestamp: _Optional[int] = ..., toTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOAOrderListByPositionIdRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "order", "hasMore")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    HASMORE_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    order: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAOrder]
    hasMore: bool
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., order: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAOrder, _Mapping]]] = ..., hasMore: bool = ...) -> None: ...

class ProtoOADealOffsetListReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "dealId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    DEALID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    dealId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., dealId: _Optional[int] = ...) -> None: ...

class ProtoOADealOffsetListRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "offsetBy", "offsetting")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    OFFSETBY_FIELD_NUMBER: _ClassVar[int]
    OFFSETTING_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    offsetBy: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADealOffset]
    offsetting: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOADealOffset]
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., offsetBy: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADealOffset, _Mapping]]] = ..., offsetting: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOADealOffset, _Mapping]]] = ...) -> None: ...

class ProtoOAGetPositionUnrealizedPnLReq(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ...) -> None: ...

class ProtoOAGetPositionUnrealizedPnLRes(_message.Message):
    __slots__ = ("payloadType", "ctidTraderAccountId", "positionUnrealizedPnL", "moneyDigits")
    PAYLOADTYPE_FIELD_NUMBER: _ClassVar[int]
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    POSITIONUNREALIZEDPNL_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    payloadType: _OpenApiModelMessages_pb2.ProtoOAPayloadType
    ctidTraderAccountId: int
    positionUnrealizedPnL: _containers.RepeatedCompositeFieldContainer[_OpenApiModelMessages_pb2.ProtoOAPositionUnrealizedPnL]
    moneyDigits: int
    def __init__(self, payloadType: _Optional[_Union[_OpenApiModelMessages_pb2.ProtoOAPayloadType, str]] = ..., ctidTraderAccountId: _Optional[int] = ..., positionUnrealizedPnL: _Optional[_Iterable[_Union[_OpenApiModelMessages_pb2.ProtoOAPositionUnrealizedPnL, _Mapping]]] = ..., moneyDigits: _Optional[int] = ...) -> None: ...
