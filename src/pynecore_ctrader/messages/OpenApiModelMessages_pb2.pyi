from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProtoOAPayloadType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROTO_OA_APPLICATION_AUTH_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_APPLICATION_AUTH_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ACCOUNT_AUTH_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ACCOUNT_AUTH_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_VERSION_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_VERSION_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_NEW_ORDER_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_TRAILING_SL_CHANGED_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_CANCEL_ORDER_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_AMEND_ORDER_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_AMEND_POSITION_SLTP_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_CLOSE_POSITION_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ASSET_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ASSET_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOLS_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOLS_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOL_BY_ID_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOL_BY_ID_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOLS_FOR_CONVERSION_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOL_CHANGED_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_TRADER_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_TRADER_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_TRADER_UPDATE_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_RECONCILE_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_RECONCILE_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_EXECUTION_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SUBSCRIBE_SPOTS_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SUBSCRIBE_SPOTS_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_UNSUBSCRIBE_SPOTS_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_UNSUBSCRIBE_SPOTS_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SPOT_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_ERROR_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEAL_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEAL_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_TRENDBARS_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_TRENDBARS_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_EXPECTED_MARGIN_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_EXPECTED_MARGIN_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CHANGED_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ERROR_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_CASH_FLOW_HISTORY_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_TICKDATA_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_TICKDATA_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_CLIENT_DISCONNECT_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ASSET_CLASS_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ASSET_CLASS_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEPTH_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOL_CATEGORY_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SYMBOL_CATEGORY_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ACCOUNT_LOGOUT_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ACCOUNT_LOGOUT_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ACCOUNT_DISCONNECT_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CALL_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CALL_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CALL_UPDATE_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CALL_UPDATE_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CALL_UPDATE_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_MARGIN_CALL_TRIGGER_EVENT: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_REFRESH_TOKEN_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_REFRESH_TOKEN_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_DYNAMIC_LEVERAGE_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_DYNAMIC_LEVERAGE_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEAL_LIST_BY_POSITION_ID_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_DETAILS_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_DETAILS_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_ORDER_LIST_BY_POSITION_ID_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEAL_OFFSET_LIST_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_DEAL_OFFSET_LIST_RES: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_POSITION_UNREALIZED_PNL_REQ: _ClassVar[ProtoOAPayloadType]
    PROTO_OA_GET_POSITION_UNREALIZED_PNL_RES: _ClassVar[ProtoOAPayloadType]

class ProtoOADayOfWeek(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NONE: _ClassVar[ProtoOADayOfWeek]
    MONDAY: _ClassVar[ProtoOADayOfWeek]
    TUESDAY: _ClassVar[ProtoOADayOfWeek]
    WEDNESDAY: _ClassVar[ProtoOADayOfWeek]
    THURSDAY: _ClassVar[ProtoOADayOfWeek]
    FRIDAY: _ClassVar[ProtoOADayOfWeek]
    SATURDAY: _ClassVar[ProtoOADayOfWeek]
    SUNDAY: _ClassVar[ProtoOADayOfWeek]

class ProtoOACommissionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    USD_PER_MILLION_USD: _ClassVar[ProtoOACommissionType]
    USD_PER_LOT: _ClassVar[ProtoOACommissionType]
    PERCENTAGE_OF_VALUE: _ClassVar[ProtoOACommissionType]
    QUOTE_CCY_PER_LOT: _ClassVar[ProtoOACommissionType]

class ProtoOASymbolDistanceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SYMBOL_DISTANCE_IN_POINTS: _ClassVar[ProtoOASymbolDistanceType]
    SYMBOL_DISTANCE_IN_PERCENTAGE: _ClassVar[ProtoOASymbolDistanceType]

class ProtoOAMinCommissionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CURRENCY: _ClassVar[ProtoOAMinCommissionType]
    QUOTE_CURRENCY: _ClassVar[ProtoOAMinCommissionType]

class ProtoOATradingMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENABLED: _ClassVar[ProtoOATradingMode]
    DISABLED_WITHOUT_PENDINGS_EXECUTION: _ClassVar[ProtoOATradingMode]
    DISABLED_WITH_PENDINGS_EXECUTION: _ClassVar[ProtoOATradingMode]
    CLOSE_ONLY_MODE: _ClassVar[ProtoOATradingMode]

class ProtoOASwapCalculationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PIPS: _ClassVar[ProtoOASwapCalculationType]
    PERCENTAGE: _ClassVar[ProtoOASwapCalculationType]
    POINTS: _ClassVar[ProtoOASwapCalculationType]

class ProtoOAAccessRights(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FULL_ACCESS: _ClassVar[ProtoOAAccessRights]
    CLOSE_ONLY: _ClassVar[ProtoOAAccessRights]
    NO_TRADING: _ClassVar[ProtoOAAccessRights]
    NO_LOGIN: _ClassVar[ProtoOAAccessRights]

class ProtoOATotalMarginCalculationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MAX: _ClassVar[ProtoOATotalMarginCalculationType]
    SUM: _ClassVar[ProtoOATotalMarginCalculationType]
    NET: _ClassVar[ProtoOATotalMarginCalculationType]

class ProtoOAAccountType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HEDGED: _ClassVar[ProtoOAAccountType]
    NETTED: _ClassVar[ProtoOAAccountType]
    SPREAD_BETTING: _ClassVar[ProtoOAAccountType]

class ProtoOAPositionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    POSITION_STATUS_OPEN: _ClassVar[ProtoOAPositionStatus]
    POSITION_STATUS_CLOSED: _ClassVar[ProtoOAPositionStatus]
    POSITION_STATUS_CREATED: _ClassVar[ProtoOAPositionStatus]
    POSITION_STATUS_ERROR: _ClassVar[ProtoOAPositionStatus]

class ProtoOATradeSide(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BUY: _ClassVar[ProtoOATradeSide]
    SELL: _ClassVar[ProtoOATradeSide]

class ProtoOAOrderType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MARKET: _ClassVar[ProtoOAOrderType]
    LIMIT: _ClassVar[ProtoOAOrderType]
    STOP: _ClassVar[ProtoOAOrderType]
    STOP_LOSS_TAKE_PROFIT: _ClassVar[ProtoOAOrderType]
    MARKET_RANGE: _ClassVar[ProtoOAOrderType]
    STOP_LIMIT: _ClassVar[ProtoOAOrderType]

class ProtoOATimeInForce(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GOOD_TILL_DATE: _ClassVar[ProtoOATimeInForce]
    GOOD_TILL_CANCEL: _ClassVar[ProtoOATimeInForce]
    IMMEDIATE_OR_CANCEL: _ClassVar[ProtoOATimeInForce]
    FILL_OR_KILL: _ClassVar[ProtoOATimeInForce]
    MARKET_ON_OPEN: _ClassVar[ProtoOATimeInForce]

class ProtoOAOrderStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORDER_STATUS_ACCEPTED: _ClassVar[ProtoOAOrderStatus]
    ORDER_STATUS_FILLED: _ClassVar[ProtoOAOrderStatus]
    ORDER_STATUS_REJECTED: _ClassVar[ProtoOAOrderStatus]
    ORDER_STATUS_EXPIRED: _ClassVar[ProtoOAOrderStatus]
    ORDER_STATUS_CANCELLED: _ClassVar[ProtoOAOrderStatus]

class ProtoOAOrderTriggerMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRADE: _ClassVar[ProtoOAOrderTriggerMethod]
    OPPOSITE: _ClassVar[ProtoOAOrderTriggerMethod]
    DOUBLE_TRADE: _ClassVar[ProtoOAOrderTriggerMethod]
    DOUBLE_OPPOSITE: _ClassVar[ProtoOAOrderTriggerMethod]

class ProtoOAExecutionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORDER_ACCEPTED: _ClassVar[ProtoOAExecutionType]
    ORDER_FILLED: _ClassVar[ProtoOAExecutionType]
    ORDER_REPLACED: _ClassVar[ProtoOAExecutionType]
    ORDER_CANCELLED: _ClassVar[ProtoOAExecutionType]
    ORDER_EXPIRED: _ClassVar[ProtoOAExecutionType]
    ORDER_REJECTED: _ClassVar[ProtoOAExecutionType]
    ORDER_CANCEL_REJECTED: _ClassVar[ProtoOAExecutionType]
    SWAP: _ClassVar[ProtoOAExecutionType]
    DEPOSIT_WITHDRAW: _ClassVar[ProtoOAExecutionType]
    ORDER_PARTIAL_FILL: _ClassVar[ProtoOAExecutionType]
    BONUS_DEPOSIT_WITHDRAW: _ClassVar[ProtoOAExecutionType]

class ProtoOAChangeBonusType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BONUS_DEPOSIT: _ClassVar[ProtoOAChangeBonusType]
    BONUS_WITHDRAW: _ClassVar[ProtoOAChangeBonusType]

class ProtoOAChangeBalanceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BALANCE_DEPOSIT: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_STRATEGY_COMMISSION_INNER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_STRATEGY_COMMISSION_INNER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_IB_COMMISSIONS: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_IB_SHARED_PERCENTAGE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_IB_SHARED_PERCENTAGE_FROM_SUB_IB: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_IB_SHARED_PERCENTAGE_FROM_BROKER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_REBATE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_REBATE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_STRATEGY_COMMISSION_OUTER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_STRATEGY_COMMISSION_OUTER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_BONUS_COMPENSATION: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_IB_SHARED_PERCENTAGE_TO_BROKER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_DIVIDENDS: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_DIVIDENDS: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_GSL_CHARGE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_ROLLOVER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_NONWITHDRAWABLE_BONUS: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_NONWITHDRAWABLE_BONUS: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_SWAP: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_SWAP: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_MANAGEMENT_FEE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_MANAGEMENT_FEE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_PERFORMANCE_FEE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_FOR_SUBACCOUNT: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_TO_SUBACCOUNT: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_FROM_SUBACCOUNT: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_FROM_SUBACCOUNT: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_COPY_FEE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_INACTIVITY_FEE: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_TRANSFER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_WITHDRAW_TRANSFER: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_CONVERTED_BONUS: _ClassVar[ProtoOAChangeBalanceType]
    BALANCE_DEPOSIT_NEGATIVE_BALANCE_PROTECTION: _ClassVar[ProtoOAChangeBalanceType]

class ProtoOADealStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FILLED: _ClassVar[ProtoOADealStatus]
    PARTIALLY_FILLED: _ClassVar[ProtoOADealStatus]
    REJECTED: _ClassVar[ProtoOADealStatus]
    INTERNALLY_REJECTED: _ClassVar[ProtoOADealStatus]
    ERROR: _ClassVar[ProtoOADealStatus]
    MISSED: _ClassVar[ProtoOADealStatus]

class ProtoOATrendbarPeriod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    M1: _ClassVar[ProtoOATrendbarPeriod]
    M2: _ClassVar[ProtoOATrendbarPeriod]
    M3: _ClassVar[ProtoOATrendbarPeriod]
    M4: _ClassVar[ProtoOATrendbarPeriod]
    M5: _ClassVar[ProtoOATrendbarPeriod]
    M10: _ClassVar[ProtoOATrendbarPeriod]
    M15: _ClassVar[ProtoOATrendbarPeriod]
    M30: _ClassVar[ProtoOATrendbarPeriod]
    H1: _ClassVar[ProtoOATrendbarPeriod]
    H4: _ClassVar[ProtoOATrendbarPeriod]
    H12: _ClassVar[ProtoOATrendbarPeriod]
    D1: _ClassVar[ProtoOATrendbarPeriod]
    W1: _ClassVar[ProtoOATrendbarPeriod]
    MN1: _ClassVar[ProtoOATrendbarPeriod]

class ProtoOAQuoteType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BID: _ClassVar[ProtoOAQuoteType]
    ASK: _ClassVar[ProtoOAQuoteType]

class ProtoOAClientPermissionScope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCOPE_VIEW: _ClassVar[ProtoOAClientPermissionScope]
    SCOPE_TRADE: _ClassVar[ProtoOAClientPermissionScope]

class ProtoOANotificationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MARGIN_LEVEL_THRESHOLD_1: _ClassVar[ProtoOANotificationType]
    MARGIN_LEVEL_THRESHOLD_2: _ClassVar[ProtoOANotificationType]
    MARGIN_LEVEL_THRESHOLD_3: _ClassVar[ProtoOANotificationType]

class ProtoOAErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OA_AUTH_TOKEN_EXPIRED: _ClassVar[ProtoOAErrorCode]
    ACCOUNT_NOT_AUTHORIZED: _ClassVar[ProtoOAErrorCode]
    RET_NO_SUCH_LOGIN: _ClassVar[ProtoOAErrorCode]
    ALREADY_LOGGED_IN: _ClassVar[ProtoOAErrorCode]
    RET_ACCOUNT_DISABLED: _ClassVar[ProtoOAErrorCode]
    CH_CLIENT_AUTH_FAILURE: _ClassVar[ProtoOAErrorCode]
    CH_CLIENT_NOT_AUTHENTICATED: _ClassVar[ProtoOAErrorCode]
    CH_CLIENT_ALREADY_AUTHENTICATED: _ClassVar[ProtoOAErrorCode]
    CH_ACCESS_TOKEN_INVALID: _ClassVar[ProtoOAErrorCode]
    CH_SERVER_NOT_REACHABLE: _ClassVar[ProtoOAErrorCode]
    CH_CTID_TRADER_ACCOUNT_NOT_FOUND: _ClassVar[ProtoOAErrorCode]
    CH_OA_CLIENT_NOT_FOUND: _ClassVar[ProtoOAErrorCode]
    REQUEST_FREQUENCY_EXCEEDED: _ClassVar[ProtoOAErrorCode]
    SERVER_IS_UNDER_MAINTENANCE: _ClassVar[ProtoOAErrorCode]
    CHANNEL_IS_BLOCKED: _ClassVar[ProtoOAErrorCode]
    CONNECTIONS_LIMIT_EXCEEDED: _ClassVar[ProtoOAErrorCode]
    WORSE_GSL_NOT_ALLOWED: _ClassVar[ProtoOAErrorCode]
    SYMBOL_HAS_HOLIDAY: _ClassVar[ProtoOAErrorCode]
    NOT_SUBSCRIBED_TO_SPOTS: _ClassVar[ProtoOAErrorCode]
    ALREADY_SUBSCRIBED: _ClassVar[ProtoOAErrorCode]
    SYMBOL_NOT_FOUND: _ClassVar[ProtoOAErrorCode]
    UNKNOWN_SYMBOL: _ClassVar[ProtoOAErrorCode]
    INCORRECT_BOUNDARIES: _ClassVar[ProtoOAErrorCode]
    NO_QUOTES: _ClassVar[ProtoOAErrorCode]
    NOT_ENOUGH_MONEY: _ClassVar[ProtoOAErrorCode]
    MAX_EXPOSURE_REACHED: _ClassVar[ProtoOAErrorCode]
    POSITION_NOT_FOUND: _ClassVar[ProtoOAErrorCode]
    ORDER_NOT_FOUND: _ClassVar[ProtoOAErrorCode]
    POSITION_NOT_OPEN: _ClassVar[ProtoOAErrorCode]
    POSITION_LOCKED: _ClassVar[ProtoOAErrorCode]
    TOO_MANY_POSITIONS: _ClassVar[ProtoOAErrorCode]
    TRADING_BAD_VOLUME: _ClassVar[ProtoOAErrorCode]
    TRADING_BAD_STOPS: _ClassVar[ProtoOAErrorCode]
    TRADING_BAD_PRICES: _ClassVar[ProtoOAErrorCode]
    TRADING_BAD_STAKE: _ClassVar[ProtoOAErrorCode]
    PROTECTION_IS_TOO_CLOSE_TO_MARKET: _ClassVar[ProtoOAErrorCode]
    TRADING_BAD_EXPIRATION_DATE: _ClassVar[ProtoOAErrorCode]
    PENDING_EXECUTION: _ClassVar[ProtoOAErrorCode]
    TRADING_DISABLED: _ClassVar[ProtoOAErrorCode]
    TRADING_NOT_ALLOWED: _ClassVar[ProtoOAErrorCode]
    UNABLE_TO_CANCEL_ORDER: _ClassVar[ProtoOAErrorCode]
    UNABLE_TO_AMEND_ORDER: _ClassVar[ProtoOAErrorCode]
    SHORT_SELLING_NOT_ALLOWED: _ClassVar[ProtoOAErrorCode]

class ProtoOALimitedRiskMarginCalculationStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCORDING_TO_LEVERAGE: _ClassVar[ProtoOALimitedRiskMarginCalculationStrategy]
    ACCORDING_TO_GSL: _ClassVar[ProtoOALimitedRiskMarginCalculationStrategy]
    ACCORDING_TO_GSL_AND_LEVERAGE: _ClassVar[ProtoOALimitedRiskMarginCalculationStrategy]

class ProtoOAStopOutStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MOST_MARGIN_USED_FIRST: _ClassVar[ProtoOAStopOutStrategy]
    MOST_LOSING_FIRST: _ClassVar[ProtoOAStopOutStrategy]
PROTO_OA_APPLICATION_AUTH_REQ: ProtoOAPayloadType
PROTO_OA_APPLICATION_AUTH_RES: ProtoOAPayloadType
PROTO_OA_ACCOUNT_AUTH_REQ: ProtoOAPayloadType
PROTO_OA_ACCOUNT_AUTH_RES: ProtoOAPayloadType
PROTO_OA_VERSION_REQ: ProtoOAPayloadType
PROTO_OA_VERSION_RES: ProtoOAPayloadType
PROTO_OA_NEW_ORDER_REQ: ProtoOAPayloadType
PROTO_OA_TRAILING_SL_CHANGED_EVENT: ProtoOAPayloadType
PROTO_OA_CANCEL_ORDER_REQ: ProtoOAPayloadType
PROTO_OA_AMEND_ORDER_REQ: ProtoOAPayloadType
PROTO_OA_AMEND_POSITION_SLTP_REQ: ProtoOAPayloadType
PROTO_OA_CLOSE_POSITION_REQ: ProtoOAPayloadType
PROTO_OA_ASSET_LIST_REQ: ProtoOAPayloadType
PROTO_OA_ASSET_LIST_RES: ProtoOAPayloadType
PROTO_OA_SYMBOLS_LIST_REQ: ProtoOAPayloadType
PROTO_OA_SYMBOLS_LIST_RES: ProtoOAPayloadType
PROTO_OA_SYMBOL_BY_ID_REQ: ProtoOAPayloadType
PROTO_OA_SYMBOL_BY_ID_RES: ProtoOAPayloadType
PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ: ProtoOAPayloadType
PROTO_OA_SYMBOLS_FOR_CONVERSION_RES: ProtoOAPayloadType
PROTO_OA_SYMBOL_CHANGED_EVENT: ProtoOAPayloadType
PROTO_OA_TRADER_REQ: ProtoOAPayloadType
PROTO_OA_TRADER_RES: ProtoOAPayloadType
PROTO_OA_TRADER_UPDATE_EVENT: ProtoOAPayloadType
PROTO_OA_RECONCILE_REQ: ProtoOAPayloadType
PROTO_OA_RECONCILE_RES: ProtoOAPayloadType
PROTO_OA_EXECUTION_EVENT: ProtoOAPayloadType
PROTO_OA_SUBSCRIBE_SPOTS_REQ: ProtoOAPayloadType
PROTO_OA_SUBSCRIBE_SPOTS_RES: ProtoOAPayloadType
PROTO_OA_UNSUBSCRIBE_SPOTS_REQ: ProtoOAPayloadType
PROTO_OA_UNSUBSCRIBE_SPOTS_RES: ProtoOAPayloadType
PROTO_OA_SPOT_EVENT: ProtoOAPayloadType
PROTO_OA_ORDER_ERROR_EVENT: ProtoOAPayloadType
PROTO_OA_DEAL_LIST_REQ: ProtoOAPayloadType
PROTO_OA_DEAL_LIST_RES: ProtoOAPayloadType
PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ: ProtoOAPayloadType
PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_REQ: ProtoOAPayloadType
PROTO_OA_GET_TRENDBARS_REQ: ProtoOAPayloadType
PROTO_OA_GET_TRENDBARS_RES: ProtoOAPayloadType
PROTO_OA_EXPECTED_MARGIN_REQ: ProtoOAPayloadType
PROTO_OA_EXPECTED_MARGIN_RES: ProtoOAPayloadType
PROTO_OA_MARGIN_CHANGED_EVENT: ProtoOAPayloadType
PROTO_OA_ERROR_RES: ProtoOAPayloadType
PROTO_OA_CASH_FLOW_HISTORY_LIST_REQ: ProtoOAPayloadType
PROTO_OA_CASH_FLOW_HISTORY_LIST_RES: ProtoOAPayloadType
PROTO_OA_GET_TICKDATA_REQ: ProtoOAPayloadType
PROTO_OA_GET_TICKDATA_RES: ProtoOAPayloadType
PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT: ProtoOAPayloadType
PROTO_OA_CLIENT_DISCONNECT_EVENT: ProtoOAPayloadType
PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ: ProtoOAPayloadType
PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES: ProtoOAPayloadType
PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_REQ: ProtoOAPayloadType
PROTO_OA_GET_CTID_PROFILE_BY_TOKEN_RES: ProtoOAPayloadType
PROTO_OA_ASSET_CLASS_LIST_REQ: ProtoOAPayloadType
PROTO_OA_ASSET_CLASS_LIST_RES: ProtoOAPayloadType
PROTO_OA_DEPTH_EVENT: ProtoOAPayloadType
PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_REQ: ProtoOAPayloadType
PROTO_OA_SUBSCRIBE_DEPTH_QUOTES_RES: ProtoOAPayloadType
PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_REQ: ProtoOAPayloadType
PROTO_OA_UNSUBSCRIBE_DEPTH_QUOTES_RES: ProtoOAPayloadType
PROTO_OA_SYMBOL_CATEGORY_REQ: ProtoOAPayloadType
PROTO_OA_SYMBOL_CATEGORY_RES: ProtoOAPayloadType
PROTO_OA_ACCOUNT_LOGOUT_REQ: ProtoOAPayloadType
PROTO_OA_ACCOUNT_LOGOUT_RES: ProtoOAPayloadType
PROTO_OA_ACCOUNT_DISCONNECT_EVENT: ProtoOAPayloadType
PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES: ProtoOAPayloadType
PROTO_OA_UNSUBSCRIBE_LIVE_TRENDBAR_RES: ProtoOAPayloadType
PROTO_OA_MARGIN_CALL_LIST_REQ: ProtoOAPayloadType
PROTO_OA_MARGIN_CALL_LIST_RES: ProtoOAPayloadType
PROTO_OA_MARGIN_CALL_UPDATE_REQ: ProtoOAPayloadType
PROTO_OA_MARGIN_CALL_UPDATE_RES: ProtoOAPayloadType
PROTO_OA_MARGIN_CALL_UPDATE_EVENT: ProtoOAPayloadType
PROTO_OA_MARGIN_CALL_TRIGGER_EVENT: ProtoOAPayloadType
PROTO_OA_REFRESH_TOKEN_REQ: ProtoOAPayloadType
PROTO_OA_REFRESH_TOKEN_RES: ProtoOAPayloadType
PROTO_OA_ORDER_LIST_REQ: ProtoOAPayloadType
PROTO_OA_ORDER_LIST_RES: ProtoOAPayloadType
PROTO_OA_GET_DYNAMIC_LEVERAGE_REQ: ProtoOAPayloadType
PROTO_OA_GET_DYNAMIC_LEVERAGE_RES: ProtoOAPayloadType
PROTO_OA_DEAL_LIST_BY_POSITION_ID_REQ: ProtoOAPayloadType
PROTO_OA_DEAL_LIST_BY_POSITION_ID_RES: ProtoOAPayloadType
PROTO_OA_ORDER_DETAILS_REQ: ProtoOAPayloadType
PROTO_OA_ORDER_DETAILS_RES: ProtoOAPayloadType
PROTO_OA_ORDER_LIST_BY_POSITION_ID_REQ: ProtoOAPayloadType
PROTO_OA_ORDER_LIST_BY_POSITION_ID_RES: ProtoOAPayloadType
PROTO_OA_DEAL_OFFSET_LIST_REQ: ProtoOAPayloadType
PROTO_OA_DEAL_OFFSET_LIST_RES: ProtoOAPayloadType
PROTO_OA_GET_POSITION_UNREALIZED_PNL_REQ: ProtoOAPayloadType
PROTO_OA_GET_POSITION_UNREALIZED_PNL_RES: ProtoOAPayloadType
NONE: ProtoOADayOfWeek
MONDAY: ProtoOADayOfWeek
TUESDAY: ProtoOADayOfWeek
WEDNESDAY: ProtoOADayOfWeek
THURSDAY: ProtoOADayOfWeek
FRIDAY: ProtoOADayOfWeek
SATURDAY: ProtoOADayOfWeek
SUNDAY: ProtoOADayOfWeek
USD_PER_MILLION_USD: ProtoOACommissionType
USD_PER_LOT: ProtoOACommissionType
PERCENTAGE_OF_VALUE: ProtoOACommissionType
QUOTE_CCY_PER_LOT: ProtoOACommissionType
SYMBOL_DISTANCE_IN_POINTS: ProtoOASymbolDistanceType
SYMBOL_DISTANCE_IN_PERCENTAGE: ProtoOASymbolDistanceType
CURRENCY: ProtoOAMinCommissionType
QUOTE_CURRENCY: ProtoOAMinCommissionType
ENABLED: ProtoOATradingMode
DISABLED_WITHOUT_PENDINGS_EXECUTION: ProtoOATradingMode
DISABLED_WITH_PENDINGS_EXECUTION: ProtoOATradingMode
CLOSE_ONLY_MODE: ProtoOATradingMode
PIPS: ProtoOASwapCalculationType
PERCENTAGE: ProtoOASwapCalculationType
POINTS: ProtoOASwapCalculationType
FULL_ACCESS: ProtoOAAccessRights
CLOSE_ONLY: ProtoOAAccessRights
NO_TRADING: ProtoOAAccessRights
NO_LOGIN: ProtoOAAccessRights
MAX: ProtoOATotalMarginCalculationType
SUM: ProtoOATotalMarginCalculationType
NET: ProtoOATotalMarginCalculationType
HEDGED: ProtoOAAccountType
NETTED: ProtoOAAccountType
SPREAD_BETTING: ProtoOAAccountType
POSITION_STATUS_OPEN: ProtoOAPositionStatus
POSITION_STATUS_CLOSED: ProtoOAPositionStatus
POSITION_STATUS_CREATED: ProtoOAPositionStatus
POSITION_STATUS_ERROR: ProtoOAPositionStatus
BUY: ProtoOATradeSide
SELL: ProtoOATradeSide
MARKET: ProtoOAOrderType
LIMIT: ProtoOAOrderType
STOP: ProtoOAOrderType
STOP_LOSS_TAKE_PROFIT: ProtoOAOrderType
MARKET_RANGE: ProtoOAOrderType
STOP_LIMIT: ProtoOAOrderType
GOOD_TILL_DATE: ProtoOATimeInForce
GOOD_TILL_CANCEL: ProtoOATimeInForce
IMMEDIATE_OR_CANCEL: ProtoOATimeInForce
FILL_OR_KILL: ProtoOATimeInForce
MARKET_ON_OPEN: ProtoOATimeInForce
ORDER_STATUS_ACCEPTED: ProtoOAOrderStatus
ORDER_STATUS_FILLED: ProtoOAOrderStatus
ORDER_STATUS_REJECTED: ProtoOAOrderStatus
ORDER_STATUS_EXPIRED: ProtoOAOrderStatus
ORDER_STATUS_CANCELLED: ProtoOAOrderStatus
TRADE: ProtoOAOrderTriggerMethod
OPPOSITE: ProtoOAOrderTriggerMethod
DOUBLE_TRADE: ProtoOAOrderTriggerMethod
DOUBLE_OPPOSITE: ProtoOAOrderTriggerMethod
ORDER_ACCEPTED: ProtoOAExecutionType
ORDER_FILLED: ProtoOAExecutionType
ORDER_REPLACED: ProtoOAExecutionType
ORDER_CANCELLED: ProtoOAExecutionType
ORDER_EXPIRED: ProtoOAExecutionType
ORDER_REJECTED: ProtoOAExecutionType
ORDER_CANCEL_REJECTED: ProtoOAExecutionType
SWAP: ProtoOAExecutionType
DEPOSIT_WITHDRAW: ProtoOAExecutionType
ORDER_PARTIAL_FILL: ProtoOAExecutionType
BONUS_DEPOSIT_WITHDRAW: ProtoOAExecutionType
BONUS_DEPOSIT: ProtoOAChangeBonusType
BONUS_WITHDRAW: ProtoOAChangeBonusType
BALANCE_DEPOSIT: ProtoOAChangeBalanceType
BALANCE_WITHDRAW: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_STRATEGY_COMMISSION_INNER: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_STRATEGY_COMMISSION_INNER: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_IB_COMMISSIONS: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_IB_SHARED_PERCENTAGE: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_IB_SHARED_PERCENTAGE_FROM_SUB_IB: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_IB_SHARED_PERCENTAGE_FROM_BROKER: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_REBATE: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_REBATE: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_STRATEGY_COMMISSION_OUTER: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_STRATEGY_COMMISSION_OUTER: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_BONUS_COMPENSATION: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_IB_SHARED_PERCENTAGE_TO_BROKER: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_DIVIDENDS: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_DIVIDENDS: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_GSL_CHARGE: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_ROLLOVER: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_NONWITHDRAWABLE_BONUS: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_NONWITHDRAWABLE_BONUS: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_SWAP: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_SWAP: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_MANAGEMENT_FEE: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_MANAGEMENT_FEE: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_PERFORMANCE_FEE: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_FOR_SUBACCOUNT: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_TO_SUBACCOUNT: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_FROM_SUBACCOUNT: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_FROM_SUBACCOUNT: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_COPY_FEE: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_INACTIVITY_FEE: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_TRANSFER: ProtoOAChangeBalanceType
BALANCE_WITHDRAW_TRANSFER: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_CONVERTED_BONUS: ProtoOAChangeBalanceType
BALANCE_DEPOSIT_NEGATIVE_BALANCE_PROTECTION: ProtoOAChangeBalanceType
FILLED: ProtoOADealStatus
PARTIALLY_FILLED: ProtoOADealStatus
REJECTED: ProtoOADealStatus
INTERNALLY_REJECTED: ProtoOADealStatus
ERROR: ProtoOADealStatus
MISSED: ProtoOADealStatus
M1: ProtoOATrendbarPeriod
M2: ProtoOATrendbarPeriod
M3: ProtoOATrendbarPeriod
M4: ProtoOATrendbarPeriod
M5: ProtoOATrendbarPeriod
M10: ProtoOATrendbarPeriod
M15: ProtoOATrendbarPeriod
M30: ProtoOATrendbarPeriod
H1: ProtoOATrendbarPeriod
H4: ProtoOATrendbarPeriod
H12: ProtoOATrendbarPeriod
D1: ProtoOATrendbarPeriod
W1: ProtoOATrendbarPeriod
MN1: ProtoOATrendbarPeriod
BID: ProtoOAQuoteType
ASK: ProtoOAQuoteType
SCOPE_VIEW: ProtoOAClientPermissionScope
SCOPE_TRADE: ProtoOAClientPermissionScope
MARGIN_LEVEL_THRESHOLD_1: ProtoOANotificationType
MARGIN_LEVEL_THRESHOLD_2: ProtoOANotificationType
MARGIN_LEVEL_THRESHOLD_3: ProtoOANotificationType
OA_AUTH_TOKEN_EXPIRED: ProtoOAErrorCode
ACCOUNT_NOT_AUTHORIZED: ProtoOAErrorCode
RET_NO_SUCH_LOGIN: ProtoOAErrorCode
ALREADY_LOGGED_IN: ProtoOAErrorCode
RET_ACCOUNT_DISABLED: ProtoOAErrorCode
CH_CLIENT_AUTH_FAILURE: ProtoOAErrorCode
CH_CLIENT_NOT_AUTHENTICATED: ProtoOAErrorCode
CH_CLIENT_ALREADY_AUTHENTICATED: ProtoOAErrorCode
CH_ACCESS_TOKEN_INVALID: ProtoOAErrorCode
CH_SERVER_NOT_REACHABLE: ProtoOAErrorCode
CH_CTID_TRADER_ACCOUNT_NOT_FOUND: ProtoOAErrorCode
CH_OA_CLIENT_NOT_FOUND: ProtoOAErrorCode
REQUEST_FREQUENCY_EXCEEDED: ProtoOAErrorCode
SERVER_IS_UNDER_MAINTENANCE: ProtoOAErrorCode
CHANNEL_IS_BLOCKED: ProtoOAErrorCode
CONNECTIONS_LIMIT_EXCEEDED: ProtoOAErrorCode
WORSE_GSL_NOT_ALLOWED: ProtoOAErrorCode
SYMBOL_HAS_HOLIDAY: ProtoOAErrorCode
NOT_SUBSCRIBED_TO_SPOTS: ProtoOAErrorCode
ALREADY_SUBSCRIBED: ProtoOAErrorCode
SYMBOL_NOT_FOUND: ProtoOAErrorCode
UNKNOWN_SYMBOL: ProtoOAErrorCode
INCORRECT_BOUNDARIES: ProtoOAErrorCode
NO_QUOTES: ProtoOAErrorCode
NOT_ENOUGH_MONEY: ProtoOAErrorCode
MAX_EXPOSURE_REACHED: ProtoOAErrorCode
POSITION_NOT_FOUND: ProtoOAErrorCode
ORDER_NOT_FOUND: ProtoOAErrorCode
POSITION_NOT_OPEN: ProtoOAErrorCode
POSITION_LOCKED: ProtoOAErrorCode
TOO_MANY_POSITIONS: ProtoOAErrorCode
TRADING_BAD_VOLUME: ProtoOAErrorCode
TRADING_BAD_STOPS: ProtoOAErrorCode
TRADING_BAD_PRICES: ProtoOAErrorCode
TRADING_BAD_STAKE: ProtoOAErrorCode
PROTECTION_IS_TOO_CLOSE_TO_MARKET: ProtoOAErrorCode
TRADING_BAD_EXPIRATION_DATE: ProtoOAErrorCode
PENDING_EXECUTION: ProtoOAErrorCode
TRADING_DISABLED: ProtoOAErrorCode
TRADING_NOT_ALLOWED: ProtoOAErrorCode
UNABLE_TO_CANCEL_ORDER: ProtoOAErrorCode
UNABLE_TO_AMEND_ORDER: ProtoOAErrorCode
SHORT_SELLING_NOT_ALLOWED: ProtoOAErrorCode
ACCORDING_TO_LEVERAGE: ProtoOALimitedRiskMarginCalculationStrategy
ACCORDING_TO_GSL: ProtoOALimitedRiskMarginCalculationStrategy
ACCORDING_TO_GSL_AND_LEVERAGE: ProtoOALimitedRiskMarginCalculationStrategy
MOST_MARGIN_USED_FIRST: ProtoOAStopOutStrategy
MOST_LOSING_FIRST: ProtoOAStopOutStrategy

class ProtoOAAsset(_message.Message):
    __slots__ = ("assetId", "name", "displayName", "digits")
    ASSETID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DISPLAYNAME_FIELD_NUMBER: _ClassVar[int]
    DIGITS_FIELD_NUMBER: _ClassVar[int]
    assetId: int
    name: str
    displayName: str
    digits: int
    def __init__(self, assetId: _Optional[int] = ..., name: _Optional[str] = ..., displayName: _Optional[str] = ..., digits: _Optional[int] = ...) -> None: ...

class ProtoOASymbol(_message.Message):
    __slots__ = ("symbolId", "digits", "pipPosition", "enableShortSelling", "guaranteedStopLoss", "swapRollover3Days", "swapLong", "swapShort", "maxVolume", "minVolume", "stepVolume", "maxExposure", "schedule", "commission", "commissionType", "slDistance", "tpDistance", "gslDistance", "gslCharge", "distanceSetIn", "minCommission", "minCommissionType", "minCommissionAsset", "rolloverCommission", "skipRolloverDays", "scheduleTimeZone", "tradingMode", "rolloverCommission3Days", "swapCalculationType", "lotSize", "preciseTradingCommissionRate", "preciseMinCommission", "holiday", "pnlConversionFeeRate", "leverageId", "swapPeriod", "swapTime", "skipSWAPPeriods", "chargeSwapAtWeekends", "measurementUnits")
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    DIGITS_FIELD_NUMBER: _ClassVar[int]
    PIPPOSITION_FIELD_NUMBER: _ClassVar[int]
    ENABLESHORTSELLING_FIELD_NUMBER: _ClassVar[int]
    GUARANTEEDSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    SWAPROLLOVER3DAYS_FIELD_NUMBER: _ClassVar[int]
    SWAPLONG_FIELD_NUMBER: _ClassVar[int]
    SWAPSHORT_FIELD_NUMBER: _ClassVar[int]
    MAXVOLUME_FIELD_NUMBER: _ClassVar[int]
    MINVOLUME_FIELD_NUMBER: _ClassVar[int]
    STEPVOLUME_FIELD_NUMBER: _ClassVar[int]
    MAXEXPOSURE_FIELD_NUMBER: _ClassVar[int]
    SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_FIELD_NUMBER: _ClassVar[int]
    COMMISSIONTYPE_FIELD_NUMBER: _ClassVar[int]
    SLDISTANCE_FIELD_NUMBER: _ClassVar[int]
    TPDISTANCE_FIELD_NUMBER: _ClassVar[int]
    GSLDISTANCE_FIELD_NUMBER: _ClassVar[int]
    GSLCHARGE_FIELD_NUMBER: _ClassVar[int]
    DISTANCESETIN_FIELD_NUMBER: _ClassVar[int]
    MINCOMMISSION_FIELD_NUMBER: _ClassVar[int]
    MINCOMMISSIONTYPE_FIELD_NUMBER: _ClassVar[int]
    MINCOMMISSIONASSET_FIELD_NUMBER: _ClassVar[int]
    ROLLOVERCOMMISSION_FIELD_NUMBER: _ClassVar[int]
    SKIPROLLOVERDAYS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULETIMEZONE_FIELD_NUMBER: _ClassVar[int]
    TRADINGMODE_FIELD_NUMBER: _ClassVar[int]
    ROLLOVERCOMMISSION3DAYS_FIELD_NUMBER: _ClassVar[int]
    SWAPCALCULATIONTYPE_FIELD_NUMBER: _ClassVar[int]
    LOTSIZE_FIELD_NUMBER: _ClassVar[int]
    PRECISETRADINGCOMMISSIONRATE_FIELD_NUMBER: _ClassVar[int]
    PRECISEMINCOMMISSION_FIELD_NUMBER: _ClassVar[int]
    HOLIDAY_FIELD_NUMBER: _ClassVar[int]
    PNLCONVERSIONFEERATE_FIELD_NUMBER: _ClassVar[int]
    LEVERAGEID_FIELD_NUMBER: _ClassVar[int]
    SWAPPERIOD_FIELD_NUMBER: _ClassVar[int]
    SWAPTIME_FIELD_NUMBER: _ClassVar[int]
    SKIPSWAPPERIODS_FIELD_NUMBER: _ClassVar[int]
    CHARGESWAPATWEEKENDS_FIELD_NUMBER: _ClassVar[int]
    MEASUREMENTUNITS_FIELD_NUMBER: _ClassVar[int]
    symbolId: int
    digits: int
    pipPosition: int
    enableShortSelling: bool
    guaranteedStopLoss: bool
    swapRollover3Days: ProtoOADayOfWeek
    swapLong: float
    swapShort: float
    maxVolume: int
    minVolume: int
    stepVolume: int
    maxExposure: int
    schedule: _containers.RepeatedCompositeFieldContainer[ProtoOAInterval]
    commission: int
    commissionType: ProtoOACommissionType
    slDistance: int
    tpDistance: int
    gslDistance: int
    gslCharge: int
    distanceSetIn: ProtoOASymbolDistanceType
    minCommission: int
    minCommissionType: ProtoOAMinCommissionType
    minCommissionAsset: str
    rolloverCommission: int
    skipRolloverDays: int
    scheduleTimeZone: str
    tradingMode: ProtoOATradingMode
    rolloverCommission3Days: ProtoOADayOfWeek
    swapCalculationType: ProtoOASwapCalculationType
    lotSize: int
    preciseTradingCommissionRate: int
    preciseMinCommission: int
    holiday: _containers.RepeatedCompositeFieldContainer[ProtoOAHoliday]
    pnlConversionFeeRate: int
    leverageId: int
    swapPeriod: int
    swapTime: int
    skipSWAPPeriods: int
    chargeSwapAtWeekends: bool
    measurementUnits: str
    def __init__(self, symbolId: _Optional[int] = ..., digits: _Optional[int] = ..., pipPosition: _Optional[int] = ..., enableShortSelling: bool = ..., guaranteedStopLoss: bool = ..., swapRollover3Days: _Optional[_Union[ProtoOADayOfWeek, str]] = ..., swapLong: _Optional[float] = ..., swapShort: _Optional[float] = ..., maxVolume: _Optional[int] = ..., minVolume: _Optional[int] = ..., stepVolume: _Optional[int] = ..., maxExposure: _Optional[int] = ..., schedule: _Optional[_Iterable[_Union[ProtoOAInterval, _Mapping]]] = ..., commission: _Optional[int] = ..., commissionType: _Optional[_Union[ProtoOACommissionType, str]] = ..., slDistance: _Optional[int] = ..., tpDistance: _Optional[int] = ..., gslDistance: _Optional[int] = ..., gslCharge: _Optional[int] = ..., distanceSetIn: _Optional[_Union[ProtoOASymbolDistanceType, str]] = ..., minCommission: _Optional[int] = ..., minCommissionType: _Optional[_Union[ProtoOAMinCommissionType, str]] = ..., minCommissionAsset: _Optional[str] = ..., rolloverCommission: _Optional[int] = ..., skipRolloverDays: _Optional[int] = ..., scheduleTimeZone: _Optional[str] = ..., tradingMode: _Optional[_Union[ProtoOATradingMode, str]] = ..., rolloverCommission3Days: _Optional[_Union[ProtoOADayOfWeek, str]] = ..., swapCalculationType: _Optional[_Union[ProtoOASwapCalculationType, str]] = ..., lotSize: _Optional[int] = ..., preciseTradingCommissionRate: _Optional[int] = ..., preciseMinCommission: _Optional[int] = ..., holiday: _Optional[_Iterable[_Union[ProtoOAHoliday, _Mapping]]] = ..., pnlConversionFeeRate: _Optional[int] = ..., leverageId: _Optional[int] = ..., swapPeriod: _Optional[int] = ..., swapTime: _Optional[int] = ..., skipSWAPPeriods: _Optional[int] = ..., chargeSwapAtWeekends: bool = ..., measurementUnits: _Optional[str] = ...) -> None: ...

class ProtoOALightSymbol(_message.Message):
    __slots__ = ("symbolId", "symbolName", "enabled", "baseAssetId", "quoteAssetId", "symbolCategoryId", "description", "sortingNumber")
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLNAME_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    BASEASSETID_FIELD_NUMBER: _ClassVar[int]
    QUOTEASSETID_FIELD_NUMBER: _ClassVar[int]
    SYMBOLCATEGORYID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SORTINGNUMBER_FIELD_NUMBER: _ClassVar[int]
    symbolId: int
    symbolName: str
    enabled: bool
    baseAssetId: int
    quoteAssetId: int
    symbolCategoryId: int
    description: str
    sortingNumber: float
    def __init__(self, symbolId: _Optional[int] = ..., symbolName: _Optional[str] = ..., enabled: bool = ..., baseAssetId: _Optional[int] = ..., quoteAssetId: _Optional[int] = ..., symbolCategoryId: _Optional[int] = ..., description: _Optional[str] = ..., sortingNumber: _Optional[float] = ...) -> None: ...

class ProtoOAArchivedSymbol(_message.Message):
    __slots__ = ("symbolId", "name", "utcLastUpdateTimestamp", "description")
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    UTCLASTUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    symbolId: int
    name: str
    utcLastUpdateTimestamp: int
    description: str
    def __init__(self, symbolId: _Optional[int] = ..., name: _Optional[str] = ..., utcLastUpdateTimestamp: _Optional[int] = ..., description: _Optional[str] = ...) -> None: ...

class ProtoOASymbolCategory(_message.Message):
    __slots__ = ("id", "assetClassId", "name", "sortingNumber")
    ID_FIELD_NUMBER: _ClassVar[int]
    ASSETCLASSID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SORTINGNUMBER_FIELD_NUMBER: _ClassVar[int]
    id: int
    assetClassId: int
    name: str
    sortingNumber: float
    def __init__(self, id: _Optional[int] = ..., assetClassId: _Optional[int] = ..., name: _Optional[str] = ..., sortingNumber: _Optional[float] = ...) -> None: ...

class ProtoOAInterval(_message.Message):
    __slots__ = ("startSecond", "endSecond")
    STARTSECOND_FIELD_NUMBER: _ClassVar[int]
    ENDSECOND_FIELD_NUMBER: _ClassVar[int]
    startSecond: int
    endSecond: int
    def __init__(self, startSecond: _Optional[int] = ..., endSecond: _Optional[int] = ...) -> None: ...

class ProtoOATrader(_message.Message):
    __slots__ = ("ctidTraderAccountId", "balance", "balanceVersion", "managerBonus", "ibBonus", "nonWithdrawableBonus", "accessRights", "depositAssetId", "swapFree", "leverageInCents", "totalMarginCalculationType", "maxLeverage", "frenchRisk", "traderLogin", "accountType", "brokerName", "registrationTimestamp", "isLimitedRisk", "limitedRiskMarginCalculationStrategy", "moneyDigits", "fairStopOut", "stopOutStrategy")
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    BALANCE_FIELD_NUMBER: _ClassVar[int]
    BALANCEVERSION_FIELD_NUMBER: _ClassVar[int]
    MANAGERBONUS_FIELD_NUMBER: _ClassVar[int]
    IBBONUS_FIELD_NUMBER: _ClassVar[int]
    NONWITHDRAWABLEBONUS_FIELD_NUMBER: _ClassVar[int]
    ACCESSRIGHTS_FIELD_NUMBER: _ClassVar[int]
    DEPOSITASSETID_FIELD_NUMBER: _ClassVar[int]
    SWAPFREE_FIELD_NUMBER: _ClassVar[int]
    LEVERAGEINCENTS_FIELD_NUMBER: _ClassVar[int]
    TOTALMARGINCALCULATIONTYPE_FIELD_NUMBER: _ClassVar[int]
    MAXLEVERAGE_FIELD_NUMBER: _ClassVar[int]
    FRENCHRISK_FIELD_NUMBER: _ClassVar[int]
    TRADERLOGIN_FIELD_NUMBER: _ClassVar[int]
    ACCOUNTTYPE_FIELD_NUMBER: _ClassVar[int]
    BROKERNAME_FIELD_NUMBER: _ClassVar[int]
    REGISTRATIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ISLIMITEDRISK_FIELD_NUMBER: _ClassVar[int]
    LIMITEDRISKMARGINCALCULATIONSTRATEGY_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    FAIRSTOPOUT_FIELD_NUMBER: _ClassVar[int]
    STOPOUTSTRATEGY_FIELD_NUMBER: _ClassVar[int]
    ctidTraderAccountId: int
    balance: int
    balanceVersion: int
    managerBonus: int
    ibBonus: int
    nonWithdrawableBonus: int
    accessRights: ProtoOAAccessRights
    depositAssetId: int
    swapFree: bool
    leverageInCents: int
    totalMarginCalculationType: ProtoOATotalMarginCalculationType
    maxLeverage: int
    frenchRisk: bool
    traderLogin: int
    accountType: ProtoOAAccountType
    brokerName: str
    registrationTimestamp: int
    isLimitedRisk: bool
    limitedRiskMarginCalculationStrategy: ProtoOALimitedRiskMarginCalculationStrategy
    moneyDigits: int
    fairStopOut: bool
    stopOutStrategy: ProtoOAStopOutStrategy
    def __init__(self, ctidTraderAccountId: _Optional[int] = ..., balance: _Optional[int] = ..., balanceVersion: _Optional[int] = ..., managerBonus: _Optional[int] = ..., ibBonus: _Optional[int] = ..., nonWithdrawableBonus: _Optional[int] = ..., accessRights: _Optional[_Union[ProtoOAAccessRights, str]] = ..., depositAssetId: _Optional[int] = ..., swapFree: bool = ..., leverageInCents: _Optional[int] = ..., totalMarginCalculationType: _Optional[_Union[ProtoOATotalMarginCalculationType, str]] = ..., maxLeverage: _Optional[int] = ..., frenchRisk: bool = ..., traderLogin: _Optional[int] = ..., accountType: _Optional[_Union[ProtoOAAccountType, str]] = ..., brokerName: _Optional[str] = ..., registrationTimestamp: _Optional[int] = ..., isLimitedRisk: bool = ..., limitedRiskMarginCalculationStrategy: _Optional[_Union[ProtoOALimitedRiskMarginCalculationStrategy, str]] = ..., moneyDigits: _Optional[int] = ..., fairStopOut: bool = ..., stopOutStrategy: _Optional[_Union[ProtoOAStopOutStrategy, str]] = ...) -> None: ...

class ProtoOAPosition(_message.Message):
    __slots__ = ("positionId", "tradeData", "positionStatus", "swap", "price", "stopLoss", "takeProfit", "utcLastUpdateTimestamp", "commission", "marginRate", "mirroringCommission", "guaranteedStopLoss", "usedMargin", "stopLossTriggerMethod", "moneyDigits", "trailingStopLoss")
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    TRADEDATA_FIELD_NUMBER: _ClassVar[int]
    POSITIONSTATUS_FIELD_NUMBER: _ClassVar[int]
    SWAP_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    STOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    UTCLASTUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_FIELD_NUMBER: _ClassVar[int]
    MARGINRATE_FIELD_NUMBER: _ClassVar[int]
    MIRRORINGCOMMISSION_FIELD_NUMBER: _ClassVar[int]
    GUARANTEEDSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    USEDMARGIN_FIELD_NUMBER: _ClassVar[int]
    STOPLOSSTRIGGERMETHOD_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    TRAILINGSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    positionId: int
    tradeData: ProtoOATradeData
    positionStatus: ProtoOAPositionStatus
    swap: int
    price: float
    stopLoss: float
    takeProfit: float
    utcLastUpdateTimestamp: int
    commission: int
    marginRate: float
    mirroringCommission: int
    guaranteedStopLoss: bool
    usedMargin: int
    stopLossTriggerMethod: ProtoOAOrderTriggerMethod
    moneyDigits: int
    trailingStopLoss: bool
    def __init__(self, positionId: _Optional[int] = ..., tradeData: _Optional[_Union[ProtoOATradeData, _Mapping]] = ..., positionStatus: _Optional[_Union[ProtoOAPositionStatus, str]] = ..., swap: _Optional[int] = ..., price: _Optional[float] = ..., stopLoss: _Optional[float] = ..., takeProfit: _Optional[float] = ..., utcLastUpdateTimestamp: _Optional[int] = ..., commission: _Optional[int] = ..., marginRate: _Optional[float] = ..., mirroringCommission: _Optional[int] = ..., guaranteedStopLoss: bool = ..., usedMargin: _Optional[int] = ..., stopLossTriggerMethod: _Optional[_Union[ProtoOAOrderTriggerMethod, str]] = ..., moneyDigits: _Optional[int] = ..., trailingStopLoss: bool = ...) -> None: ...

class ProtoOATradeData(_message.Message):
    __slots__ = ("symbolId", "volume", "tradeSide", "openTimestamp", "label", "guaranteedStopLoss", "comment", "measurementUnits", "closeTimestamp")
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    TRADESIDE_FIELD_NUMBER: _ClassVar[int]
    OPENTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    GUARANTEEDSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    MEASUREMENTUNITS_FIELD_NUMBER: _ClassVar[int]
    CLOSETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    symbolId: int
    volume: int
    tradeSide: ProtoOATradeSide
    openTimestamp: int
    label: str
    guaranteedStopLoss: bool
    comment: str
    measurementUnits: str
    closeTimestamp: int
    def __init__(self, symbolId: _Optional[int] = ..., volume: _Optional[int] = ..., tradeSide: _Optional[_Union[ProtoOATradeSide, str]] = ..., openTimestamp: _Optional[int] = ..., label: _Optional[str] = ..., guaranteedStopLoss: bool = ..., comment: _Optional[str] = ..., measurementUnits: _Optional[str] = ..., closeTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOAOrder(_message.Message):
    __slots__ = ("orderId", "tradeData", "orderType", "orderStatus", "expirationTimestamp", "executionPrice", "executedVolume", "utcLastUpdateTimestamp", "baseSlippagePrice", "slippageInPoints", "closingOrder", "limitPrice", "stopPrice", "stopLoss", "takeProfit", "clientOrderId", "timeInForce", "positionId", "relativeStopLoss", "relativeTakeProfit", "isStopOut", "trailingStopLoss", "stopTriggerMethod")
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    TRADEDATA_FIELD_NUMBER: _ClassVar[int]
    ORDERTYPE_FIELD_NUMBER: _ClassVar[int]
    ORDERSTATUS_FIELD_NUMBER: _ClassVar[int]
    EXPIRATIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONPRICE_FIELD_NUMBER: _ClassVar[int]
    EXECUTEDVOLUME_FIELD_NUMBER: _ClassVar[int]
    UTCLASTUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    BASESLIPPAGEPRICE_FIELD_NUMBER: _ClassVar[int]
    SLIPPAGEINPOINTS_FIELD_NUMBER: _ClassVar[int]
    CLOSINGORDER_FIELD_NUMBER: _ClassVar[int]
    LIMITPRICE_FIELD_NUMBER: _ClassVar[int]
    STOPPRICE_FIELD_NUMBER: _ClassVar[int]
    STOPLOSS_FIELD_NUMBER: _ClassVar[int]
    TAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    CLIENTORDERID_FIELD_NUMBER: _ClassVar[int]
    TIMEINFORCE_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    RELATIVESTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    RELATIVETAKEPROFIT_FIELD_NUMBER: _ClassVar[int]
    ISSTOPOUT_FIELD_NUMBER: _ClassVar[int]
    TRAILINGSTOPLOSS_FIELD_NUMBER: _ClassVar[int]
    STOPTRIGGERMETHOD_FIELD_NUMBER: _ClassVar[int]
    orderId: int
    tradeData: ProtoOATradeData
    orderType: ProtoOAOrderType
    orderStatus: ProtoOAOrderStatus
    expirationTimestamp: int
    executionPrice: float
    executedVolume: int
    utcLastUpdateTimestamp: int
    baseSlippagePrice: float
    slippageInPoints: int
    closingOrder: bool
    limitPrice: float
    stopPrice: float
    stopLoss: float
    takeProfit: float
    clientOrderId: str
    timeInForce: ProtoOATimeInForce
    positionId: int
    relativeStopLoss: int
    relativeTakeProfit: int
    isStopOut: bool
    trailingStopLoss: bool
    stopTriggerMethod: ProtoOAOrderTriggerMethod
    def __init__(self, orderId: _Optional[int] = ..., tradeData: _Optional[_Union[ProtoOATradeData, _Mapping]] = ..., orderType: _Optional[_Union[ProtoOAOrderType, str]] = ..., orderStatus: _Optional[_Union[ProtoOAOrderStatus, str]] = ..., expirationTimestamp: _Optional[int] = ..., executionPrice: _Optional[float] = ..., executedVolume: _Optional[int] = ..., utcLastUpdateTimestamp: _Optional[int] = ..., baseSlippagePrice: _Optional[float] = ..., slippageInPoints: _Optional[int] = ..., closingOrder: bool = ..., limitPrice: _Optional[float] = ..., stopPrice: _Optional[float] = ..., stopLoss: _Optional[float] = ..., takeProfit: _Optional[float] = ..., clientOrderId: _Optional[str] = ..., timeInForce: _Optional[_Union[ProtoOATimeInForce, str]] = ..., positionId: _Optional[int] = ..., relativeStopLoss: _Optional[int] = ..., relativeTakeProfit: _Optional[int] = ..., isStopOut: bool = ..., trailingStopLoss: bool = ..., stopTriggerMethod: _Optional[_Union[ProtoOAOrderTriggerMethod, str]] = ...) -> None: ...

class ProtoOABonusDepositWithdraw(_message.Message):
    __slots__ = ("operationType", "bonusHistoryId", "managerBonus", "managerDelta", "ibBonus", "ibDelta", "changeBonusTimestamp", "externalNote", "introducingBrokerId", "moneyDigits")
    OPERATIONTYPE_FIELD_NUMBER: _ClassVar[int]
    BONUSHISTORYID_FIELD_NUMBER: _ClassVar[int]
    MANAGERBONUS_FIELD_NUMBER: _ClassVar[int]
    MANAGERDELTA_FIELD_NUMBER: _ClassVar[int]
    IBBONUS_FIELD_NUMBER: _ClassVar[int]
    IBDELTA_FIELD_NUMBER: _ClassVar[int]
    CHANGEBONUSTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXTERNALNOTE_FIELD_NUMBER: _ClassVar[int]
    INTRODUCINGBROKERID_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    operationType: ProtoOAChangeBonusType
    bonusHistoryId: int
    managerBonus: int
    managerDelta: int
    ibBonus: int
    ibDelta: int
    changeBonusTimestamp: int
    externalNote: str
    introducingBrokerId: int
    moneyDigits: int
    def __init__(self, operationType: _Optional[_Union[ProtoOAChangeBonusType, str]] = ..., bonusHistoryId: _Optional[int] = ..., managerBonus: _Optional[int] = ..., managerDelta: _Optional[int] = ..., ibBonus: _Optional[int] = ..., ibDelta: _Optional[int] = ..., changeBonusTimestamp: _Optional[int] = ..., externalNote: _Optional[str] = ..., introducingBrokerId: _Optional[int] = ..., moneyDigits: _Optional[int] = ...) -> None: ...

class ProtoOADepositWithdraw(_message.Message):
    __slots__ = ("operationType", "balanceHistoryId", "balance", "delta", "changeBalanceTimestamp", "externalNote", "balanceVersion", "equity", "moneyDigits")
    OPERATIONTYPE_FIELD_NUMBER: _ClassVar[int]
    BALANCEHISTORYID_FIELD_NUMBER: _ClassVar[int]
    BALANCE_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    CHANGEBALANCETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXTERNALNOTE_FIELD_NUMBER: _ClassVar[int]
    BALANCEVERSION_FIELD_NUMBER: _ClassVar[int]
    EQUITY_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    operationType: ProtoOAChangeBalanceType
    balanceHistoryId: int
    balance: int
    delta: int
    changeBalanceTimestamp: int
    externalNote: str
    balanceVersion: int
    equity: int
    moneyDigits: int
    def __init__(self, operationType: _Optional[_Union[ProtoOAChangeBalanceType, str]] = ..., balanceHistoryId: _Optional[int] = ..., balance: _Optional[int] = ..., delta: _Optional[int] = ..., changeBalanceTimestamp: _Optional[int] = ..., externalNote: _Optional[str] = ..., balanceVersion: _Optional[int] = ..., equity: _Optional[int] = ..., moneyDigits: _Optional[int] = ...) -> None: ...

class ProtoOADeal(_message.Message):
    __slots__ = ("dealId", "orderId", "positionId", "volume", "filledVolume", "symbolId", "createTimestamp", "executionTimestamp", "utcLastUpdateTimestamp", "executionPrice", "tradeSide", "dealStatus", "marginRate", "commission", "baseToUsdConversionRate", "closePositionDetail", "moneyDigits")
    DEALID_FIELD_NUMBER: _ClassVar[int]
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    FILLEDVOLUME_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    CREATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    UTCLASTUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONPRICE_FIELD_NUMBER: _ClassVar[int]
    TRADESIDE_FIELD_NUMBER: _ClassVar[int]
    DEALSTATUS_FIELD_NUMBER: _ClassVar[int]
    MARGINRATE_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_FIELD_NUMBER: _ClassVar[int]
    BASETOUSDCONVERSIONRATE_FIELD_NUMBER: _ClassVar[int]
    CLOSEPOSITIONDETAIL_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    dealId: int
    orderId: int
    positionId: int
    volume: int
    filledVolume: int
    symbolId: int
    createTimestamp: int
    executionTimestamp: int
    utcLastUpdateTimestamp: int
    executionPrice: float
    tradeSide: ProtoOATradeSide
    dealStatus: ProtoOADealStatus
    marginRate: float
    commission: int
    baseToUsdConversionRate: float
    closePositionDetail: ProtoOAClosePositionDetail
    moneyDigits: int
    def __init__(self, dealId: _Optional[int] = ..., orderId: _Optional[int] = ..., positionId: _Optional[int] = ..., volume: _Optional[int] = ..., filledVolume: _Optional[int] = ..., symbolId: _Optional[int] = ..., createTimestamp: _Optional[int] = ..., executionTimestamp: _Optional[int] = ..., utcLastUpdateTimestamp: _Optional[int] = ..., executionPrice: _Optional[float] = ..., tradeSide: _Optional[_Union[ProtoOATradeSide, str]] = ..., dealStatus: _Optional[_Union[ProtoOADealStatus, str]] = ..., marginRate: _Optional[float] = ..., commission: _Optional[int] = ..., baseToUsdConversionRate: _Optional[float] = ..., closePositionDetail: _Optional[_Union[ProtoOAClosePositionDetail, _Mapping]] = ..., moneyDigits: _Optional[int] = ...) -> None: ...

class ProtoOADealOffset(_message.Message):
    __slots__ = ("dealId", "volume", "executionTimestamp", "executionPrice")
    DEALID_FIELD_NUMBER: _ClassVar[int]
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONPRICE_FIELD_NUMBER: _ClassVar[int]
    dealId: int
    volume: int
    executionTimestamp: int
    executionPrice: float
    def __init__(self, dealId: _Optional[int] = ..., volume: _Optional[int] = ..., executionTimestamp: _Optional[int] = ..., executionPrice: _Optional[float] = ...) -> None: ...

class ProtoOAClosePositionDetail(_message.Message):
    __slots__ = ("entryPrice", "grossProfit", "swap", "commission", "balance", "quoteToDepositConversionRate", "closedVolume", "balanceVersion", "moneyDigits", "pnlConversionFee")
    ENTRYPRICE_FIELD_NUMBER: _ClassVar[int]
    GROSSPROFIT_FIELD_NUMBER: _ClassVar[int]
    SWAP_FIELD_NUMBER: _ClassVar[int]
    COMMISSION_FIELD_NUMBER: _ClassVar[int]
    BALANCE_FIELD_NUMBER: _ClassVar[int]
    QUOTETODEPOSITCONVERSIONRATE_FIELD_NUMBER: _ClassVar[int]
    CLOSEDVOLUME_FIELD_NUMBER: _ClassVar[int]
    BALANCEVERSION_FIELD_NUMBER: _ClassVar[int]
    MONEYDIGITS_FIELD_NUMBER: _ClassVar[int]
    PNLCONVERSIONFEE_FIELD_NUMBER: _ClassVar[int]
    entryPrice: float
    grossProfit: int
    swap: int
    commission: int
    balance: int
    quoteToDepositConversionRate: float
    closedVolume: int
    balanceVersion: int
    moneyDigits: int
    pnlConversionFee: int
    def __init__(self, entryPrice: _Optional[float] = ..., grossProfit: _Optional[int] = ..., swap: _Optional[int] = ..., commission: _Optional[int] = ..., balance: _Optional[int] = ..., quoteToDepositConversionRate: _Optional[float] = ..., closedVolume: _Optional[int] = ..., balanceVersion: _Optional[int] = ..., moneyDigits: _Optional[int] = ..., pnlConversionFee: _Optional[int] = ...) -> None: ...

class ProtoOATrendbar(_message.Message):
    __slots__ = ("volume", "period", "low", "deltaOpen", "deltaClose", "deltaHigh", "utcTimestampInMinutes")
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    DELTAOPEN_FIELD_NUMBER: _ClassVar[int]
    DELTACLOSE_FIELD_NUMBER: _ClassVar[int]
    DELTAHIGH_FIELD_NUMBER: _ClassVar[int]
    UTCTIMESTAMPINMINUTES_FIELD_NUMBER: _ClassVar[int]
    volume: int
    period: ProtoOATrendbarPeriod
    low: int
    deltaOpen: int
    deltaClose: int
    deltaHigh: int
    utcTimestampInMinutes: int
    def __init__(self, volume: _Optional[int] = ..., period: _Optional[_Union[ProtoOATrendbarPeriod, str]] = ..., low: _Optional[int] = ..., deltaOpen: _Optional[int] = ..., deltaClose: _Optional[int] = ..., deltaHigh: _Optional[int] = ..., utcTimestampInMinutes: _Optional[int] = ...) -> None: ...

class ProtoOAExpectedMargin(_message.Message):
    __slots__ = ("volume", "buyMargin", "sellMargin")
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    BUYMARGIN_FIELD_NUMBER: _ClassVar[int]
    SELLMARGIN_FIELD_NUMBER: _ClassVar[int]
    volume: int
    buyMargin: int
    sellMargin: int
    def __init__(self, volume: _Optional[int] = ..., buyMargin: _Optional[int] = ..., sellMargin: _Optional[int] = ...) -> None: ...

class ProtoOATickData(_message.Message):
    __slots__ = ("timestamp", "tick")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TICK_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    tick: int
    def __init__(self, timestamp: _Optional[int] = ..., tick: _Optional[int] = ...) -> None: ...

class ProtoOACtidProfile(_message.Message):
    __slots__ = ("userId",)
    USERID_FIELD_NUMBER: _ClassVar[int]
    userId: int
    def __init__(self, userId: _Optional[int] = ...) -> None: ...

class ProtoOACtidTraderAccount(_message.Message):
    __slots__ = ("ctidTraderAccountId", "isLive", "traderLogin", "lastClosingDealTimestamp", "lastBalanceUpdateTimestamp", "brokerTitleShort")
    CTIDTRADERACCOUNTID_FIELD_NUMBER: _ClassVar[int]
    ISLIVE_FIELD_NUMBER: _ClassVar[int]
    TRADERLOGIN_FIELD_NUMBER: _ClassVar[int]
    LASTCLOSINGDEALTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    LASTBALANCEUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    BROKERTITLESHORT_FIELD_NUMBER: _ClassVar[int]
    ctidTraderAccountId: int
    isLive: bool
    traderLogin: int
    lastClosingDealTimestamp: int
    lastBalanceUpdateTimestamp: int
    brokerTitleShort: str
    def __init__(self, ctidTraderAccountId: _Optional[int] = ..., isLive: bool = ..., traderLogin: _Optional[int] = ..., lastClosingDealTimestamp: _Optional[int] = ..., lastBalanceUpdateTimestamp: _Optional[int] = ..., brokerTitleShort: _Optional[str] = ...) -> None: ...

class ProtoOAAssetClass(_message.Message):
    __slots__ = ("id", "name", "sortingNumber")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SORTINGNUMBER_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    sortingNumber: float
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., sortingNumber: _Optional[float] = ...) -> None: ...

class ProtoOADepthQuote(_message.Message):
    __slots__ = ("id", "size", "bid", "ask")
    ID_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    BID_FIELD_NUMBER: _ClassVar[int]
    ASK_FIELD_NUMBER: _ClassVar[int]
    id: int
    size: int
    bid: int
    ask: int
    def __init__(self, id: _Optional[int] = ..., size: _Optional[int] = ..., bid: _Optional[int] = ..., ask: _Optional[int] = ...) -> None: ...

class ProtoOAMarginCall(_message.Message):
    __slots__ = ("marginCallType", "marginLevelThreshold", "utcLastUpdateTimestamp")
    MARGINCALLTYPE_FIELD_NUMBER: _ClassVar[int]
    MARGINLEVELTHRESHOLD_FIELD_NUMBER: _ClassVar[int]
    UTCLASTUPDATETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    marginCallType: ProtoOANotificationType
    marginLevelThreshold: float
    utcLastUpdateTimestamp: int
    def __init__(self, marginCallType: _Optional[_Union[ProtoOANotificationType, str]] = ..., marginLevelThreshold: _Optional[float] = ..., utcLastUpdateTimestamp: _Optional[int] = ...) -> None: ...

class ProtoOAHoliday(_message.Message):
    __slots__ = ("holidayId", "name", "description", "scheduleTimeZone", "holidayDate", "isRecurring", "startSecond", "endSecond")
    HOLIDAYID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SCHEDULETIMEZONE_FIELD_NUMBER: _ClassVar[int]
    HOLIDAYDATE_FIELD_NUMBER: _ClassVar[int]
    ISRECURRING_FIELD_NUMBER: _ClassVar[int]
    STARTSECOND_FIELD_NUMBER: _ClassVar[int]
    ENDSECOND_FIELD_NUMBER: _ClassVar[int]
    holidayId: int
    name: str
    description: str
    scheduleTimeZone: str
    holidayDate: int
    isRecurring: bool
    startSecond: int
    endSecond: int
    def __init__(self, holidayId: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., scheduleTimeZone: _Optional[str] = ..., holidayDate: _Optional[int] = ..., isRecurring: bool = ..., startSecond: _Optional[int] = ..., endSecond: _Optional[int] = ...) -> None: ...

class ProtoOADynamicLeverage(_message.Message):
    __slots__ = ("leverageId", "tiers")
    LEVERAGEID_FIELD_NUMBER: _ClassVar[int]
    TIERS_FIELD_NUMBER: _ClassVar[int]
    leverageId: int
    tiers: _containers.RepeatedCompositeFieldContainer[ProtoOADynamicLeverageTier]
    def __init__(self, leverageId: _Optional[int] = ..., tiers: _Optional[_Iterable[_Union[ProtoOADynamicLeverageTier, _Mapping]]] = ...) -> None: ...

class ProtoOADynamicLeverageTier(_message.Message):
    __slots__ = ("volume", "leverage")
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    LEVERAGE_FIELD_NUMBER: _ClassVar[int]
    volume: int
    leverage: int
    def __init__(self, volume: _Optional[int] = ..., leverage: _Optional[int] = ...) -> None: ...

class ProtoOAPositionUnrealizedPnL(_message.Message):
    __slots__ = ("positionId", "grossUnrealizedPnL", "netUnrealizedPnL")
    POSITIONID_FIELD_NUMBER: _ClassVar[int]
    GROSSUNREALIZEDPNL_FIELD_NUMBER: _ClassVar[int]
    NETUNREALIZEDPNL_FIELD_NUMBER: _ClassVar[int]
    positionId: int
    grossUnrealizedPnL: int
    netUnrealizedPnL: int
    def __init__(self, positionId: _Optional[int] = ..., grossUnrealizedPnL: _Optional[int] = ..., netUnrealizedPnL: _Optional[int] = ...) -> None: ...
