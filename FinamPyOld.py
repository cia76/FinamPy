from typing import Union  # Объединение типов
from datetime import datetime
from time import sleep
from os.path import isfile  # Справочник тикеров будем хранить в файле
from uuid import uuid4  # Номера подписок должны быть уникальными во времени и пространстве
from queue import SimpleQueue  # Очередь подписок/отписок
from threading import Thread, Event as ThreadingEvent  # Поток обработки подписок и событие
import logging  # Будем вести лог

from google.protobuf.json_format import MessageToJson, MessageToDict, Parse  # Будем хранить справочник в файле в формате JSON
from google.protobuf.timestamp_pb2 import Timestamp  # Представление времени
from google.protobuf.wrappers_pb2 import DoubleValue  # Представление цены

# REST API - Структуры
import google.type.date_pb2 as google_date
import FinamPy.proto_old.candles_pb2 as proto_candles  # Свечи https://finamweb.github.io/trade-api-docs/rest-api/candles
import FinamPy.proto_old.common_pb2 as proto_common  # Описание общих типов https://finamweb.github.io/trade-api-docs/rest-api/common-types
import FinamPy.proto_old.events_pb2 as proto_events  # События
import FinamPy.proto_old.orders_pb2 as proto_orders  # Заявки https://finamweb.github.io/trade-api-docs/rest-api/orders
import FinamPy.proto_old.portfolios_pb2 as proto_portfolios  # Портфели https://finamweb.github.io/trade-api-docs/rest-api/portfolios
import FinamPy.proto_old.security_pb2 as proto_security  # Инструменты https://finamweb.github.io/trade-api-docs/rest-api/securities
import FinamPy.proto_old.stops_pb2 as proto_stops  # Стоп-заявки https://finamweb.github.io/trade-api-docs/rest-api/stops

# gRPC - Сервисы
from FinamPy.grpc_old.candles_pb2_grpc import CandlesStub  # Сервис свечей
from FinamPy.grpc_old.events_pb2_grpc import EventsStub  # Сервис подписок https://finamweb.github.io/trade-api-docs/grpc/events
from FinamPy.grpc_old.orders_pb2_grpc import OrdersStub  # Сервис заявок https://finamweb.github.io/trade-api-docs/grpc/orders
from FinamPy.grpc_old.portfolios_pb2_grpc import PortfoliosStub  # Сервис портфелей https://finamweb.github.io/trade-api-docs/grpc/portfolios
import FinamPy.grpc_old.securities_pb2 as grpc_securities  # Структуры тикеров
from FinamPy.grpc_old.securities_pb2_grpc import SecuritiesStub  # Сервис тикеров https://finamweb.github.io/trade-api-docs/grpc/securities
from FinamPy.grpc_old.stops_pb2_grpc import StopsStub  # Сервис стоп заявок https://finamweb.github.io/trade-api-docs/grpc/stops

from pytz import timezone, utc  # Работаем с временнОй зоной и UTC
from grpc import ssl_channel_credentials, secure_channel, RpcError  # Защищенный канал

from FinamPy import Config  # Файл конфигурации


# noinspection PyProtectedMember
class FinamPyOld:
    """Работа с Finam Trade API gRPC https://finamweb.github.io/trade-api-docs/category/grpc из Python
    Генерация кода в папки grpc/proto осуществлена из proto контрактов: https://github.com/FinamWeb/trade-api-docs/tree/master/contracts
    """
    tz_msk = timezone('Europe/Moscow')  # Время UTC будем приводить к московскому времени
    server = 'trade-api.finam.ru'  # Сервер для исполнения вызовов
    markets = {proto_common.Market.MARKET_STOCK: 'Фондовый рынок Московской Биржи',
               proto_common.Market.MARKET_FORTS: 'Срочный рынок Московской Биржи',
               proto_common.Market.MARKET_SPBEX: 'Санкт-Петербургская биржа',
               proto_common.Market.MARKET_MMA: 'Фондовый рынок США',
               proto_common.Market.MARKET_ETS: 'Валютный рынок Московской Биржи',
               proto_common.Market.MARKET_BONDS: 'Долговой рынок Московской Биржи',
               proto_common.Market.MARKET_OPTIONS: 'Рынок опционов Московской Биржи'}  # Рынки
    securities_filename = 'FinamSecurities.json'  # Имя файла справочника тикеров
    logger = logging.getLogger('FinamPy')  # Будем вести лог

    def __init__(self, client_ids=Config.client_ids, access_token=Config.access_token_old):
        """Инициализация

        :param tuple[str] client_ids: Торговые счета
        :param str access_token: Торговый токен
        """
        self.metadata = [('x-api-key', access_token)]  # Торговый токен доступа
        self.channel = secure_channel(self.server, ssl_channel_credentials())  # Защищенный канал

        # Структуры
        self.google_date = google_date.Date
        self.message_to_dict = MessageToDict
        self.proto_candles = proto_candles
        self.proto_common = proto_common
        self.proto_events = proto_events
        self.proto_orders = proto_orders
        self.proto_portfolios = proto_portfolios
        self.proto_security = proto_security
        self.proto_stops = proto_stops

        # Сервисы
        self.candles_stub = CandlesStub(self.channel)  # Свечи
        self.orders_stub = OrdersStub(self.channel)  # Заявки
        self.portfolios_stub = PortfoliosStub(self.channel)  # Портфели
        self.securities_stub = SecuritiesStub(self.channel)  # Тикеры
        self.stops_stub = StopsStub(self.channel)  # Стоп заявки

        # События
        self.on_order = self.default_handler  # Заявка
        self.on_trade = self.default_handler  # Сделка
        self.on_order_book = self.default_handler  # Стакан
        self.on_portfolio = self.default_handler  # Портфель
        self.on_response = self.default_handler  # Подтверждающее сообщение для поддержания активности

        self.keep_alive_exit_event = ThreadingEvent()  # Определяем событие выхода из потока поддержания активности
        self.keep_alive_thread = None  # Поток поддержания активности создадим при первой подписке
        self.subscription_queue: SimpleQueue[proto_events.SubscriptionRequest] = SimpleQueue()  # Буфер команд на подписку/отписку
        self.subscriptions_thread = None  # Поток обработки подписок создадим при первой подписке

        self.client_ids = client_ids  # Пока нет сервиса получения торговых счетов, указываем явно счета в файле конфигурации
        self.symbols = self.get_securities()  # Получаем справочник тикеров из файла или сервиса

    # Заявки / Orders (https://finamweb.github.io/trade-api-docs/grpc/orders)

    def get_orders(self, client_id, include_matched=True, include_canceled=True, include_active=True) -> Union[proto_orders.GetOrdersResult, None]:
        """Возвращает список заявок

        :param str client_id: Идентификатор торгового счёта
        :param bool include_matched: Вернуть исполненные заявки
        :param bool include_canceled: Вернуть отмененные заявки
        :param bool include_active: Вернуть активные заявки
        """
        request = proto_orders.GetOrdersRequest(client_id=client_id, include_matched=include_matched, include_canceled=include_canceled, include_active=include_active)
        return self.call_function(self.orders_stub.GetOrders, request)

    def new_order(self, client_id, security_board, security_code, buy_sell, quantity, use_credit=False, price: float = None,
                  order_property=proto_orders.OrderProperty.ORDER_PROPERTY_PUT_IN_QUEUE, condition=None, valid_before=None) -> Union[proto_orders.NewOrderResult, None]:
        """Создать новую заявку

        :param str client_id: Идентификатор торгового счёта
        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param proto_common.BuySell.ValueType buy_sell: Направление сделки
            BUY_SELL_BUY - Покупка
            BUY_SELL_SELL - Продажа
        :param int quantity: Количество лотов инструмента для заявки
        :param bool use_credit: Использовать кредит. Недоступно для срочного рынка
        :param float price: Цена заявки. None для рыночной заявки
        :param proto_orders.OrderProperty.ValueType order_property: Поведение заявки при выставлении в стакан
            ORDER_PROPERTY_PUT_IN_QUEUE - Неисполненная часть заявки помещается в очередь заявок Биржи
            ORDER_PROPERTY_CANCEL_BALANCE - (FOK) Неисполненная часть заявки снимается с торгов
            ORDER_PROPERTY_IMM_OR_CANCEL - (IOC) Сделки совершаются только в том случае, если заявка может быть удовлетворена полностью и сразу при выставлении
        :param proto_orders.OrderCondition condition: Типы условных ордеров
            type - Тип условия (OrderConditionType)
                ORDER_CONDITION_TYPE_BID - Лучшая цена покупки
                ORDER_CONDITION_TYPE_BID_OR_LAST - Лучшая цена покупки или сделка по заданной цене и выше
                ORDER_CONDITION_TYPE_ASK - Лучшая цена продажи
                ORDER_CONDITION_TYPE_ASK_OR_LAST - Лучшая цена продажи или сделка по заданной цене и ниже
                ORDER_CONDITION_TYPE_TIME - Время выставления заявки на Биржу. Параметр OrderCondition.time должен быть установлен
                ORDER_CONDITION_TYPE_COV_DOWN - Обеспеченность ниже заданной
                ORDER_CONDITION_TYPE_COV_UP: - Обеспеченность выше заданной
                ORDER_CONDITION_TYPE_LAST_UP - Сделка на рынке по заданной цене или выше
                ORDER_CONDITION_TYPE_LAST_DOWN - Сделка на рынке по заданной цене или ниже
            price - Значение цены для условия
            time - Время выставления в UTC
        :param proto_common.OrderValidBefore valid_before: Условие по времени действия заявки
            type - Установка временных рамок действия заявки (OrderValidBeforeType)
                ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION - Заявка действует до конца сессии
                ORDER_VALID_BEFORE_TYPE_TILL_CANCELLED - Заявка действует, пока не будет отменена
                ORDER_VALID_BEFORE_TYPE_EXACT_TIME - Заявка действует до указанного времени. Параметр OrderValidBefore.time должно быть установлен
            time: Время действия заявки в UTC
        """
        if price:  # Если указана цена
            request = proto_orders.NewOrderRequest(
                client_id=client_id, security_board=security_board, security_code=security_code, buy_sell=buy_sell, quantity=quantity, price=DoubleValue(value=price),
                use_credit=use_credit, property=order_property, condition=condition, valid_before=valid_before)  # То выставляем лимитную заявку
        else:  # Если цена не указана
            request = proto_orders.NewOrderRequest(
                client_id=client_id, security_board=security_board, security_code=security_code, buy_sell=buy_sell, quantity=quantity,
                use_credit=use_credit, property=order_property, condition=condition, valid_before=valid_before)  # То выставляем рыночную заявку
        return self.call_function(self.orders_stub.NewOrder, request)

    def cancel_order(self, client_id, transaction_id) -> Union[proto_orders.CancelOrderResult, None]:
        """Отменяет заявку

        :param str client_id: Идентификатор торгового счёта
        :param int transaction_id: Идентификатор транзакции, который может быть использован для отмены заявки или определения номера заявки в сервисе событий
        """
        request = proto_orders.CancelOrderRequest(client_id=client_id, transaction_id=transaction_id)
        return self.call_function(self.orders_stub.CancelOrder, request)

        # Portfolios

    # Стоп-заявки / Stops (https://finamweb.github.io/trade-api-docs/grpc/stops)

    def get_stops(self, client_id, include_executed=True, include_canceled=True, include_active=True) -> Union[proto_stops.GetStopsResult, None]:
        """Возвращает список стоп-заявок

        :param str client_id: Идентификатор торгового счёта
        :param bool include_executed: Вернуть исполненные стоп-заявки
        :param bool include_canceled: Вернуть отмененные стоп-заявки
        :param bool include_active: Вернуть активные стоп-заявки
        """
        request = proto_stops.GetStopsRequest(client_id=client_id, include_executed=include_executed, include_canceled=include_canceled, include_active=include_active)
        return self.call_function(self.stops_stub.GetStops, request)

    def new_stop(self, client_id, security_board, security_code, buy_sell, stop_loss=None, take_profit=None,
                 expiration_date=None, link_order=None, valid_before=None) -> Union[proto_stops.NewStopResult, None]:
        """Выставляет стоп-заявку

        :param str client_id: Идентификатор торгового счёта
        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param proto_common.BuySell.ValueType buy_sell: Направление сделки
            BUY_SELL_BUY - Покупка
            BUY_SELL_SELL - Продажа
        :param proto_stops.StopLoss stop_loss: Стоп лосс заявка
            activation_price - Цена активации
            price - Цена заявки
            market_price - По рынку
            quantity - Объем стоп-заявки (StopQuantity)
                value - Значение объема
                units - Единицы объема
                    STOP_QUANTITY_UNITS_PERCENT - Значение а процентах
                    STOP_QUANTITY_UNITS_LOTS - Значение в лотах
            time: Защитное время, сек.
            use_credit: Использовать кредит
        :param proto_stops.TakeProfit take_profit: Тейк профит заявка
            activation_price - Цена активации
            correction_price - Коррекция (StopPrice)
                value - Значение цены
                units - Единицы цены
                    STOP_PRICE_UNITS_PERCENT - Значение в процентах
                    STOP_PRICE_UNITS_PIPS - Значение в лотах
            spread_price - Защитный спрэд (StopPrice)
                value - Значение цены
                units - Единицы цены
                    STOP_PRICE_UNITS_PERCENT - Значение в процентах
                    STOP_PRICE_UNITS_PIPS - Значение в лотах
            market_price - По рынку
            quantity - Количество тейк-профит заявки (StopQuantity)
                value - Значение объема
                units - Единицы объема
                    STOP_QUANTITY_UNITS_PERCENT - Значение а процентах
                    STOP_QUANTITY_UNITS_LOTS - Значение в лотах
            time - Защитное время, сек.
            use_credit - Использовать кредит
        :param Timestamp expiration_date: Дата экспирации заявки FORTS
        :param int link_order: Биржевой номер связанной (активной) заявки
        :param proto_common.OrderValidBefore valid_before: Время действия заявки
            type - Установка временных рамок действия заявки (OrderValidBeforeType)
                ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION - Заявка действует до конца сессии
                ORDER_VALID_BEFORE_TYPE_TILL_CANCELLED - Заявка действует, пока не будет отменена
                ORDER_VALID_BEFORE_TYPE_EXACT_TIME - Заявка действует до указанного времени. Параметр OrderValidBefore.time должно быть установлен
            time: Время действия заявки в UTC
        """
        request = proto_stops.NewStopRequest(
            client_id=client_id, security_board=security_board, security_code=security_code, buy_sell=buy_sell,
            stop_loss=stop_loss, take_profit=take_profit,
            expiration_date=expiration_date, link_order=link_order, valid_before=valid_before)
        return self.call_function(self.stops_stub.NewStop, request)

    def cancel_stop(self, client_id, stop_id) -> Union[proto_stops.CancelStopResult, None]:
        """Снимает стоп-заявку

        :param str client_id: Идентификатор торгового счёта
        :param int stop_id: Идентификатор стоп-заявки
        """
        request = proto_stops.CancelStopRequest(client_id=client_id, stop_id=stop_id)
        return self.call_function(self.stops_stub.CancelStop, request)

    # Портфели / Portfolios (https://finamweb.github.io/trade-api-docs/grpc/portfolios)

    def get_portfolio(self, client_id, include_currencies=True, include_money=True, include_positions=True, include_max_buy_sell=True) -> Union[proto_portfolios.GetPortfolioResult, None]:
        """Возвращает портфель

        :param str client_id: Идентификатор торгового счёта
        :param bool include_currencies: Валютные позиции
        :param bool include_money: Денежные позиции
        :param bool include_positions: Позиции DEPO
        :param bool include_max_buy_sell: Лимиты покупки и продажи
        """
        request = proto_portfolios.GetPortfolioRequest(client_id=client_id, content=proto_portfolios.PortfolioContent(
            include_currencies=include_currencies,
            include_money=include_money,
            include_positions=include_positions,
            include_max_buy_sell=include_max_buy_sell))
        return self.call_function(self.portfolios_stub.GetPortfolio, request)

    # Подписки / Events (https://finamweb.github.io/trade-api-docs/grpc/events)

    def subscribe_order_trade(self, client_ids, include_trades=True, include_orders=True, request_id=None) -> str:
        """Запрос подписки на ордера и сделки

        :param list client_ids: Торговые коды счетов
        :param bool include_trades: Включить сделки в подписку
        :param bool include_orders: Включить заявки в подписку
        :param str request_id: Идентификатор запроса
        """
        self.check_threads()  # Запускаем поток обработки подписок, если не был запущен
        if not request_id:  # Если идентификатор запроса не указан
            request_id = uuid4().hex[:16].upper()  # то создаем его из первых 16-и символов уникального идентификатора
        request = proto_events.SubscriptionRequest(
            order_trade_subscribe_request=proto_events.OrderTradeSubscribeRequest(request_id=request_id, client_ids=client_ids, include_trades=include_trades, include_orders=include_orders))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку
        return request_id

    def unsubscribe_order_trade(self, request_id):
        """Отменить все предыдущие запросы на подписки на ордера и сделки

        :param str request_id: Идентификатор запроса
        """
        request = proto_events.SubscriptionRequest(
            order_trade_unsubscribe_request=proto_events.OrderTradeUnsubscribeRequest(request_id=request_id))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку

    def subscribe_order_book(self, security_code, security_board, request_id=None) -> str:
        """Запрос подписки на стакан

        :param str security_code: Тикер инструмента
        :param str security_board: Режим торгов
        :param str request_id: Идентификатор запроса
        """
        self.check_threads()  # Запускаем поток обработки подписок, если не был запущен
        if not request_id:  # Если идентификатор запроса не указан
            request_id = uuid4().hex[:16].upper()  # то создаем его из первых 16-и символов уникального идентификатора
        request = proto_events.SubscriptionRequest(
            order_book_subscribe_request=proto_events.OrderBookSubscribeRequest(request_id=request_id, security_code=security_code, security_board=security_board))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку
        return request_id

    def unsubscribe_order_book(self, request_id, security_code, security_board):
        """Запрос на отписку от стакана

        :param str request_id: Идентификатор запроса
        :param str security_code: Тикер инструмента
        :param str security_board: Режим торгов
        """
        request = proto_events.SubscriptionRequest(
            order_book_unsubscribe_request=proto_events.OrderBookUnsubscribeRequest(request_id=request_id, security_code=security_code, security_board=security_board))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку

    def keep_alive(self, request_id=None) -> str:
        """Сообщение для поддержания активности

        :param str request_id: Идентификатор запроса
        """
        self.check_threads()  # Запускаем поток обработки подписок, если не был запущен
        if not request_id:  # Если идентификатор запроса не указан
            request_id = uuid4().hex[:16].upper()  # то создаем его из первых 16-и символов уникального идентификатора
        request = proto_events.SubscriptionRequest(keep_alive_request=proto_events.KeepAliveRequest(request_id=request_id))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку
        return request_id

    # Инструменты / Securities

    # noinspection PyUnusedLocal
    def get_securities(self, security_board=None, security_code=None) -> Union[grpc_securities.GetSecuritiesResult, None]:
        """Справочник инструментов

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        """
        symbols = grpc_securities.GetSecuritiesResult()  # Тип списка инструментов
        if isfile(self.securities_filename):  # Если справочник уже был сохранен в файл
            with open(self.securities_filename, 'r', encoding='UTF-8') as f:  # Открываем файл на чтение
                Parse(f.read(), symbols)  # Получаем список инструментов из файла, приводим к типу
        else:  # Если файла справочника нет
            request = grpc_securities.GetSecuritiesRequest()  # Запрос справочника
            # TODO Пока поиск по режиму торгов и тикеру не работает у Финама
            # if security_board and security_code:  # Если указаны режим торгов и тикер
            #     request = grpc_securities.GetSecuritiesRequest(board=security_board, seccode=security_code)  # Запрос справочника по режиму торгов и тикеру
            # elif security_board:  # Если указан только режим торгов
            #     request = grpc_securities.GetSecuritiesRequest(board=security_board)  # Запрос справочника по режиму торгов
            # elif security_code:  # Если указан только тикер
            #     request = grpc_securities.GetSecuritiesRequest(seccode=security_code)  # Запрос справочника по тикеру
            # else:  # Ничего не указано
            #     request = grpc_securities.GetSecuritiesRequest()  # Запрос справочника
            symbols = self.call_function(self.securities_stub.GetSecurities, request)  # Выполняем запрос
            with open(self.securities_filename, 'w', encoding='UTF-8') as f:  # Открываем файл на запись
                f.write(MessageToJson(symbols))  # Сохраняем список инструментов в файл
        return symbols

    # Свечи / Candles

    def get_day_candles(self, security_board, security_code, time_frame, interval) -> proto_candles.GetDayCandlesResult:
        """Запрос дневных/недельных свечей

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param DayCandleTimeFrame time_frame: Временной интервал дневной свечи
            DAYCANDLE_TIMEFRAME_D1 - 1 день
            DAYCANDLE_TIMEFRAME_W1 - 1 неделя
        :param DayCandleInterval interval: Интервал запроса дневных свечей. Максимум 365 дней
            setattr(DayCandleInterval, 'from', <Дата начала в формате yyyy-MM-dd в часовом поясе UTC>)
            to - Дата окончания в формате yyyy-MM-dd в часовом поясе UTC
            count - Кол-во свечей. Максимум 500
        """
        request = proto_candles.GetDayCandlesRequest(security_board=security_board, security_code=security_code, time_frame=time_frame, interval=interval)
        return self.call_function(self.candles_stub.GetDayCandles, request)

    def get_intraday_candles(self, security_board, security_code, time_frame, interval) -> proto_candles.GetIntradayCandlesResult:
        """Запрос внутридневных свечей

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param IntradayCandleTimeFrame time_frame: Временной интервал внутридневной свечи
            INTRADAYCANDLE_TIMEFRAME_M1 - 1 минута
            INTRADAYCANDLE_TIMEFRAME_M5 - 5 минут
            INTRADAYCANDLE_TIMEFRAME_M15 - 15 минут
            INTRADAYCANDLE_TIMEFRAME_H1 - 1 час
        :param IntradayCandleInterval interval: Интервал запроса внутридневных свечей. Максимум 30 дней
            setattr(DayCandleInterval, 'from', <Дата начала в формате yyyy-MM-ddTHH:mm:ssZ в часовом поясе UTC>)
            to - Дата окончания в формате yyyy-MM-ddTHH:mm:ssZ в часовом поясе UTC
            count - Кол-во свечей. Максимум 500
        """
        request = proto_candles.GetIntradayCandlesRequest(security_board=security_board, security_code=security_code, time_frame=time_frame, interval=interval)
        return self.call_function(self.candles_stub.GetIntradayCandles, request)

    # Запросы

    def call_function(self, func, request):
        """Вызов функции"""
        while True:  # Пока не получим ответ или ошибку
            try:  # Пытаемся
                response, call = func.with_call(request=request, metadata=self.metadata)  # вызвать функцию
                # self.logger.debug(f'Запрос: {request} Ответ: {response}')  # Для отладки работоспособности сервера Финам
                return response  # и вернуть ответ
            except RpcError as ex:  # Если получили ошибку канала
                func_name = func._method.decode('utf-8')  # Название функции
                details = ex.args[0].details  # Сообщение об ошибке
                if 'Too many requests' in details:  # Если превышено допустимое кол-во запросов в минуту
                    sleep_seconds = 60
                    self.logger.warning(f'Превышение кол-ва запросов в минуту при вызове функции {func_name} с параметрами {request}Запрос повторится через {sleep_seconds} с')
                    sleep(sleep_seconds)  # Ждем
                else:  # В остальных случаях
                    self.logger.error(f'Ошибка {details} при вызове функции {func_name} с параметрами {request}')
                    return None  # Возвращаем пустое значение

    # Подписки

    def default_handler(self, event: Union[proto_events.OrderEvent, proto_events.TradeEvent, proto_events.OrderBookEvent, proto_events.PortfolioEvent, proto_common.ResponseEvent]):
        """Пустой обработчик события по умолчанию. Его можно заменить на пользовательский"""
        pass

    def check_threads(self):
        """Запуск потоков поддержания активности и обработки подписок, если не были запущены"""
        if not self.keep_alive_thread:  # Если еще нет потока поддержания активности
            self.keep_alive_thread = Thread(target=self.keep_alive_handler, name='KeepAliveThread')  # Создаем поток поддержания активности
            self.keep_alive_thread.start()  # Запускаем поток
        if not self.subscriptions_thread:  # Если еще нет потока обработки подписок
            self.subscriptions_thread = Thread(target=self.subscriptions_handler, name='SubscriptionsThread')  # Создаем поток обработки подписок
            self.subscriptions_thread.start()  # Запускаем поток

    def request_iterator(self):
        """Генератор запросов на подписку/отписку"""
        while True:  # Будем пытаться читать из очереди до закрытия канала
            yield self.subscription_queue.get()  # Возврат из этой функции. При повторном ее вызове исполнение продолжится с этой строки

    def subscriptions_handler(self):
        """Поток обработки подписок"""
        events_stub = EventsStub(self.channel)  # Сервис событий (подписок)
        events = events_stub.GetEvents(request_iterator=self.request_iterator(), metadata=self.metadata)  # Получаем значения подписок
        try:
            for event in events:  # Пробегаемся по значениям подписок до закрытия канала
                e: proto_events.Event = event  # Приводим пришедшее значение к подпискам
                if e.order != proto_events.OrderEvent():  # Если пришло событие с заявкой
                    self.logger.debug(f'subscriptions_handler: Пришли данные подписки OrderEvent {e.order}')
                    self.on_order(e.order)
                if e.trade != proto_events.TradeEvent():  # Если пришло событие со сделкой
                    self.logger.debug(f'subscriptions_handler: Пришли данные подписки TradeEvent {e.trade}')
                    self.on_trade(e.trade)
                if e.order_book != proto_events.OrderBookEvent():  # Если пришло событие стакана
                    self.logger.debug(f'subscriptions_handler: Пришли данные подписки OrderBookEvent {e.order_book}')
                    self.on_order_book(e.order_book)
                if e.portfolio != proto_events.PortfolioEvent():  # Если пришло событие портфеля
                    self.logger.debug(f'subscriptions_handler: Пришли данные подписки PortfolioEvent {e.portfolio}')
                    self.on_portfolio(e.portfolio)
                if e.response != proto_common.ResponseEvent():  # Если пришло событие результата выполнения запроса
                    self.logger.debug(f'subscriptions_handler: Пришли данные подписки ResponseEvent {e.response}')
                    self.on_response(e.response)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            self.subscriptions_thread = None  # Сбрасываем поток обработки подписок. Запустим его снова на новой подписке

    def keep_alive_handler(self):
        """Поток поддержания активности. Чтобы сервер Финам не удалял канал, и не получали ошибку Stream removed"""
        while True:
            self.logger.debug(f'keep_alive_handler: Отправлено сообщение для поддержания активности {self.keep_alive()}')
            exit_event_set = self.keep_alive_exit_event.wait(120)  # Ждем нового бара или события выхода из потока  # Ждем 2 минуты (подбирается экспериментально)
            if exit_event_set:  # Если произошло событие выхода из потока
                self.logger.debug('Выход из потока поддержания активности')
                return  # Выходим из потока, дальше не продолжаем

    # Выход и закрытие

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_channel()

    def __del__(self):
        self.close_channel()

    def close_channel(self):
        """Закрытие канала"""
        self.keep_alive_exit_event.set()  # Останавливаем поток поддержания активности
        self.channel.close()  # Закрываем канал

    # Функции конвертации

    @staticmethod
    def finam_board_to_board(finam_board):
        """Канонический код режима торгов из кода режима торгов Финам

        :param str finam_board: Код режима торгов Финам
        :return: Канонический код режима торгов
        """
        if finam_board == 'FUT':  # Для фьючерсов
            return 'SPBFUT'
        elif finam_board == 'OPT':  # Для опционов
            return 'SPBOPT'
        return finam_board

    @staticmethod
    def board_to_finam_board(board):
        """Код режима торгов Финам из канонического кода режима торгов

        :param str board: Канонический код режима торгов
        :return: Код режима торгов Финам
        """
        if board == 'SPBFUT':  # Для фьючерсов
            return 'FUT'
        if board == 'SPBOPT':  # Для опционов
            return 'OPT'
        return board

    def dataname_to_finam_board_symbol(self, dataname) -> tuple[Union[str, None], str]:
        """Код режима торгов и тикер из названия тикера

        :param str dataname: Название тикера
        :return: Код режима торгов и тикер
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код режима торгов>.<Код тикера>
            board = symbol_parts[0]  # Код режима торгов
            symbol = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без режима торгов
            symbol = dataname  # Код тикера
            board = next((item.board for item in self.symbols.securities if item.code == symbol), None)  # Получаем код режима торгов первого совпадающего тикера
            if board is None:  # Если спецификация тикера нигде не найдена
                return None, symbol  # то возвращаем без кода режима торгов
        finam_board = self.board_to_finam_board(board)  # Код режима торгов Финам
        return finam_board, symbol

    def finam_board_symbol_to_dataname(self, finam_board, symbol) -> str:
        """Название тикера из кода режима торгов Финам и тикера

        :param str finam_board: Код режима торгов Финам
        :param str symbol: Тикер
        :return: Название тикера
        """
        return f'{self.finam_board_to_board(finam_board)}.{symbol}'

    def get_symbol_info(self, board_market, symbol) -> Union[proto_security.Security, None]:
        """Спецификация тикера

        :param str board_market: Код режима торгов или рынка
        :param str symbol: Тикер
        :return: Значение из кэша или None, если тикер не найден
        """
        symbol_info = next((security for security in self.symbols.securities if security.board == board_market and security.code == symbol), None)  # Ищем значение в справочнике по коду режима торгов
        if not symbol_info:  # Если тикер не найден
            symbol_info = next((security for security in self.symbols.securities if security.market == board_market and security.code == symbol), None)  # то ищем по рынку тикера (для позиций)
        if not symbol_info:  # Если тикер не найден
            self.logger.warning(f'Информация о {board_market}.{symbol} не найдена')
        return symbol_info

    @staticmethod
    def timeframe_to_finam_timeframe(tf):
        """Перевод временнОго интервала во временной интервал Финам

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал Финам, внутридневной интервал
        """
        if tf[0:1] == 'D':  # 1 день
            return proto_candles.DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1, False
        if tf[0:1] == 'W':  # 1 неделя
            return proto_candles.DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_W1, False
        if tf[0:1] == 'M':  # Минуты
            if not tf[1:].isdigit():  # Если после минут не стоит число (Месяц MN?)
                raise NotImplementedError  # то с такими временнЫми интервалами не работаем
            interval = int(tf[1:])  # Временной интервал
            if interval == 1:  # 1 минута
                return proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1, True
            if interval == 5:  # 5 минут
                return proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5, True
            if interval == 15:  # 15 минут
                return proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15, True
            if interval == 60:  # 1 час
                return proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1, True
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def finam_timeframe_to_timeframe(tf, intraday) -> str:
        """Перевод временнОго интервала Финама во временной интервал

        :param IntradayCandleTimeFrame|DayCandleTimeFrame tf: Временной интервал Финама
        :param bool intraday: Внутридневной интервал
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        """
        if intraday:  # Для внутридневных интервалов
            if tf == proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1:  # 1 минута
                return 'M1'
            elif tf == proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5:  # 5 минут
                return 'M5'
            elif tf == proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15:  # 15 минут
                return 'M15'
            elif tf == proto_candles.IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1:  # 1 час
                return 'M60'
        else:  # Для дневных и недельных интервалов
            if tf == proto_candles.DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1:  # 1 день
                return 'D1'
            elif tf == proto_candles.DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_W1:  # 1 неделя
                return 'W1'
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def decimal_to_float(decimal) -> float:
        """Перевод из Google Decimal в вещественное число

        :param Decimal decimal: Google Decimal
        :return: Вещественное число
        """
        return round(decimal.num * 10 ** -decimal.scale, decimal.scale)

    @staticmethod
    def dict_decimal_to_float(dict_decimal) -> float:
        """Перевод из словаря Google Decimal в вещественное число

        :param dict dict_decimal: Словарь Google Decimal
        :return: Вещественное число
        """
        return round(int(dict_decimal['num']) * 10 ** -int(dict_decimal['scale']), int(dict_decimal['scale']))

    def price_to_finam_price(self, board, symbol, price) -> float:
        """Перевод цены в цену Финама

        :param str board: Код режима торгов
        :param str symbol: Тикер
        :param float price: Цена
        :return: Цена в Финам
        """
        si = self.get_symbol_info(board, symbol)  # Информация о тикере
        if board in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            finam_price = price / (si.bp_cost / 10 ** -si.decimals / 100)  # Пункты цены для котировок облигаций представляют собой проценты номинала облигации
        else:  # В остальных случаях
            finam_price = price  # Цена не изменяется
        min_step = 10 ** -si.decimals * si.min_step  # Шаг цены
        return round(finam_price // min_step * min_step, si.decimals)  # Округляем цену кратно шага цены

    def finam_price_to_price(self, board, symbol, finam_price) -> float:
        """Перевод цены Финама в цену

        :param str board: Код режима торгов
        :param str symbol: Тикер
        :param float finam_price: Цена в Финам
        :return: Цена
        """
        si = self.get_symbol_info(board, symbol)  # Информация о тикере
        min_step = 10 ** -si.decimals * si.min_step  # Шаг цены
        finam_price = finam_price // min_step * min_step  # Цена кратная шагу цены
        if board in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            price = finam_price * (si.bp_cost / 10 ** -si.decimals / 100)  # Пункты цены для котировок облигаций представляют собой проценты номинала облигации
        else:  # В остальных случаях
            price = finam_price  # Цена не изменяется
        return round(price, si.decimals)

    def msk_to_utc_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из московского в UTC

        :param datetime dt: Московское время
        :param bool tzinfo: Отображать временнУю зону
        :return: Время UTC
        """
        dt_msk = self.tz_msk.localize(dt)  # Задаем временнУю зону МСК
        dt_utc = dt_msk.astimezone(utc)  # Переводим в UTC
        return dt_utc if tzinfo else dt_utc.replace(tzinfo=None)

    def utc_to_msk_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из UTC в московское

        :param datetime dt: Время UTC
        :param bool tzinfo: Отображать временнУю зону
        :return: Московское время
        """
        dt_utc = utc.localize(dt)  # Задаем временнУю зону UTC
        dt_msk = dt_utc.astimezone(self.tz_msk)  # Переводим в МСК
        return dt_msk if tzinfo else dt_msk.replace(tzinfo=None)
