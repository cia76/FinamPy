from datetime import datetime
from typing import Union  # Объединение типов
from uuid import uuid4  # Номера подписок должны быть уникальными во времени и пространстве

import pytz
from pytz import timezone, utc  # Работаем с временнОй зоной и UTC
from threading import Thread  # Поток обработки подписок
from queue import SimpleQueue  # Очередь подписок/отписок
from grpc import ssl_channel_credentials, secure_channel, RpcError  # Защищенный канал

from google.protobuf.timestamp_pb2 import Timestamp  # Представление времени
from google.protobuf.wrappers_pb2 import DoubleValue  # Представление цены
from .proto.tradeapi.v1 import common_pb2 as common  # Покупка/продажа
from .proto.tradeapi.v1.common_pb2 import Market, OrderValidBefore, ResponseEvent  # Рынки и событие результата выполнения запроса
from .proto.tradeapi.v1.security_pb2 import Security  # Тикер
from .proto.tradeapi.v1.candles_pb2 import (
    DayCandleTimeFrame, DayCandleInterval, GetDayCandlesRequest, GetDayCandlesResult,
    IntradayCandleTimeFrame, IntradayCandleInterval, GetIntradayCandlesRequest, GetIntradayCandlesResult)  # Свечи
from .proto.tradeapi.v1.events_pb2 import (
    SubscriptionRequest, OrderBookSubscribeRequest, OrderBookUnsubscribeRequest, OrderTradeSubscribeRequest, OrderTradeUnsubscribeRequest,
    Event, OrderEvent, TradeEvent, OrderBookEvent, PortfolioEvent)  # Запросы и события подписок
from .grpc.tradeapi.v1.events_pb2_grpc import EventsStub  # Сервис подписок
from .proto.tradeapi.v1.orders_pb2 import (
    GetOrdersRequest, GetOrdersResult,
    OrderProperty, OrderCondition, NewOrderRequest, NewOrderResult,
    CancelOrderRequest, CancelOrderResult)  # Заявки
from .grpc.tradeapi.v1.candles_pb2_grpc import CandlesStub  # Сервис свечей
from .grpc.tradeapi.v1.orders_pb2_grpc import OrdersStub  # Сервис заявок
from .proto.tradeapi.v1.portfolios_pb2 import PortfolioContent, GetPortfolioRequest, GetPortfolioResult  # Портфель
from .grpc.tradeapi.v1.portfolios_pb2_grpc import PortfoliosStub  # Сервис портфелей
from .grpc.tradeapi.v1.securities_pb2 import GetSecuritiesRequest, GetSecuritiesResult  # Тикеры
from .grpc.tradeapi.v1.securities_pb2_grpc import SecuritiesStub  # Сервис тикеров
from .proto.tradeapi.v1.stops_pb2 import (
    GetStopsRequest, GetStopsResult, StopLoss, TakeProfit, NewStopRequest, NewStopResult, CancelStopRequest, CancelStopResult)   # Стоп заявки
from .grpc.tradeapi.v1.stops_pb2_grpc import StopsStub  # Сервис стоп заявок


class FinamPy:
    """Работа с сервером TRANSAQ из Python через REST/gRPC
    Документация интерфейса Finam Trade API: https://finamweb.github.io/trade-api-docs/
    Генерация кода в папках grpc/proto осуществлена из proto контрактов: https://github.com/FinamWeb/trade-api-docs/tree/master/contracts
    """
    tz_msk = timezone('Europe/Moscow')  # Время UTC будем приводить к московскому времени
    server = 'trade-api.finam.ru'  # Сервер для исполнения вызовов
    markets = {Market.MARKET_STOCK: 'Фондовый рынок Московской Биржи',
               Market.MARKET_FORTS: 'Срочный рынок Московской Биржи',
               Market.MARKET_SPBEX: 'Санкт-Петербургская биржа',
               Market.MARKET_MMA: 'Фондовый рынок США',
               Market.MARKET_ETS: 'Валютный рынок Московской Биржи',
               Market.MARKET_BONDS: 'Долговой рынок Московской Биржи',
               Market.MARKET_OPTIONS: 'Рынок опционов Московской Биржи'}  # Рынки

    def __init__(self, access_token):
        """Инициализация

        :param str access_token: Торговый токен доступа
        """
        self.metadata = [('x-api-key', access_token)]  # Торговый токен доступа
        self.channel = secure_channel(self.server, ssl_channel_credentials())  # Защищенный канал

        # Сервисы
        self.candles_stub = CandlesStub(self.channel)  # Сервис свечей
        self.orders_stub = OrdersStub(self.channel)  # Сервис заявок
        self.portfolios_stub = PortfoliosStub(self.channel)  # Сервис портфелей
        self.securities_stub = SecuritiesStub(self.channel)  # Сервис тикеров
        self.stops_stub = StopsStub(self.channel)  # Сервис стоп заявок

        # События
        self.on_order = self.default_handler  # Заявка
        self.on_trade = self.default_handler  # Сделка
        self.on_order_book = self.default_handler  # Стакан
        self.on_portfolio = self.default_handler  # Портфель
        self.on_response = self.default_handler  # Результат выполнения запроса

        self.subscription_queue: SimpleQueue[SubscriptionRequest] = SimpleQueue()  # Буфер команд на подписку/отписку
        self.subscriptions_thread = None  # Поток обработки подписок создадим позже

        self.symbols = self.get_securities()  # Получаем справочник тикеров (занимает несколько секунд)

    # Запросы

    def call_function(self, func, request):
        """Вызов функции"""
        try:  # Пытаемся
            response, call = func.with_call(request=request, metadata=self.metadata)  # вызвать функцию
            return response  # и вернуть ответ
        except RpcError:  # Если получили ошибку канала
            return None  # то возвращаем пустое значение

    # Candles

    def get_day_candles(self, security_board, security_code, time_frame, interval) -> GetDayCandlesResult:
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
        request = GetDayCandlesRequest(security_board=security_board, security_code=security_code,
                                       time_frame=time_frame, interval=interval)
        return self.call_function(self.candles_stub.GetDayCandles, request)

    def get_intraday_candles(self, security_board, security_code, time_frame, interval) -> GetIntradayCandlesResult:
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
        request = GetIntradayCandlesRequest(security_board=security_board, security_code=security_code,
                                            time_frame=time_frame, interval=interval)
        return self.call_function(self.candles_stub.GetIntradayCandles, request)

    # Events

    def subscribe_order_book(self, security_code, security_board, request_id=None) -> str:
        """Запрос подписки на стакан

        :param str security_code: Тикер инструмента
        :param str security_board: Режим торгов
        :param str request_id: Идентификатор запроса
        """
        if not request_id:  # Если идентификатор запроса не указан
            request_id = uuid4().hex[:16].upper()  # то создаем его из первых 16-и символов уникального идентификатора
        if not self.subscriptions_thread:  # Если еще нет потока обработки подписок
            self.subscriptions_thread = Thread(target=self.subscriptions_handler, name='SubscriptionsThread')  # Создаем поток обработки подписок
            self.subscriptions_thread.start()  # Запускаем поток
        request = SubscriptionRequest(order_book_subscribe_request=OrderBookSubscribeRequest(
            request_id=request_id, security_code=security_code, security_board=security_board))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку
        return request_id

    def unsubscribe_order_book(self, request_id, security_code, security_board):
        """Запрос на отписку от стакана

        :param str request_id: Идентификатор запроса
        :param str security_code: Тикер инструмента
        :param str security_board: Режим торгов
        """
        request = SubscriptionRequest(order_book_unsubscribe_request=OrderBookUnsubscribeRequest(
            request_id=request_id, security_code=security_code, security_board=security_board))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку

    def subscribe_order_trade(self, client_ids, include_trades=True, include_orders=True, request_id=None) -> str:
        """Запрос подписки на ордера и сделки

        :param list client_ids: Торговые коды счетов
        :param bool include_trades: Включить сделки в подписку
        :param bool include_orders: Включить заявки в подписку
        :param str request_id: Идентификатор запроса
        """
        if not request_id:  # Если идентификатор запроса не указан
            request_id = uuid4().hex[:16].upper()  # то создаем его из первых 16-и символов уникального идентификатора
        if not self.subscriptions_thread:  # Если еще нет потока обработки подписок
            self.subscriptions_thread = Thread(target=self.subscriptions_handler, name='SubscriptionsThread')  # Создаем поток обработки подписок
            self.subscriptions_thread.start()  # Запускаем поток
        request = SubscriptionRequest(order_trade_subscribe_request=OrderTradeSubscribeRequest(
            request_id=request_id, client_ids=client_ids, include_trades=include_trades, include_orders=include_orders))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку
        return request_id

    def unsubscribe_order_trade(self, request_id):
        """Отменить все предыдущие запросы на подписки на ордера и сделки

        :param str request_id: Идентификатор запроса
        """
        request = SubscriptionRequest(order_trade_unsubscribe_request=OrderTradeUnsubscribeRequest(
            request_id=request_id))
        self.subscription_queue.put(request)  # Отправляем в очередь на отправку

    # Orders

    def get_orders(self, client_id, include_matched=True, include_canceled=True, include_active=True) -> Union[GetOrdersResult, None]:
        """Возвращает список заявок

        :param str client_id: Идентификатор торгового счёта
        :param bool include_matched: Вернуть исполненные заявки
        :param bool include_canceled: Вернуть отмененные заявки
        :param bool include_active: Вернуть активные заявки
        """
        request = GetOrdersRequest(client_id=client_id, include_matched=include_matched, include_canceled=include_canceled, include_active=include_active)
        return self.call_function(self.orders_stub.GetOrders, request)

    def new_order(self, client_id, security_board, security_code, buy_sell: common, quantity, use_credit=False, price: float = None,
                  order_property: OrderProperty = OrderProperty.ORDER_PROPERTY_PUT_IN_QUEUE, condition: OrderCondition = None, valid_before: OrderValidBefore = None) -> Union[NewOrderResult, None]:
        """Создать новую заявку

        :param str client_id: Идентификатор торгового счёта
        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param common buy_sell: Направление сделки
            BUY_SELL_BUY - Покупка
            BUY_SELL_SELL - Продажа
        :param int quantity: Количество лотов инструмента для заявки
        :param bool use_credit: Использовать кредит. Недоступно для срочного рынка
        :param float price: Цена заявки. None для рыночной заявки
        :param OrderProperty order_property: Поведение заявки при выставлении в стакан
            ORDER_PROPERTY_PUT_IN_QUEUE - Неисполненная часть заявки помещается в очередь заявок Биржи
            ORDER_PROPERTY_CANCEL_BALANCE - (FOK) Неисполненная часть заявки снимается с торгов
            ORDER_PROPERTY_IMM_OR_CANCEL - (IOC) Сделки совершаются только в том случае, если заявка может быть удовлетворена полностью и сразу при выставлении
        :param OrderCondition condition: Типы условных ордеров
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
        :param OrderValidBefore valid_before: Условие по времени действия заявки
            type - Установка временных рамок действия заявки (OrderValidBeforeType)
                ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION - Заявка действует до конца сессии
                ORDER_VALID_BEFORE_TYPE_TILL_CANCELLED - Заявка действует, пока не будет отменена
                ORDER_VALID_BEFORE_TYPE_EXACT_TIME - Заявка действует до указанного времени. Параметр OrderValidBefore.time должно быть установлен
            time: Время действия заявки в UTC
        """
        if price:  # Если указана цена
            request = NewOrderRequest(client_id=client_id, security_board=security_board, security_code=security_code, buy_sell=buy_sell, quantity=quantity, price=DoubleValue(value=price),
                                      use_credit=use_credit, property=order_property, condition=condition, valid_before=valid_before)  # То выставляем лимитную заявку
        else:  # Если цена не указана
            request = NewOrderRequest(client_id=client_id, security_board=security_board, security_code=security_code, buy_sell=buy_sell, quantity=quantity,
                                      use_credit=use_credit, property=order_property, condition=condition, valid_before=valid_before)  # То выставляем рыночную заявку
        return self.call_function(self.orders_stub.NewOrder, request)

    def cancel_order(self, client_id, transaction_id) -> Union[CancelOrderResult, None]:
        """Отменяет заявку

        :param str client_id: Идентификатор торгового счёта
        :param int transaction_id: Идентификатор транзакции, который может быть использован для отмены заявки или определения номера заявки в сервисе событий
        """
        request = CancelOrderRequest(client_id=client_id, transaction_id=transaction_id)
        return self.call_function(self.orders_stub.CancelOrder, request)

        # Portfolios

    # Portfolios

    def get_portfolio(self, client_id, include_currencies=True, include_money=True, include_positions=True, include_max_buy_sell=True) -> Union[GetPortfolioResult, None]:
        """Возвращает портфель

        :param str client_id: Идентификатор торгового счёта
        :param bool include_currencies: Валютные позиции
        :param bool include_money: Денежные позиции
        :param bool include_positions: Позиции DEPO
        :param bool include_max_buy_sell: Лимиты покупки и продажи
        """
        request = GetPortfolioRequest(client_id=client_id, content=PortfolioContent(
            include_currencies=include_currencies,
            include_money=include_money,
            include_positions=include_positions,
            include_max_buy_sell=include_max_buy_sell))
        return self.call_function(self.portfolios_stub.GetPortfolio, request)

    # Securities

    def get_securities(self) -> Union[GetSecuritiesResult, None]:
        """Справочник инструментов"""
        request = GetSecuritiesRequest()
        return self.call_function(self.securities_stub.GetSecurities, request)

    # Stops

    def get_stops(self, client_id, include_executed=True, include_canceled=True, include_active=True) -> Union[GetStopsResult, None]:
        """Возвращает список стоп-заявок

        :param str client_id: Идентификатор торгового счёта
        :param bool include_executed: Вернуть исполненные стоп-заявки
        :param bool include_canceled: Вернуть отмененные стоп-заявки
        :param bool include_active: Вернуть активные стоп-заявки
        """
        request = GetStopsRequest(client_id=client_id, include_executed=include_executed, include_canceled=include_canceled, include_active=include_active)
        return self.call_function(self.stops_stub.GetStops, request)

    def new_stop(self, client_id, security_board, security_code, buy_sell: common,
                 stop_loss: StopLoss = None, take_profit: TakeProfit = None,
                 expiration_date: Timestamp = None, link_order=None, valid_before: common.OrderValidBefore = None) -> Union[NewStopResult, None]:
        """Выставляет стоп-заявку

        :param str client_id: Идентификатор торгового счёта
        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param common buy_sell: Направление сделки
            BUY_SELL_BUY - Покупка
            BUY_SELL_SELL - Продажа
        :param StopLoss stop_loss: Стоп лосс заявка
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
        :param TakeProfit take_profit: Тейк профит заявка
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
        :param common.OrderValidBefore valid_before: Время действия заявки
            type - Установка временных рамок действия заявки (OrderValidBeforeType)
                ORDER_VALID_BEFORE_TYPE_TILL_END_SESSION - Заявка действует до конца сессии
                ORDER_VALID_BEFORE_TYPE_TILL_CANCELLED - Заявка действует, пока не будет отменена
                ORDER_VALID_BEFORE_TYPE_EXACT_TIME - Заявка действует до указанного времени. Параметр OrderValidBefore.time должно быть установлен
            time: Время действия заявки в UTC
        """
        request = NewStopRequest(client_id=client_id, security_board=security_board, security_code=security_code, buy_sell=buy_sell,
                                 stop_loss=stop_loss, take_profit=take_profit,
                                 expiration_date=expiration_date, link_order=link_order, valid_before=valid_before)
        return self.call_function(self.stops_stub.NewStop, request)

    def cancel_stop(self, client_id, stop_id) -> Union[CancelStopResult, None]:
        """Снимает стоп-заявку

        :param str client_id: Идентификатор торгового счёта
        :param int stop_id: Идентификатор стоп-заявки
        """
        request = CancelStopRequest(client_id=client_id, stop_id=stop_id)
        return self.call_function(self.stops_stub.CancelStop, request)

    # Поток обработки подписок

    def default_handler(self, event: Union[OrderEvent, TradeEvent, OrderBookEvent, PortfolioEvent, ResponseEvent]):
        """Пустой обработчик события по умолчанию. Его можно заменить на пользовательский"""
        pass

    def request_iterator(self):
        """Генератор запросов на подписку/отписку"""
        while True:  # Будем пытаться читать из очереди до закрытии канала
            yield self.subscription_queue.get()  # Возврат из этой функции. При повторном ее вызове исполнение продолжится с этой строки

    def subscriptions_handler(self):
        """Поток обработки подписок"""
        events_stub = EventsStub(self.channel)  # Сервис событий (подписок)
        events = events_stub.GetEvents(request_iterator=self.request_iterator(), metadata=self.metadata)  # Получаем значения подписок
        try:
            for event in events:  # Пробегаемся по значениям подписок до закрытия канала
                e: Event = event  # Приводим пришедшее значение к подпискам
                if e.order != OrderEvent():  # Если пришло событие с заявкой
                    self.on_order(e.order)
                if e.trade != TradeEvent():  # Если пришло событие со сделкой
                    self.on_trade(e.trade)
                if e.order_book != OrderBookEvent():  # Если пришло событие стакана
                    self.on_order_book(e.order_book)
                if e.portfolio != PortfolioEvent():  # Если пришло событие портфеля
                    self.on_portfolio(e.portfolio)
                if e.response != ResponseEvent:  # Если пришло событие результата выполнения запроса
                    self.on_response(e.response)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            self.subscriptions_thread = None  # Сбрасываем поток обработки подписок. Запустим его снова на новой подписке

    # Выход и закрытие

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_channel()

    def __del__(self):
        self.close_channel()

    def close_channel(self):
        """Закрытие канала"""
        self.channel.close()

    # Функции конвертации

    def get_symbol_info(self, board, symbol) -> Union[Security, None]:
        """Получение информации тикера

        :param str board: Код площадки
        :param str symbol: Тикер
        :return: Значение из кэша или None, если тикер не найден
        """
        try:  # Пробуем
            return next(item for item in self.symbols.securities if item.board == board and item.code == symbol)  # вернуть значение из справочника
        except StopIteration:  # Если тикер не найден
            print(f'Информация о {board}.{symbol} не найдена')
            return None  # то возвращаем пустое значение

    def dataname_to_board_symbol(self, dataname) -> tuple[str, str]:
        """код площадки и тикера из названия тикера

        :param str dataname: Название тикера
        :return: Код площадки и тикер
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код площадки>.<Код тикера>
            board = symbol_parts[0]  # Код площадки
            symbol = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без площадки
            symbol = dataname  # Код тикера
            try:  # Пробуем по тикеру получить площадку
                board = next(item.board for item in self.symbols.securities if item.code == symbol)  # Получаем код площадки первого совпадающего тикера
            except StopIteration:  # Если площадка не найдена
                board = None  # то возвращаем пустое значение
        return board, symbol  # Возвращаем код площадки и код тикера

    @staticmethod
    def board_symbol_to_dataname(board, symbol) -> str:
        """Название тикера из кода площадки и тикера

        :param str board: Код площадки
        :param str symbol: Тикер
        :return: Название тикера
        """
        return f'{board}.{symbol}'

    def price_to_finam_price(self, board, symbol, price) -> float:
        """Перевод цены в цену Финам

        :param str board: Код площадки
        :param str symbol: Тикер
        :param float price: Цена
        :return: Цена в Финам
        """
        si = self.get_symbol_info(board, symbol)  # Информация о тикере
        if board == 'TQOB':  # Для рынка облигаций
            price /= 10  # цену делим на 10
        return round(price, si.decimals)  # Округляем цену

    def finam_price_to_price(self, board, symbol, price) -> float:
        """Перевод цены Финам в цену

        :param str board: Код площадки
        :param str symbol: Тикер
        :param float price: Цена в Финам
        :return: Цена
        """
        si = self.get_symbol_info(board, symbol)  # Информация о тикере
        if board == 'TQOB':  # Для рынка облигаций
            price *= 10  # цену умножаем на 10
        return price

    def utc_to_msk_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из UTC в московское

        :param datetime dt: Время UTC
        :param bool tzinfo: Отображать временнУю зону
        :return: Московское время
        """
        dt_utc = utc.localize(dt)  # Задаем временнУю зону UTC
        dt_msk = dt_utc.astimezone(self.tz_msk)  # Переводим в МСК
        return dt_msk if tzinfo else dt_msk.replace(tzinfo=None)

    def msk_to_utc_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из московского в UTC

        :param datetime dt: Московское время
        :param bool tzinfo: Отображать временнУю зону
        :return: Время UTC
        """
        dt_msk = self.tz_msk.localize(dt)  # Задаем временнУю зону МСК
        dt_utc = dt_msk.astimezone(pytz.utc)  # Переводим в UTC
        return dt_utc if tzinfo else dt_utc.replace(tzinfo=None)
