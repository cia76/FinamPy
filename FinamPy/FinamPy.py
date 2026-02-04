import logging  # Будем вести лог
from datetime import datetime, timedelta, timezone
from time import sleep
from zoneinfo import ZoneInfo  # ВременнАя зона
from typing import Any  # Любой тип
from queue import SimpleQueue  # Очередь подписок/отписок

import keyring  # Безопасное хранение торгового токена
import keyring.errors  # Ошибки хранилища
from grpc import ssl_channel_credentials, secure_channel, RpcError, StatusCode  # Защищенный канал

# Структуры
from FinamPy.grpc.auth import auth_service_pb2 as auth_service  # Подключение
from FinamPy.grpc.assets import assets_service_pb2 as assets_service  # Информация о биржах и тикерах
from FinamPy.grpc.marketdata import marketdata_service_pb2 as marketdata_service  # Рыночные данные
from FinamPy.grpc.orders import orders_service_pb2 as orders_service  # Заявки

# gRPC - Сервисы
from FinamPy.grpc.auth.auth_service_pb2_grpc import AuthServiceStub  # Подлключение https://tradeapi.finam.ru/docs/guides/grpc/auth_service
from FinamPy.grpc.accounts.accounts_service_pb2_grpc import AccountsServiceStub  # Счета https://tradeapi.finam.ru/docs/guides/grpc/accounts_service/
from FinamPy.grpc.assets.assets_service_pb2_grpc import AssetsServiceStub  # Инструменты https://tradeapi.finam.ru/docs/guides/grpc/assets_service/
from FinamPy.grpc.orders.orders_service_pb2_grpc import OrdersServiceStub  # Заявки https://tradeapi.finam.ru/docs/guides/grpc/orders_service/
from FinamPy.grpc.marketdata.marketdata_service_pb2_grpc import MarketDataServiceStub  # Рыночные данные https://tradeapi.finam.ru/docs/guides/grpc/marketdata_service/


class FinamPy:
    """Работа с Finam Trade API gRPC https://tradeapi.finam.ru из Python"""
    tz_msk = ZoneInfo('Europe/Moscow')  # Время UTC будем приводить к московскому времени
    min_history_date = datetime(2015, 6, 29)  # Первая дата, с которой можно получать историю
    server = 'api.finam.ru:443'  # Сервер для исполнения вызовов
    jwt_token_ttl = 15 * 60  # Время жизни токена JWT 15 минут в секундах
    logger = logging.getLogger('FinamPy')  # Будем вести лог
    metadata: tuple[str, str]  # Токен JWT в запросах

    def __init__(self, access_token=None):
        """Инициализация

        :param str access_token: Торговый токен
        """
        self.channel = secure_channel(self.server, ssl_channel_credentials())  # Защищенный канал
        self.order_trade_queue: SimpleQueue[orders_service.OrderTradeRequest] = SimpleQueue()  # Буфер команд заявок/сделок

        # Сервисы
        self.auth_stub = AuthServiceStub(self.channel)
        self.accounts_stub = AccountsServiceStub(self.channel)
        self.assets_stub = AssetsServiceStub(self.channel)
        self.orders_stub = OrdersServiceStub(self.channel)
        self.marketdata_stub = MarketDataServiceStub(self.channel)

        # События
        self.on_quote = Event()  # Котировка по инструменту
        self.on_order_book = Event()  # Стакан по инструменту
        self.on_latest_trades = Event()  # Обезличенные сделки по инструменту
        self.on_new_bar = Event()  # Свечи по инструменту и временнОму интервалу
        self.on_order = Event()  # Свои заявки
        self.on_trade = Event()  # Свои сделки

        if access_token is None:  # Если торговый токен не указан
            self.access_token = self.get_long_token_from_keyring('FinamPy', 'access_token')  # то получаем его из защищенного хранилища по частям
        else:  # Если указан торговый токен
            self.access_token = access_token  # Торговый токен
            self.set_long_token_to_keyring('FinamPy', 'access_token', self.access_token)  # Сохраняем его в защищенное хранилище

        self.jwt_token = ''  # Токен JWT
        self.jwt_token_issued = 0  # UNIX время в секундах выдачи токена JWT
        self.auth()  # Получаем токен JWT
        self.account_ids = list(self.token_details().account_ids)  # Из инфрмации о токене получаем список счетов

        self.exchanges = None  # Список всех бирж
        self.assets = None  # Справочник всех доступных инструментов
        self.symbols = {}  # Справочник тикеров
        self.subscriptions = {}  # Список подписок на свои заявки и сделки

    # Подключение

    def auth(self) -> None:
        """Получение JWT токена из API токена"""
        now = int(datetime.timestamp(datetime.now()))  # Текущая дата и время в виде UNIX времени в секундах
        if not self.jwt_token or now - self.jwt_token_issued > self.jwt_token_ttl:  # Если токен JWT не был выдан или был просрочен
            response: auth_service.AuthResponse
            response, _ = self.auth_stub.Auth.with_call(request=auth_service.AuthRequest(secret=self.access_token))
            self.jwt_token = response.token  # Токен JWT
            self.jwt_token_issued = now  # Дата выдачи токена JWT
            self.metadata = ('authorization', self.jwt_token)  # Токен JWT в запросах

    def token_details(self) -> auth_service.TokenDetailsResponse:
        """Получение информации о токене сессии"""
        self.auth()  # Получаем токен JWT
        response: auth_service.TokenDetailsResponse
        response, _ = self.auth_stub.TokenDetails.with_call(request=auth_service.TokenDetailsRequest(token=self.jwt_token))
        return response

    # Запросы

    def call_function(self, func, request):
        """Вызов функции"""
        self.auth()  # Получаем токен JWT
        # noinspection PyProtectedMember
        func_name = func._method.decode('utf-8')  # Название функции
        self.logger.debug(f'Запрос : {func_name}({request})')
        while True:  # Пока не получим ответ или ошибку
            try:  # Пытаемся
                response, call = func.with_call(request=request, metadata=(self.metadata,))  # вызвать функцию
                self.logger.debug(f'Ответ  : {response}')
                return response  # и вернуть ответ
            except RpcError as ex:  # Если получили ошибку канала
                details = ex.args[0].details  # Сообщение об ошибке
                if 'GetAsset' not in func_name:  # При переводе канонического названия тикера в вид Финама приходится подбирать биржу. Поэтому, ошибки ф-ии GetAsset игнорируем
                    self.logger.error(f'Ошибка {details} при вызове функции {func_name}({request})')
                return None  # Возвращаем пустое значение

    # Подписки

    def subscribe_quote_thread(self, symbols):
        """Подписка на котировки по инструменту"""
        while True:  # Пока мы не закрыли канал
            try:
                stream = self.marketdata_stub.SubscribeQuote(request=marketdata_service.SubscribeQuoteRequest(symbols=symbols), metadata=(self.metadata,))  # Поток подписки
                while True:  # Пока можем получать данные из потока
                    event: marketdata_service.SubscribeQuoteResponse = next(stream)  # Читаем событие из потока подписки
                    self.on_quote.trigger(event)  # Вызываем событие
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def subscribe_order_book_thread(self, symbol):
        """Подписка на стакан по инструменту"""
        while True:  # Пока мы не закрыли канал
            try:
                stream = self.marketdata_stub.SubscribeOrderBook(request=marketdata_service.SubscribeOrderBookRequest(symbol=symbol), metadata=(self.metadata,))  # Поток подписки
                while True:  # Пока можем получать данные из потока
                    event: marketdata_service.SubscribeOrderBookResponse = next(stream)  # Читаем событие из потока подписки
                    self.on_order_book.trigger(event)  # Вызываем событие
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def subscribe_latest_trades_thread(self, symbol):
        """Подписка на сделки по инструменту"""
        while True:  # Пока мы не закрыли канал
            try:
                stream = self.marketdata_stub.SubscribeLatestTrades(request=marketdata_service.SubscribeLatestTradesRequest(symbol=symbol), metadata=(self.metadata,))  # Поток подписки
                while True:  # Пока можем получать данные из потока
                    event: marketdata_service.SubscribeLatestTradesResponse = next(stream)  # Читаем событие из потока подписки
                    self.on_latest_trades.trigger(event)  # Вызываем событие
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def subscribe_bars_thread(self, symbol, finam_timeframe: marketdata_service.TimeFrame.ValueType):
        """Подписка на свечи по инструменту и временнОму интервалу"""
        while True:  # Пока мы не закрыли канал
            try:
                stream = self.marketdata_stub.SubscribeBars(request=marketdata_service.SubscribeBarsRequest(symbol=symbol, timeframe=finam_timeframe), metadata=(self.metadata,))  # Поток подписки
                while True:  # Пока можем получать данные из потока
                    event: marketdata_service.SubscribeBarsResponse = next(stream)  # Читаем событие из потока подписки
                    self.on_new_bar.trigger(event, finam_timeframe)  # Вызываем событие
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def subscribe_orders_thread(self, account_id=None):
        """Подписка на свои заявки

        param str account_id: Номер счета
        """
        if account_id is None:  # Если не указан счет
            account_id = self.account_ids[0]  # то берем первый из списка
        while True:  # Пока мы не закрыли канал
            try:
                stream = self.orders_stub.SubscribeOrders(request=orders_service.SubscribeOrdersRequest(account_id=account_id), metadata=(self.metadata,))  # Поток подписки
                while True:  # Пока можем получать данные из потока
                    event: orders_service.SubscribeOrdersResponse = next(stream)  # Читаем событие из потока подписки
                    for order in event.orders:  # Пробегаемся по всем пришедшим заявкам
                        self.on_order.trigger(order)
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def subscribe_trades_thread(self, account_id=None):
        """Подписка на свои сделки

        param str account_id: Номер счета
        """
        if account_id is None:  # Если не указан счет
            account_id = self.account_ids[0]  # то берем первый из списка
        while True:  # Пока мы не закрыли канал
            try:
                stream = self.orders_stub.SubscribeTrades(request=orders_service.SubscribeTradesRequest(account_id=account_id), metadata=(self.metadata,))  # Поток подписки
                while True:  # Пока можем получать данные из потока
                    event: orders_service.SubscribeTradesResponse = next(stream)  # Читаем событие из потока подписки
                    for trade in event.trades:  # Пробегаемся по всем пришедшим сделкам
                        self.on_trade.trigger(trade)
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def subscribe_orders_trades_thread(self):
        """Подписка на свои заявки и сделки для совместимости. В будущих версиях будет удалена Финамом"""
        while True:  # Пока мы не закрыли канал
            try:
                for account_id, (orders, trades) in self.subscriptions.items():  # Для каждого счета
                    self.subscribe_orders_trades(orders=orders, trades=trades, account_id=account_id)  # Восстанавливаем подписку
                stream = self.orders_stub.SubscribeOrderTrade(request_iterator=self._request_order_trade_iterator(), metadata=(self.metadata,))  # Двунаправленный поток подписки
                while True:  # Пока можем получать данные из потока
                    event: orders_service.OrderTradeResponse = next(stream)  # Читаем событие из потока подписки
                    if event.orders:  # Если пришли заявки
                        for order in event.orders:
                            self.on_order.trigger(order)
                    if event.trades:  # Если пришли сделки
                        for t in event.trades:
                            self.on_trade.trigger(t)
            except ValueError:  # Если канал уже закрыт (Cannot invoke RPC: Channel closed!)
                break  # то выходим из потока, дальше не продолжаем
            except RpcError as rpc_error:
                if rpc_error.code() == StatusCode.CANCELLED:  # Если закрываем канал (grpc._channel._MultiThreadedRendezvous)
                    break  # то выходим из потока, дальше не продолжаем
                else:  # При другой ошибке
                    sleep(5)  # попытаемся переподключиться через 5 секунд

    def _request_order_trade_iterator(self):
        """Генератор запросов на подписку/отписку своих заявок и сделок"""
        while True:  # Будем пытаться читать из очереди до закрытия канала
            yield self.order_trade_queue.get()  # Возврат из этой функции. При повторном ее вызове исполнение продолжится с этой строки

    def subscribe_orders_trades(self, orders=True, trades=True, account_id=None):
        if account_id is None:  # Если не указан счет
            account_id = self.account_ids[0]  # то берем первый из списка
        if orders and trades:  # Если подписываемся на заявки и сделки
            data_type_subscribe = orders_service.OrderTradeRequest.DataType.DATA_TYPE_ALL
            data_type_unsubscribe = None
        elif orders:  # Если подписываемся на заявки
            data_type_subscribe = orders_service.OrderTradeRequest.DataType.DATA_TYPE_ORDERS
            data_type_unsubscribe = orders_service.OrderTradeRequest.DataType.DATA_TYPE_TRADES
        elif trades:  # Если подписываемся на сделки
            data_type_subscribe = orders_service.OrderTradeRequest.DataType.DATA_TYPE_TRADES
            data_type_unsubscribe = orders_service.OrderTradeRequest.DataType.DATA_TYPE_ORDERS
        else:  # Если не подписываемся
            data_type_subscribe = None
            data_type_unsubscribe = orders_service.OrderTradeRequest.DataType.DATA_TYPE_ALL
        if data_type_subscribe is not None:  # Если подписываемся
            self.order_trade_queue.put(orders_service.OrderTradeRequest(  # Ставим в буфер команд/сделок
                action=orders_service.OrderTradeRequest.Action.ACTION_SUBSCRIBE,  # Подписываемся
                data_type=data_type_subscribe,  # на свои заявки/сделки
                account_id=account_id))  # по торговому счету
        if data_type_unsubscribe is not None:  # Если отменяем подписку
            self.order_trade_queue.put(orders_service.OrderTradeRequest(  # Ставим в буфер команд/сделок
                action=orders_service.OrderTradeRequest.Action.ACTION_UNSUBSCRIBE,  # Отменяем подписку
                data_type=data_type_unsubscribe,  # на свои заявки/сделки
                account_id=account_id))  # по торговому счету
        self.subscriptions[account_id] = (orders, trades)  # Запоминаем подписку

    # Выход и закрытие

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_channel()

    def __del__(self):
        self.close_channel()

    def close_channel(self):
        """Закрытие канала"""
        if self.channel is not None:  # Если канал открыт
            self.channel.close()  # то закрываем канал
            self.channel = None  # Помечаем канал как закрытый

    # Функции конвертации

    @staticmethod
    def finam_board_to_board(finam_board: str) -> str:
        """Канонический код режима торгов из кода режима торгов Финама

        :param str finam_board: Код режима торгов Финама
        :return: Канонический код режима торгов
        :rtype: str
        """
        board_map = {
            'FUT': 'SPBFUT',  # Фьючерсы
            'OPT': 'SPBOPT',  # Опционы
        }
        return board_map.get(finam_board, finam_board)

    @staticmethod
    def board_to_finam_board(board: str) -> str:
        """Код режима торгов Финама из канонического кода режима торгов

        :param str board: Канонический код режима торгов
        :return: Код режима торгов Финама
        :rtype: str
        """
        finam_board_map = {
            'SPBFUT': 'FUT',  # Фьючерсы
            'SPBOPT': 'OPT',  # Опционы
        }
        return finam_board_map.get(board, board)

    def dataname_to_finam_board_ticker(self, dataname) -> tuple[str | None, str]:
        """Код режима торгов Финама и тикер из названия тикера

        :param str dataname: Название тикера
        :return: Код режима торгов и тикер
        """
        symbol_parts = dataname.split('.')  # По разделителю пытаемся разбить тикер на части
        if len(symbol_parts) >= 2:  # Если тикер задан в формате <Код режима торгов>.<Код тикера>
            finam_board = self.board_to_finam_board(symbol_parts[0])  # Код режима торгов
            ticker = '.'.join(symbol_parts[1:])  # Код тикера
        else:  # Если тикер задан без кода режима торгов
            ticker = dataname  # Код тикера
            if self.assets is None:  # Если нет справочника инструментов
                self.assets: assets_service.AssetsResponse = self.call_function(self.assets_stub.Assets, assets_service.AssetsRequest())  # то получаем его из Финама
            mic = next((asset.mic for asset in self.assets.assets if asset.ticker == ticker), None)  # Биржа тикера из справочника
            if mic is None:  # Если биржа не найдена
                return None, ticker  # то возвращаем без кода режима торгов
            si: assets_service.GetAssetResponse = self.call_function(self.assets_stub.GetAsset, assets_service.GetAssetRequest(symbol=f'{ticker}@{mic}', account_id=self.account_ids[0]))
            finam_board = si.board  # Код режима торгов
        return finam_board, ticker

    def finam_board_ticker_to_dataname(self, finam_board, ticker) -> str:
        """Название тикера из кода режима торгов Финама и тикера

        :param str finam_board: Код режима торгов Финама
        :param str ticker: Тикер
        :return: Название тикера
        """
        return f'{self.finam_board_to_board(finam_board)}.{ticker}'

    def get_mic(self, finam_board, ticker):
        """Биржа тикера по ISO 10383 Market Identifier Codes из кода режима торгов Финама и тикера

        :param str finam_board: Код режима торгов
        :param str ticker: Тикер
        :return: Код биржи по ISO 10383 Market Identifier Codes. MISX - МосБиржа - все основные рынки, RTSX - МосБиржа - рынок деривативов
        """
        if self.exchanges is None:  # Если нет списка всех бирж
            self.exchanges: assets_service.ExchangesResponse = self.call_function(self.assets_stub.Exchanges, assets_service.ExchangesRequest())  # то получаем список всех бирж
        for exchange in self.exchanges.exchanges:  # Пробегаемся по всем биржам
            si = self.get_symbol_info(ticker, exchange.mic)
            if si and si.board == finam_board:  # Если информация о тикере найдена, и режим торгов есть на бирже
                return exchange.mic  # то биржа найдена
        return None  # Если биржа не была найдена, то возвращаем пустое значение

    def get_symbol_info(self, ticker, mic, reload=False) -> assets_service.GetAssetResponse | None:
        """Спецификация тикера

        :param str ticker: Тикер
        :param str mic: Код биржи по ISO 10383 Market Identifier Codes. MISX - МосБиржа - все основные рынки, RTSX - МосБиржа - рынок деривативов
        :param bool reload: Получить информацию из Финам
        :return: Спецификация тикера из кэша/Финам или None, если тикер не найден
        """
        if reload or (ticker, mic) not in self.symbols:  # Если нужно получить информацию из Финам или нет информации о тикере в справочнике
            si = self.call_function(self.assets_stub.GetAsset, assets_service.GetAssetRequest(symbol=f'{ticker}@{mic}', account_id=self.account_ids[0]))  # Получаем информацию о тикере из Финам
            if si is None:  # Если тикер не найден
                return None  # то возвращаем пустое значение
            self.symbols[(ticker, mic)] = si  # Заносим информацию о тикере в справочник
        return self.symbols[(ticker, mic)]  # Возвращаем значение из справочника

    @staticmethod
    def timeframe_to_finam_timeframe(tf: str) -> tuple[marketdata_service.TimeFrame.ValueType, timedelta, bool]:
        """Перевод временнОго интервала во временной интервал Финама

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал Финама, максимальный размер запроса в днях, внутридневной бар
        """
        tf_map = {
            'M1': (marketdata_service.TimeFrame.TIME_FRAME_M1, timedelta(days=7), True),
            'M5': (marketdata_service.TimeFrame.TIME_FRAME_M5, timedelta(days=30), True),
            'M15': (marketdata_service.TimeFrame.TIME_FRAME_M15, timedelta(days=30), True),
            'M30': (marketdata_service.TimeFrame.TIME_FRAME_M30, timedelta(days=30), True),
            'M60': (marketdata_service.TimeFrame.TIME_FRAME_H1, timedelta(days=30), True),
            'M120': (marketdata_service.TimeFrame.TIME_FRAME_H2, timedelta(days=30), True),
            'M240': (marketdata_service.TimeFrame.TIME_FRAME_H4, timedelta(days=30), True),
            'M480': (marketdata_service.TimeFrame.TIME_FRAME_H8, timedelta(days=30), True),
            'D1': (marketdata_service.TimeFrame.TIME_FRAME_D, timedelta(days=365), False),
            'W1': (marketdata_service.TimeFrame.TIME_FRAME_W, timedelta(days=365 * 5), False),
            'MN1': (marketdata_service.TimeFrame.TIME_FRAME_MN, timedelta(days=365 * 5), False),
            'MN3': (marketdata_service.TimeFrame.TIME_FRAME_QR, timedelta(days=365 * 5), False),
        }  # Справочник временнЫх интервалов
        if tf in tf_map:  # Если временной интервал есть в справочнике
            return tf_map[tf]  # то возвращаем временной интервал Финама
        raise NotImplementedError(f'Временной интервал {tf} не поддерживается')  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def finam_timeframe_to_timeframe(finam_tf) -> tuple[str, timedelta, bool]:
        """Перевод временнОго интервала Финама во временной интервал

        :param marketdata_service.TimeFrame.ValueType finam_tf: Временной интервал Финама
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм, максимальный размер запроса в днях, внутридневной бар
        """
        finam_tf_map = {
            marketdata_service.TimeFrame.TIME_FRAME_M1: ('M1', timedelta(days=7), True),
            marketdata_service.TimeFrame.TIME_FRAME_M5: ('M5', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_M15: ('M15', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_M30: ('M30', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_H1: ('M60', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_H2: ('M120', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_H4: ('M240', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_H8: ('M480', timedelta(days=30), True),
            marketdata_service.TimeFrame.TIME_FRAME_D: ('D1', timedelta(days=365), False),
            marketdata_service.TimeFrame.TIME_FRAME_W: ('W1', timedelta(days=365 * 5), False),
            marketdata_service.TimeFrame.TIME_FRAME_MN: ('MN1', timedelta(days=365 * 5), False),
            marketdata_service.TimeFrame.TIME_FRAME_QR: ('MN3', timedelta(days=365 * 5), False),
        }  # Справочник временнЫх интервалов Финама
        if finam_tf in finam_tf_map:  # Если временной интервал Финама есть в справочнике
            return finam_tf_map[finam_tf]  # то возвращаем временной интервал
        raise NotImplementedError(f'Временной интервал Финама {finam_tf} не поддерживается')  # С остальными временнЫми интервалами Финама не работаем

    def price_to_finam_price(self, ticker, mic, price) -> int | float:
        """Перевод цены в рублях за штуку в цену Финам

        :param str ticker: Тикер
        :param str mic: Код биржи по ISO 10383 Market Identifier Codes. MISX - МосБиржа - все основные рынки, RTSX - МосБиржа - рынок деривативов
        :param float price: Цена в рублях за штуку
        :return: Цена в Финам
        """
        si = self.get_symbol_info(ticker, mic)  # Спецификация тикера
        board = si.board  # Режим торгов
        if board in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            finam_price = price / 10  # Цена -> % от номинала облигации (* 100 / 1000 = / 10)
        elif board == 'FUT':  # Для рынка фьючерсов
            # TODO: Выделить фьючерсы на сырье. У них тоже лот = 1
            lot_size = 1 if si.expiration_date.year == 0 else int(float(si.lot_size.value))  # Рамер лота в штуках. Для вечных фьючерсов (нет даты экспирации) не используется
            finam_price = price * lot_size
        elif board == 'CETS':  # Для валют
            finam_price = price
        else:  # Для акций
            finam_price = price
        decimals = si.decimals  # Кол-во десятичных знаков
        min_price_step = si.min_step / (10 ** si.decimals)  # Шаг цены
        finam_price = round(finam_price // min_price_step * min_price_step, decimals)  # Проверяем цену в Алор на корректность. Округляем по кол-ву десятичных знаков тикера
        return int(finam_price) if finam_price.is_integer() else finam_price

    def finam_price_to_price(self, ticker, mic, finam_price) -> float:
        """Перевод цены Финам в цену в рублях за штуку

        :param str ticker: Тикер
        :param str mic: Код биржи по ISO 10383 Market Identifier Codes. MISX - МосБиржа - все основные рынки, RTSX - МосБиржа - рынок деривативов
        :param float finam_price: Цена в Финам
        :return: Цена в рублях за штуку
        """
        si = self.get_symbol_info(ticker, mic)  # Спецификация тикера
        decimals = si.decimals  # Кол-во десятичных знаков
        min_price_step = si.min_step / (10 ** si.decimals)  # Шаг цены
        finam_price = round(finam_price // min_price_step * min_price_step, decimals)  # Проверяем цену в Алор на корректность. Округляем по кол-ву десятичных знаков тикера
        board = si.board  # Режим торгов
        if board in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            price = finam_price * 10  # % от номинала облигации -> Цена (/ 100 * 1000 = * 10)
        elif board == 'FUT':  # Для рынка фьючерсов
            # TODO: Выделить фьючерсы на сырье. У них тоже лот = 1
            lot_size = 1 if si.expiration_date.year == 0 else int(float(si.lot_size.value))  # Рамер лота в штуках. Для вечных фьючерсов (нет даты экспирации) не используется
            price = finam_price / lot_size
        elif board == 'CETS':  # Для валют
            price = finam_price
        else:  # Для акций
            price = finam_price
        return price  # Цена в рублях за штуку

    def msk_datetime_to_timestamp(self, dt) -> int:
        """Перевод московского времени в кол-во секунд, прошедших с 01.01.1970 00:00 UTC

        :param datetime dt: Московское время
        :return: Кол-во секунд, прошедших с 01.01.1970 00:00 UTC
        :rtype: int
        """
        dt_msk = dt.replace(tzinfo=self.tz_msk)  # Заданное время ставим в зону МСК
        return int(dt_msk.timestamp())  # Переводим в кол-во секунд, прошедших с 01.01.1970 в UTC

    def timestamp_to_msk_datetime(self, seconds) -> datetime:
        """Перевод кол-ва секунд, прошедших с 01.01.1970 00:00 UTC, в московское время

        :param int seconds: Кол-во секунд, прошедших с 01.01.1970 00:00 UTC
        :return: Московское время без временнОй зоны
        :rtype: datetime
        """
        dt_utc = datetime.fromtimestamp(seconds, timezone.utc)  # Переводим кол-во секунд, прошедших с 01.01.1970 в UTC
        return dt_utc.astimezone(self.tz_msk).replace(tzinfo=None)  # Заданное время ставим в зону МСК. Убираем временнУю зону

    def msk_to_utc_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из московского в UTC

        :param datetime dt: Московское время
        :param bool tzinfo: Отображать временнУю зону
        :return: Время UTC
        :rtype: datetime
        """
        dt_msk = dt.replace(tzinfo=self.tz_msk)  # Заданное время ставим в зону МСК
        dt_utc = dt_msk.astimezone(timezone.utc)  # Переводим в зону UTC
        return dt_utc if tzinfo else dt_utc.replace(tzinfo=None)

    def utc_to_msk_datetime(self, dt, tzinfo=False) -> datetime:
        """Перевод времени из UTC в московское

        :param datetime dt: Время UTC
        :param bool tzinfo: Отображать временнУю зону
        :return: Московское время
        :rtype: datetime
        """
        dt_utc = dt.replace(tzinfo=timezone.utc)  # Заданное время ставим в зону UTC
        dt_msk = dt_utc.astimezone(self.tz_msk)  # Переводим в зону МСК
        return dt_msk if tzinfo else dt_msk.replace(tzinfo=None)

    def get_long_token_from_keyring(self, service: str, username: str) -> str | None:
        """Получение токена из системного хранилища keyring по частям"""
        try:
            index = 0  # Номер части токена
            token_parts = []  # Части токена
            while True:  # Пока есть части токена
                token_part = keyring.get_password(service, f'{username}{index}')  # Получаем часть токена
                if token_part is None:  # Если части токена нет
                    break  # то выходим, дальше не продолжаем
                token_parts.append(token_part)  # Добавляем часть токена
                index += 1  # Переходим к следующей части токена
            if not token_parts:  # Если токен не найден
                self.logger.error(f'Токен не найден в системном хранилище. Вызовите fp_provider = FinamPy("<Токен>")')
                return None
            token = ''.join(token_parts)  # Собираем токен из частей
            self.logger.debug('Токен успешно загружен из системного хранилища')
            return token
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка доступа к системному хранилищу: {e}')
        except Exception as e:
            self.logger.fatal(f'Ошибка при загрузке токена: {e}')

    def set_long_token_to_keyring(self, service: str, username: str, token: str, password_split_size: int = 500) -> None:
        """Установка токена в системное хранилище keyring по частям"""
        try:
            self.clear_long_token_from_keyring(service, username)  # Очищаем предыдущие части токена
            token_parts = [token[i:i + password_split_size] for i in range(0, len(token), password_split_size)]  # Разбиваем токен на части заданного размера
            for index, token_part in enumerate(token_parts):  # Пробегаемся по частям токена
                keyring.set_password(service, f'{username}{index}', token_part)  # Сохраняем часть токена
            self.logger.debug(f'Частей сохраненного токена в хранилище: {len(token_parts)}')
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка сохранения в системное хранилище: {e}')
        except Exception as e:
            self.logger.fatal(f'Ошибка при сохранении токена: {e}')

    def clear_long_token_from_keyring(self, service: str, username: str) -> None:
        """Удаление всех частей токена из системного хранилища keyring"""
        try:
            index = 0  # Номер части токена
            while True:  # Пока есть части токена
                if keyring.get_password(service, f'{username}{index}') is None:  # Если части токена нет
                    break  # то выходим, дальше не продолжаем
                keyring.delete_password(service, f'{username}{index}')  # Удаляем часть токена
                index += 1  # Переходим к следующей части токена
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка доступа к системному хранилищу: {e}')


class Event:
    """Событие с подпиской / отменой подписки"""
    def __init__(self):
        self._callbacks: set[Any] = set()  # Избегаем дубликатов функций при помощи set

    def subscribe(self, callback) -> None:
        """Подписаться на событие"""
        self._callbacks.add(callback)  # Добавляем функцию в список

    def unsubscribe(self, callback) -> None:
        """Отписаться от события"""
        self._callbacks.discard(callback)  # Удаляем функцию из списка. Если функции нет в списке, то не будет ошибки

    def trigger(self, *args, **kwargs) -> None:
        """Вызвать событие"""
        for callback in list(self._callbacks):  # Пробегаемся по копии списка, чтобы избежать исключения при удалении
            callback(*args, **kwargs)  # Вызываем функцию
