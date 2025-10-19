from datetime import datetime, timedelta
import logging  # Будем вести лог
import os
import pickle  # Хранение торгового токена
from queue import SimpleQueue  # Очередь подписок/отписок

from pytz import timezone, utc  # Работаем с временнОй зоной и UTC
from grpc import ssl_channel_credentials, secure_channel, RpcError  # Защищенный канал

# Структуры
from .grpc.auth import auth_service_pb2 as auth_service  # Подключение https://tradeapi.finam.ru/docs/guides/grpc/auth_service
from .grpc.assets.assets_service_pb2 import ExchangesRequest, ExchangesResponse, AssetsRequest, AssetsResponse, GetAssetResponse, GetAssetRequest  # Информация о биржах и тикерах
from .grpc.marketdata import marketdata_service_pb2 as marketdata_service  # Рыночные данные https://tradeapi.finam.ru/docs/guides/grpc/marketdata_service/
from .grpc.orders import orders_service_pb2 as orders_service  # Заявки https://tradeapi.finam.ru/docs/guides/grpc/orders_service/
from .grpc import trade_pb2 as trade  # Информация о сделке

# gRPC - Сервисы
from .grpc.auth.auth_service_pb2_grpc import AuthServiceStub  # Подлключение https://tradeapi.finam.ru/docs/guides/grpc/auth_service
from .grpc.accounts.accounts_service_pb2_grpc import AccountsServiceStub  # Счета https://tradeapi.finam.ru/docs/guides/grpc/accounts_service/
from .grpc.assets.assets_service_pb2_grpc import AssetsServiceStub  # Инструменты https://tradeapi.finam.ru/docs/guides/grpc/assets_service/
from .grpc.orders.orders_service_pb2_grpc import OrdersServiceStub  # Заявки https://tradeapi.finam.ru/docs/guides/grpc/orders_service/
from .grpc.marketdata.marketdata_service_pb2_grpc import MarketDataServiceStub  # Рыночные данные https://tradeapi.finam.ru/docs/guides/grpc/marketdata_service/


class FinamPy:
    """Работа с Finam Trade API gRPC https://tradeapi.finam.ru из Python"""
    tz_msk = timezone('Europe/Moscow')  # Время UTC будем приводить к московскому времени
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

        # Сервисы
        self.auth_stub = AuthServiceStub(self.channel)
        self.accounts_stub = AccountsServiceStub(self.channel)
        self.assets_stub = AssetsServiceStub(self.channel)
        self.orders_stub = OrdersServiceStub(self.channel)
        self.marketdata_stub = MarketDataServiceStub(self.channel)

        # События
        self.on_quote = self.default_handler  # Котировка по инструменту
        self.on_order_book = self.default_handler  # Стакан по инструменту
        self.on_latest_trades = self.default_handler  # Обезличенные сделки по инструменту
        self.on_new_bar = self.default_bars_handler  # Свечи по инструменту и временнОму интервалу
        self.order_trade_queue: SimpleQueue[orders_service.OrderTradeRequest] = SimpleQueue()  # Буфер команд заявок/сделок
        self.on_order = self.default_handler  # Свои заявки
        self.on_trade = self.default_handler  # Свои сделки

        config_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.pkl')  # Полный путь к файлу конфигурации
        if access_token is None:  # Если торговый токен не указан
            try:
                with open(config_filename, 'rb') as file:  # Пытаемся открыть файл конфигурации
                    self.access_token = pickle.load(file)
            except IOError:
                self.logger.fatal('Торговый токен не найден')
        else:  # Если указан торговый токен
            self.access_token = access_token  # Торговый токен
            with open(config_filename, 'wb') as file:  # Создаем файл конфигурации
                pickle.dump(self.access_token, file)

        self.jwt_token = ''  # Токен JWT
        self.jwt_token_issued = 0  # UNIX время в секундах выдачи токена JWT
        self.auth()  # Получаем токен JWT
        self.account_ids = list(self.token_details().account_ids)  # Из инфрмации о токене получаем список счетов

        self.exchanges: ExchangesResponse = self.call_function(self.assets_stub.Exchanges, ExchangesRequest())  # Список всех бирж
        self.assets: AssetsResponse = self.call_function(self.assets_stub.Assets, AssetsRequest())  # Список всех доступных инструментов

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
        self.logger.debug(f'Запрос : {request}')
        while True:  # Пока не получим ответ или ошибку
            try:  # Пытаемся
                response, call = func.with_call(request=request, metadata=(self.metadata,))  # вызвать функцию
                self.logger.debug(f'Ответ  : {response}')
                return response  # и вернуть ответ
            except RpcError as ex:  # Если получили ошибку канала
                # noinspection PyProtectedMember
                func_name = func._method.decode('utf-8')  # Название функции
                details = ex.args[0].details  # Сообщение об ошибке
                if 'GetAsset' not in func_name:  # При переводе канонического названия тикера в вид Финама приходится подбирать биржу. Поэтому, ошибки ф-ии GetAsset игнорируем
                    self.logger.error(f'Ошибка {details} при вызове функции {func_name} с параметрами {request}')
                return None  # Возвращаем пустое значение

    # Подписки

    def default_handler(self, event: list[marketdata_service.Quote] |
                        list[marketdata_service.StreamOrderBook] |
                        marketdata_service.SubscribeLatestTradesResponse |
                        list[orders_service.OrderState] |
                        list[trade.AccountTrade]):
        """Пустой обработчик события по умолчанию. Его можно заменить на пользовательский"""
        pass

    def default_bars_handler(self, event: marketdata_service.SubscribeBarsResponse, timeframe: marketdata_service.TimeFrame.ValueType):
        """Пустой обработчик события получения бар по умолчанию. Его можно заменить на пользовательский"""
        pass

    def subscribe_quote_thread(self, symbols):
        """Подписка на котировки по инструменту"""
        try:
            for event in self.marketdata_stub.SubscribeQuote(request=marketdata_service.SubscribeQuoteRequest(symbols=symbols), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeQuoteResponse = event  # Приводим пришедшее значение к подписке
                self.on_quote(e.quote)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def subscribe_order_book_thread(self, symbol):
        """Подписка на стакан по инструменту"""
        try:
            for event in self.marketdata_stub.SubscribeOrderBook(request=marketdata_service.SubscribeOrderBookRequest(symbol=symbol), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeOrderBookResponse = event  # Приводим пришедшее значение к подписке
                self.on_order_book(e.order_book)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def subscribe_latest_trades_thread(self, symbol):
        """Подписка на сделки по инструменту"""
        try:
            for event in self.marketdata_stub.SubscribeLatestTrades(request=marketdata_service.SubscribeOrderBookRequest(symbol=symbol), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeLatestTradesResponse = event  # Приводим пришедшее значение к подписке
                self.on_latest_trades(e)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def subscribe_bars_thread(self, symbol, finam_timeframe: marketdata_service.TimeFrame.ValueType):
        """Подписка на свечи по инструменту и временнОму интервалу"""
        try:
            for event in self.marketdata_stub.SubscribeBars(request=marketdata_service.SubscribeBarsRequest(symbol=symbol, timeframe=finam_timeframe), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeBarsResponse = event  # Приводим пришедшее значение к подписке
                self.on_new_bar(e, finam_timeframe)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def request_order_trade_iterator(self):
        """Генератор запросов на подписку/отписку своих заявок и сделок"""
        while True:  # Будем пытаться читать из очереди до закрытия канала
            yield self.order_trade_queue.get()  # Возврат из этой функции. При повторном ее вызове исполнение продолжится с этой строки

    def subscriptions_order_trade_handler(self):
        """Поток обработки подписок на свои заявки и сделки"""
        events = self.orders_stub.SubscribeOrderTrade(request_iterator=self.request_order_trade_iterator(), metadata=(self.metadata,))
        try:
            for event in events:
                e: orders_service.OrderTradeResponse = event  # Приводим пришедшее значение к подписке
                if e.orders:  # Если пришли заявки
                    for order in e.orders:
                        self.on_order(order)
                if e.trades:  # Если пришли сделки
                    for t in e.trades:
                        self.on_trade(t)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    # Выход и закрытие

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_channel()

    def __del__(self):
        self.close_channel()

    def close_channel(self):
        """Закрытие канала"""
        self.channel.close()  # Закрываем канал

    # Функции конвертации

    @staticmethod
    def finam_board_to_board(finam_board):
        """Канонический код режима торгов из кода режима торгов Финама

        :param str finam_board: Код режима торгов Финама
        :return: Канонический код режима торгов
        """
        if finam_board == 'FUT':  # Для фьючерсов
            return 'SPBFUT'
        if finam_board == 'OPT':  # Для опционов
            return 'SPBOPT'
        return finam_board

    @staticmethod
    def board_to_finam_board(board):
        """Код режима торгов Финама из канонического кода режима торгов

        :param str board: Канонический код режима торгов
        :return: Код режима торгов Финама
        """
        if board == 'SPBFUT':  # Для фьючерсов
            return 'FUT'
        if board == 'SPBOPT':  # Для опционов
            return 'OPT'
        return board

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
            mic = next((asset.mic for asset in self.assets.assets if asset.ticker == ticker), None)  # Биржа тикера из справочника
            if mic is None:  # Если биржа не найдена
                return None, ticker  # то возвращаем без кода режима торгов
            si: GetAssetResponse = self.call_function(self.assets_stub.GetAsset, GetAssetRequest(symbol=f'{ticker}@{mic}', account_id=self.account_ids[0]))
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
        for exchange in self.exchanges.exchanges:  # Пробегаемся по всем биржам
            si = self.get_symbol_info(ticker, exchange.mic)
            if si and si.board == finam_board:  # Если информация о тикере найдена, и режим торгов есть на бирже
                return exchange.mic  # то биржа найдена
        return None  # Если биржа не была найдена, то возвращаем пустое значение

    def get_symbol_info(self, ticker: str, mic: str) -> GetAssetResponse:
        """Спецификация тикера

        :param str ticker: Тикер
        :param str mic: Код биржи по ISO 10383 Market Identifier Codes. MISX - МосБиржа - все основные рынки, RTSX - МосБиржа - рынок деривативов
        :return: Спецификация тикера или None, если тикер не найден
        """
        return self.call_function(self.assets_stub.GetAsset, GetAssetRequest(symbol=f'{ticker}@{mic}', account_id=self.account_ids[0]))

    @staticmethod
    def timeframe_to_finam_timeframe(tf) -> tuple[marketdata_service.TimeFrame.ValueType, timedelta, bool]:
        """Перевод временнОго интервала во временной интервал Финама

        :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
        :return: Временной интервал Финама, максимальный размер запроса в днях, внутридневной бар
        """
        if 'MN3' in tf:  # 1 квартал
            return marketdata_service.TimeFrame.TIME_FRAME_QR, timedelta(days=365*5), False
        if 'MN' in tf:  # 1 месяц
            return marketdata_service.TimeFrame.TIME_FRAME_MN, timedelta(days=365*5), False
        if tf[0:1] == 'W':  # 1 неделя
            return marketdata_service.TimeFrame.TIME_FRAME_W, timedelta(days=365*5), False
        if tf[0:1] == 'D':  # 1 день
            return marketdata_service.TimeFrame.TIME_FRAME_D, timedelta(days=365), False
        if tf[0:1] == 'M':  # Минуты
            if not tf[1:].isdigit():  # Если после минут не стоит число
                raise NotImplementedError  # то с такими временнЫми интервалами не работаем
            interval = int(tf[1:])  # Временной интервал
            if interval == 480:  # 8 часов
                return marketdata_service.TimeFrame.TIME_FRAME_H8, timedelta(days=30), True
            if interval == 240:  # 4 часа
                return marketdata_service.TimeFrame.TIME_FRAME_H4, timedelta(days=30), True
            if interval == 120:  # 2 часа
                return marketdata_service.TimeFrame.TIME_FRAME_H2, timedelta(days=30), True
            if interval == 60:  # 1 час
                return marketdata_service.TimeFrame.TIME_FRAME_H1, timedelta(days=30), True
            if interval == 30:  # 30 минут
                return marketdata_service.TimeFrame.TIME_FRAME_M30, timedelta(days=30), True
            if interval == 15:  # 15 минут
                return marketdata_service.TimeFrame.TIME_FRAME_M15, timedelta(days=30), True
            if interval == 5:  # 5 минут
                return marketdata_service.TimeFrame.TIME_FRAME_M5, timedelta(days=30), True
            if interval == 1:  # 1 минута
                return marketdata_service.TimeFrame.TIME_FRAME_M1, timedelta(days=7), True
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    @staticmethod
    def finam_timeframe_to_timeframe(tf) -> tuple[str, timedelta, bool]:
        """Перевод временнОго интервала Финама во временной интервал

        :param marketdata_service.TimeFrame.ValueType tf: Временной интервал Финама
        :return: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм, максимальный размер запроса в днях, внутридневной бар
        """
        if tf == marketdata_service.TimeFrame.TIME_FRAME_M1:  # 1 минута
            return 'M1', timedelta(days=7), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_M5:  # 5 минут
            return 'M5', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_M15:  # 15 минут
            return 'M15', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_M30:  # 30 минут
            return 'M30', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_H1:  # 1 час
            return 'M60', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_H2:  # 2 часа
            return 'M120', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_H4:  # 4 часа
            return 'M240', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_H8:  # 8 часов
            return 'M480', timedelta(days=30), True
        if tf == marketdata_service.TimeFrame.TIME_FRAME_D:  # 1 день
            return 'D1', timedelta(days=365), False
        if tf == marketdata_service.TimeFrame.TIME_FRAME_W:  # 1 неделя
            return 'W1', timedelta(days=365 * 5), False
        if tf == marketdata_service.TimeFrame.TIME_FRAME_MN:  # 1 месяц
            return 'MN1', timedelta(days=365 * 5), False
        if tf == marketdata_service.TimeFrame.TIME_FRAME_QR:  # 1 квартал
            return 'MN3', timedelta(days=365 * 5), False
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

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
        dt_utc = utc.localize(dt) if dt.tzinfo is None else dt  # Задаем временнУю зону UTC если не задана
        dt_msk = dt_utc.astimezone(self.tz_msk)  # Переводим в МСК
        return dt_msk if tzinfo else dt_msk.replace(tzinfo=None)
