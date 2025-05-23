import logging  # Будем вести лог
from datetime import datetime
from typing import Union  # Объединение типов

from pytz import timezone  # Работаем с временнОй зоной
from grpc import ssl_channel_credentials, secure_channel, RpcError  # Защищенный канал

from FinamPy import Config  # Файл конфигурации

# Структуры
import FinamPy.grpc.auth.auth_service_pb2 as auth_service  # Подключение https://tradeapi.finam.ru/docs/guides/auth
import FinamPy.grpc.marketdata.marketdata_service_pb2 as marketdata_service  # Рыночные данные https://tradeapi.finam.ru/docs/guides/marketdata

# gRPC - Сервисы
from FinamPy.grpc.auth.auth_service_pb2_grpc import AuthServiceStub  # Подлключение https://tradeapi.finam.ru/docs/guides/auth#authservice
from FinamPy.grpc.accounts.accounts_service_pb2_grpc import AccountsServiceStub  # Счета https://tradeapi.finam.ru/docs/guides/accounts#accountsservice
from FinamPy.grpc.assets.assets_service_pb2_grpc import AssetsServiceStub  # Инструменты https://tradeapi.finam.ru/docs/guides/assets#assetsservice
from FinamPy.grpc.orders.orders_service_pb2_grpc import OrdersServiceStub  # Заявки https://tradeapi.finam.ru/docs/guides/orders#ordersservice
from FinamPy.grpc.marketdata.marketdata_service_pb2_grpc import MarketDataServiceStub  # Рыночные данные https://tradeapi.finam.ru/docs/guides/marketdata#marketdataservice


class FinamPy:
    """Работа с Finam Trade API gRPC https://tradeapi.finam.ru из Python"""
    tz_msk = timezone('Europe/Moscow')  # Время UTC будем приводить к московскому времени
    server = 'ftrr01.finam.ru:443'  # Сервер для исполнения вызовов
    jwt_token_ttl = 15 * 60  # Время жизни токена JWT 15 минут в секундах
    logger = logging.getLogger('FinamPy')  # Будем вести лог
    metadata: tuple[str, str]  # Токен JWT в запросах

    def __init__(self, access_token=Config.access_token):
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
        self.on_order_book = self.default_handler  # Стакан
        self.on_latest_trades = self.default_handler  # Обезличенные сделки

        self.access_token = access_token  # Торговый токен
        self.jwt_token = ''  # Токен JWT
        self.jwt_token_issued = 0  # UNIX время в секундах выдачи токена JWT
        self.auth()  # Получаем токен JWT
        self.account_ids = list(self.token_details().account_ids)  # Из инфрмации о токене получаем список счетов

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
        while True:  # Пока не получим ответ или ошибку
            try:  # Пытаемся
                response, call = func.with_call(request=request, metadata=(self.metadata,))  # вызвать функцию
                # self.logger.debug(f'Запрос: {request} Ответ: {response}')  # Для отладки работоспособности сервера Финам
                return response  # и вернуть ответ
            except RpcError as ex:  # Если получили ошибку канала
                # noinspection PyProtectedMember
                func_name = func._method.decode('utf-8')  # Название функции
                details = ex.args[0].details  # Сообщение об ошибке
                self.logger.error(f'Ошибка {details} при вызове функции {func_name} с параметрами {request}')
                return None  # Возвращаем пустое значение

    # Подписки

    def default_handler(self, event: Union[marketdata_service.Quote, marketdata_service.StreamOrderBook, marketdata_service.Trade]):
        """Пустой обработчик события по умолчанию. Его можно заменить на пользовательский"""
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
                self.on_latest_trades(e.trades)
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
