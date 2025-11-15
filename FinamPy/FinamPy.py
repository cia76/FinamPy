import logging  # Будем вести лог
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # ВременнАя зона
from typing import Any  # Любой тип
from queue import SimpleQueue  # Очередь подписок/отписок

import keyring  # Безопасное хранение торгового токена
import keyring.errors  # Ошибки хранилища
from grpc import ssl_channel_credentials, secure_channel, RpcError  # Защищенный канал

# Структуры
from .grpc.auth import auth_service_pb2 as auth_service  # Подключение https://tradeapi.finam.ru/docs/guides/grpc/auth_service
from .grpc.assets.assets_service_pb2 import ExchangesRequest, ExchangesResponse, AssetsRequest, AssetsResponse, GetAssetResponse, GetAssetRequest  # Информация о биржах и тикерах
from .grpc.marketdata import marketdata_service_pb2 as marketdata_service  # Рыночные данные https://tradeapi.finam.ru/docs/guides/grpc/marketdata_service/
from .grpc.orders import orders_service_pb2 as orders_service  # Заявки https://tradeapi.finam.ru/docs/guides/grpc/orders_service/

# gRPC - Сервисы
from .grpc.auth.auth_service_pb2_grpc import AuthServiceStub  # Подлключение https://tradeapi.finam.ru/docs/guides/grpc/auth_service
from .grpc.accounts.accounts_service_pb2_grpc import AccountsServiceStub  # Счета https://tradeapi.finam.ru/docs/guides/grpc/accounts_service/
from .grpc.assets.assets_service_pb2_grpc import AssetsServiceStub  # Инструменты https://tradeapi.finam.ru/docs/guides/grpc/assets_service/
from .grpc.orders.orders_service_pb2_grpc import OrdersServiceStub  # Заявки https://tradeapi.finam.ru/docs/guides/grpc/orders_service/
from .grpc.marketdata.marketdata_service_pb2_grpc import MarketDataServiceStub  # Рыночные данные https://tradeapi.finam.ru/docs/guides/grpc/marketdata_service/


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
        try:
            for event in self.marketdata_stub.SubscribeQuote(request=marketdata_service.SubscribeQuoteRequest(symbols=symbols), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeQuoteResponse = event  # Приводим пришедшее значение к подписке
                self.on_quote.trigger(e)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def subscribe_order_book_thread(self, symbol):
        """Подписка на стакан по инструменту"""
        try:
            for event in self.marketdata_stub.SubscribeOrderBook(request=marketdata_service.SubscribeOrderBookRequest(symbol=symbol), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeOrderBookResponse = event  # Приводим пришедшее значение к подписке
                self.on_order_book.trigger(e)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def subscribe_latest_trades_thread(self, symbol):
        """Подписка на сделки по инструменту"""
        try:
            for event in self.marketdata_stub.SubscribeLatestTrades(request=marketdata_service.SubscribeLatestTradesRequest(symbol=symbol), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeLatestTradesResponse = event  # Приводим пришедшее значение к подписке
                self.on_latest_trades.trigger(e)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

    def subscribe_bars_thread(self, symbol, finam_timeframe: marketdata_service.TimeFrame.ValueType):
        """Подписка на свечи по инструменту и временнОму интервалу"""
        try:
            for event in self.marketdata_stub.SubscribeBars(request=marketdata_service.SubscribeBarsRequest(symbol=symbol, timeframe=finam_timeframe), metadata=(self.metadata,)):
                e: marketdata_service.SubscribeBarsResponse = event  # Приводим пришедшее значение к подписке
                self.on_new_bar.trigger(e, finam_timeframe)
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
                        self.on_order.trigger(order)
                if e.trades:  # Если пришли сделки
                    for t in e.trades:
                        self.on_trade.trigger(t)
        except RpcError:  # При закрытии канала попадем на эту ошибку (grpc._channel._MultiThreadedRendezvous)
            pass  # Все в порядке, ничего делать не нужно

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
        raise NotImplementedError(f'Временной интервал Финама {finam_tf} не поддерживается')  # С остальными временнЫми интервалами Финима не работаем

    @staticmethod
    def price_to_finam_price(board: str, price: float) -> float:
        if board in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            return price / 10  # Делим цену на 10
        return price  # В остальных случаях возвращаем цену без изменений

    @staticmethod
    def finam_price_to_price(board: str, finam_price: float) -> float:
        if board in ('TQOB', 'TQCB', 'TQRD', 'TQIR'):  # Для облигаций (Т+ Гособлигации, Т+ Облигации, Т+ Облигации Д, Т+ Облигации ПИР)
            return finam_price * 10  # Умножаем цену на 10
        return finam_price  # В остальных случаях возвращаем цену без изменений

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
                self.logger.error(f'Токен не найден в системном хранилище. Передайте токен при создании объекта: FinamPy("<Токен>")')
                return None
            token = ''.join(token_parts)  # Собираем токен из частей
            self.logger.debug('Токен успешно загружен из системного хранилища')
            return token
        except keyring.errors.KeyringError as e:
            self.logger.fatal(f'Ошибка доступа к системному хранилищу: {e}')
        except Exception as e:
            self.logger.fatal(f'Ошибка при загрузке токена: {e}')

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

    def set_long_token_to_keyring(self, service: str, username: str, token: str, password_split_size: int = 500) -> None:
        """Установка токена в системное хранилище keyring по частям"""
        try:
            self.clear_long_token_from_keyring(service, username)  # Очищаем предыдущие части токена
            token_parts = [token[i:i + password_split_size] for i in range(0, len(token), password_split_size)]  # Разбиваем токен на части заданного размера
            for index, token_part in enumerate(token_parts):  # Пробегаемся по частям токена
                keyring.set_password(service, f'{username}{index}', token_part)
            self.logger.info(f'Токен сохранён в системном хранилище ({len(token_parts)} частей)')
        except keyring.errors.KeyringError as e:
            self.logger.critical(f'Ошибка сохранения в системное хранилище: {e}')
        except Exception as e:
            self.logger.critical(f'Неожиданная ошибка при сохранении токена: {e}')


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
