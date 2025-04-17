import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время
from time import sleep  # Задержка в секундах перед выполнением операций

from FinamPy import FinamPy
from google.type.decimal_pb2 import Decimal
from FinamPy.grpc.orders.orders_service_pb2 import Order, OrderState, CancelOrderRequest, TIME_IN_FORCE_GOOD_TILL_CANCEL, ORDER_TYPE_MARKET, ORDER_TYPE_LIMIT, ORDER_TYPE_STOP  # Заявки
from FinamPy.grpc.side_pb2 import SIDE_BUY, SIDE_SELL  # Направление заявки
from FinamPy.grpc.marketdata.marketdata_service_pb2 import QuoteRequest, QuoteResponse  # Последняя цена сделки


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Transactions')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Transactions.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbol = 'SBER@RUSX'  # Символ инструмента

    account_id = fp_provider.account_ids[0]  # Торговый счет, где будут выставляться заявки
    quantity = Decimal(str(10))  # Количество в шт.

    # TODO Ждем от Финама подписки на изменения счета, заявок, позиций

    # Новая рыночная заявка на покупку (открытие позиции)
    # logger.info(f'Заявка {symbol} на покупку минимального лота по рыночной цене')
    # order_state: OrderState = fp_provider.call_function(
    #     fp_provider.orders_stub.PlaceOrder,
    #     Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_BUY, type=ORDER_TYPE_MARKET, time_in_force=TIME_IN_FORCE_GOOD_TILL_CANCEL, client_order_id='MarketBuy')
    # )  # Выставление заявки
    # logger.debug(order_state)
    # logger.info(f'Номер заявки: {order_state.order_id}')
    # logger.info(f'Номер исполнения заявки: {order_state.exec_id}')
    # logger.info(f'Статус заявки: {order_state.status}')
    #
    # sleep(10)  # Ждем 10 секунд

    # Новая рыночная заявка на продажу (закрытие позиции)
    # logger.info(f'Заявка {symbol} на продажу минимального лота по рыночной цене')
    # order_state: OrderState = fp_provider.call_function(
    #     fp_provider.orders_stub.PlaceOrder,
    #     Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_SELL, type=ORDER_TYPE_MARKET, time_in_force=TIME_IN_FORCE_GOOD_TILL_CANCEL, client_order_id='MarketSell')
    # )  # Выставление заявки
    # logger.debug(order_state)
    # logger.info(f'Номер заявки: {order_state.order_id}')
    # logger.info(f'Номер исполнения заявки: {order_state.exec_id}')
    # logger.info(f'Статус заявки: {order_state.status}')
    #
    # sleep(10)  # Ждем 10 секунд

    quote_response: QuoteResponse = fp_provider.call_function(fp_provider.marketdata_stub.LastQuote, QuoteRequest(symbol=symbol))  # Получение последней котировки по инструменту
    last_price = float(quote_response.quote.last.value)  # Последняя цена сделки

    # Новая лимитная заявка на покупку
    limit_price = last_price * 0.99  # Лимитная цена на 1% ниже последней цены сделки TODO Ждем от Финама спецификацию тикеров, чтобы правильно округлять цены
    logger.info(f'Заявка {symbol} на покупку минимального лота по лимитной цене {limit_price}')
    order_state: OrderState = fp_provider.call_function(
        fp_provider.orders_stub.PlaceOrder,
        Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_BUY, type=ORDER_TYPE_LIMIT, time_in_force=TIME_IN_FORCE_GOOD_TILL_CANCEL, limit_price=Decimal(str(limit_price)), client_order_id='LimitBuy')
    )  # Выставление заявки
    logger.debug(order_state)
    order_id = order_state.order_id  # Номер заявки
    logger.info(f'Номер заявки: {order_id}')
    logger.info(f'Статус заявки: {order_state.status}')

    sleep(10)  # Ждем 10 секунд

    # Удаление существующей лимитной заявки
    logger.info(f'Удаление заявки: {order_id}')
    order_state: OrderState = fp_provider.call_function(fp_provider.orders_stub.CancelOrder, CancelOrderRequest(account_id=account_id, order_id=order_id))  # Удаление заявки
    logger.debug(order_state)
    logger.info(f'Статус заявки: {order_state.status}')

    sleep(10)  # Ждем 10 секунд

    # Новая стоп заявка на покупку
    stop_price = last_price * 1.01  # Стоп цена на 1% выше последней цены сделки TODO Ждем от Финама спецификацию тикеров, чтобы правильно округлять цены
    logger.info(f'Заявка {symbol} на покупку минимального лота по стоп цене {stop_price}')
    order_state: OrderState = fp_provider.call_function(
        fp_provider.orders_stub.PlaceOrder,
        Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_BUY, type=ORDER_TYPE_STOP, time_in_force=TIME_IN_FORCE_GOOD_TILL_CANCEL, stop_price=Decimal(str(stop_price)), client_order_id='StopMarketBuy')
    )  # Выставление заявки
    logger.debug(order_state)
    order_id = order_state.order_id  # Номер заявки
    logger.info(f'Номер заявки: {order_id}')
    logger.info(f'Статус заявки: {order_state.status}')

    sleep(10)  # Ждем 10 секунд

    # Удаление существующей стоп заявки
    logger.info(f'Удаление стоп заявки: {order_id}')
    order_state: OrderState = fp_provider.call_function(fp_provider.orders_stub.CancelOrder, CancelOrderRequest(account_id=account_id, order_id=order_id))  # Удаление заявки
    logger.debug(order_state)
    logger.info(f'Статус заявки: {order_state.status}')

    sleep(10)  # Ждем 10 секунд

    fp_provider.close_channel()  # Закрываем канал перед выходом
