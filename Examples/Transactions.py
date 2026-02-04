from datetime import datetime  # Дата и время
from time import sleep  # Задержка в секундах перед выполнением операций
import logging  # Выводим лог на консоль и в файл
from threading import Thread  # Запускаем поток подписки

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import GetAssetRequest, GetAssetResponse  # Информация по тикеру
from FinamPy.grpc.orders.orders_service_pb2 import Order, OrderState, OrderType, CancelOrderRequest, StopCondition  # Заявки
import FinamPy.grpc.side_pb2 as side  # Направление заявки
from FinamPy.grpc.marketdata.marketdata_service_pb2 import QuoteRequest, QuoteResponse  # Последняя цена сделки

from google.type.decimal_pb2 import Decimal


def _on_order(order): logger.info(f'Заявка - {order}')


def _on_trade(trade): logger.info(f'Сделка - {trade}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Transactions')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Transactions.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    dataname = 'TQBR.SBER'  # Тикер

    finam_board, ticker = fp_provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
    mic = fp_provider.get_mic(finam_board, ticker)  # Биржа тикера
    symbol = f'{ticker}@{mic}'  # Тикер Финама
    account_id = fp_provider.account_ids[0]  # Торговый счет, где будут выставляться заявки
    si: GetAssetResponse = fp_provider.call_function(fp_provider.assets_stub.GetAsset, GetAssetRequest(symbol=symbol, account_id=account_id))
    quantity = Decimal(value=str(int(float(si.lot_size.value))))  # Количество в шт

    # Свои заявки и сделки
    fp_provider.on_order.subscribe(_on_order)  # Подписываемся на заявки
    fp_provider.on_trade.subscribe(_on_trade)  # Подписываемся на сделки
    Thread(target=fp_provider.subscribe_orders_thread, name='SubscriptionOrdersThread').start()  # Создаем и запускаем поток обработки своих заявок
    Thread(target=fp_provider.subscribe_trades_thread, name='SubscriptionTradesThread').start()  # Создаем и запускаем поток обработки своих сделок

    # Новая рыночная заявка на покупку (открытие позиции)
    # logger.info(f'Заявка {symbol} на покупку минимального лота {quantity} шт. по рыночной цене')
    # order_state: OrderState = fp_provider.call_function(
    #     fp_provider.orders_stub.PlaceOrder,
    #     Order(account_id=account_id, symbol=symbol, quantity=quantity, side=side.SIDE_BUY, type=OrderType.ORDER_TYPE_MARKET,
    #           client_order_id=str(int(datetime.now().timestamp())))
    # )  # Выставление заявки
    # logger.debug(order_state)
    # logger.info(f'Номер заявки: {order_state.order_id}')
    # logger.info(f'Номер исполнения заявки: {order_state.exec_id}')
    # logger.info(f'Статус заявки: {order_state.status}')
    #
    # sleep(10)  # Ждем 10 секунд

    # Новая рыночная заявка на продажу (закрытие позиции)
    # logger.info(f'Заявка {symbol} на продажу минимального лота {quantity} шт. по рыночной цене')
    # order_state: OrderState = fp_provider.call_function(
    #     fp_provider.orders_stub.PlaceOrder,
    #     Order(account_id=account_id, symbol=symbol, quantity=quantity, side=side.SIDE_SELL, type=OrderType.ORDER_TYPE_MARKET,
    #           client_order_id=str(int(datetime.now().timestamp())))
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
    limit_price = round(last_price * 0.99, si.decimals)  # Лимитная цена на 1% ниже последней цены сделки
    logger.info(f'Заявка на покупку минимального лота {quantity} шт. {dataname} по лимитной цене {limit_price}')
    order_state: OrderState = fp_provider.call_function(
        fp_provider.orders_stub.PlaceOrder,
        Order(account_id=account_id, symbol=symbol, quantity=quantity, side=side.SIDE_BUY, type=OrderType.ORDER_TYPE_LIMIT,
              limit_price=Decimal(value=str(limit_price)), client_order_id=str(int(datetime.now().timestamp())))
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
    stop_price = round(last_price * 1.01, si.decimals)  # Стоп цена на 1% выше последней цены сделки
    logger.info(f'Заявка на покупку минимального лота {quantity} шт. {dataname} по стоп цене {stop_price}')
    order_state: OrderState = fp_provider.call_function(
        fp_provider.orders_stub.PlaceOrder,
        Order(account_id=account_id, symbol=symbol, quantity=quantity, side=side.SIDE_BUY, type=OrderType.ORDER_TYPE_STOP,
              stop_price=Decimal(value=str(stop_price)), stop_condition=StopCondition.STOP_CONDITION_LAST_UP, client_order_id=str(int(datetime.now().timestamp())))
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

    # Отмена подписок
    fp_provider.on_order.unsubscribe(_on_order)  # Сбрасываем обработчик заявок
    fp_provider.on_trade.unsubscribe(_on_trade)  # Сбрасываем обработчик сделок

    # Выход
    fp_provider.close_channel()  # Закрываем канал перед выходом
