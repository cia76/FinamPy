from datetime import datetime  # Дата и время
from time import sleep  # Задержка в секундах перед выполнением операций
import logging  # Выводим лог на консоль и в файл
from threading import Thread  # Запускаем поток подписки

from FinamPy import FinamPy
from google.type.decimal_pb2 import Decimal
from FinamPy.grpc.assets.assets_service_pb2 import GetAssetRequest, GetAssetResponse  # Информация по тикеру
from FinamPy.grpc.orders.orders_service_pb2 import OrderTradeRequest
from FinamPy.grpc.orders.orders_service_pb2 import Order, OrderState, CancelOrderRequest, ORDER_TYPE_MARKET, ORDER_TYPE_LIMIT, ORDER_TYPE_STOP, StopCondition  # Заявки
from FinamPy.grpc.side_pb2 import SIDE_BUY, SIDE_SELL  # Направление заявки
from FinamPy.grpc.marketdata.marketdata_service_pb2 import QuoteRequest, QuoteResponse  # Последняя цена сделки


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Transactions')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Transactions.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbol = 'SBER@MISX'  # Символ инструмента

    account_id = fp_provider.account_ids[0]  # Торговый счет, где будут выставляться заявки
    si: GetAssetResponse = fp_provider.call_function(fp_provider.assets_stub.GetAsset, GetAssetRequest(symbol=symbol, account_id=account_id))
    quantity = Decimal(value=str(int(float(si.lot_size.value))))  # Количество в шт

    # Свои заявки и сделки
    fp_provider.on_order = lambda order: logger.info(order)  # Обработчик события прихода своей заявки
    fp_provider.on_trade = lambda trade: logger.info(trade)  # Обработчик события прихода своей сделки
    Thread(target=fp_provider.subscriptions_order_trade_handler, name='SubscriptionsOrderTradeThread').start()  # Создаем и запускаем поток обработки своих заявок и сделок
    fp_provider.order_trade_queue.put(OrderTradeRequest(  # Ставим в буфер команд/сделок
        action=OrderTradeRequest.Action.ACTION_SUBSCRIBE,  # Подписываемся
        data_type=OrderTradeRequest.DataType.DATA_TYPE_ALL,  # на свои заявки и сделки
        account_id=account_id))  # по торговому счету

    # Новая рыночная заявка на покупку (открытие позиции)
    # logger.info(f'Заявка {symbol} на покупку минимального лота {quantity} шт. по рыночной цене')
    # order_state: OrderState = fp_provider.call_function(
    #     fp_provider.orders_stub.PlaceOrder,
    #     Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_BUY, type=ORDER_TYPE_MARKET,
    #           client_order_id=f'MarketBuy {int(datetime.now().timestamp())}')
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
    #     Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_SELL, type=ORDER_TYPE_MARKET,
    #           client_order_id=f'MarketSell {int(datetime.now().timestamp())}')
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
    logger.info(f'Заявка {symbol} на покупку минимального лота {quantity} шт. по лимитной цене {limit_price}')
    order_state: OrderState = fp_provider.call_function(
        fp_provider.orders_stub.PlaceOrder,
        Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_BUY, type=ORDER_TYPE_LIMIT,
              limit_price=Decimal(value=str(limit_price)), client_order_id=f'LimitBuy {int(datetime.now().timestamp())}')
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
    logger.info(f'Заявка {symbol} на покупку минимального лота {quantity} шт. по стоп цене {stop_price}')
    order_state: OrderState = fp_provider.call_function(
        fp_provider.orders_stub.PlaceOrder,
        Order(account_id=account_id, symbol=symbol, quantity=quantity, side=SIDE_BUY, type=ORDER_TYPE_STOP,
              stop_price=Decimal(value=str(stop_price)), stop_condition=StopCondition.STOP_CONDITION_LAST_UP, client_order_id=f'StopBuy {int(datetime.now().timestamp())}')
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

    fp_provider.order_trade_queue.put(OrderTradeRequest(  # Ставим в буфер команд/сделок
        action=OrderTradeRequest.Action.ACTION_UNSUBSCRIBE,  # Отписываемся
        data_type=OrderTradeRequest.DataType.DATA_TYPE_ALL,  # от своих заявок и сделок
        account_id=account_id))  # по торговому счету

    fp_provider.close_channel()  # Закрываем канал перед выходом
