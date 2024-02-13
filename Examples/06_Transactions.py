import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время
from time import sleep  # Задержка в секундах перед выполнением операций

from FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации

from FinamPy.proto.tradeapi.v1.events_pb2 import OrderBookEvent  # Событие стакана
from FinamPy.proto.tradeapi.v1 import common_pb2 as common  # Покупка/продажа
from FinamPy.proto.tradeapi.v1.stops_pb2 import StopLoss, StopQuantity, StopQuantityUnits  # Стоп заявка


logger = logging.getLogger('FinamPy.Transactions')  # Будем вести лог


def on_order_book(event: OrderBookEvent):
    global ask, bid
    ask = event.asks[0].price  # Лучшая цена из стакана по которой можем купить
    bid = event.bids[0].price  # Лучшая цена из стакана по которой можем продать
    logger.info(f'ask = {ask} bid = {bid}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Провайдер работает со всеми счетами по токену (из файла Config.py)
    client_id = Config.ClientIds[0]  # Будем работать с первым торговым счетом из списка

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Transactions.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    security_board = 'TQBR'  # Код режима торгов
    security_code = 'SBER'  # Тикер
    # security_board = 'FUT'  # Код режима торгов
    # security_code = 'SiH4'  # Тикер

    si = fp_provider.get_symbol_info(security_board, security_code)  # Получаем информацию о тикере
    logger.debug(si)
    decimals = si.decimals  # Кол-во десятичных знаков
    min_step = round(10 ** -decimals * si.min_step, decimals)  # Шаг цены
    ask = 0  # Лучшая цена из стакана по которой можем купить
    bid = 0  # Лучшая цена из стакана по которой можем продать

    # Обработчики подписок
    fp_provider.on_order = lambda order_event: logger.info(f'Заявка - {order_event}')  # Заявки
    fp_provider.on_trade = lambda trade_event: logger.info(f'Сделка - {trade_event}')  # Сделки
    fp_provider.on_order_book = on_order_book  # Стакан
    fp_provider.on_portfolio = lambda portfolio_event: logger.info(f'Портфель - {portfolio_event}')  # Портфель

    # Создание подписок
    order_trade_request_id = 'OrderTrade1'  # Код подписки на заявки и сделки
    fp_provider.subscribe_order_trade([client_id], request_id=order_trade_request_id)  # Подписка на заявки и сделки
    logger.info(f'Подписка на заявки и сделки {order_trade_request_id} создана')
    order_book_request_id = 'OrderBook1'  # Код подписки на стакан
    fp_provider.subscribe_order_book(security_code, security_board, order_book_request_id)  # Подписка на стакан
    logger.info(f'Подписка на стакан {order_trade_request_id} тикера {security_board}.{security_code} создана')

    while not ask or not bid:  # Пока не пришли лучшие цены покупки и продажи из стакана
        sleep(1)  # то ждем

    # Новая рыночная заявка (открытие позиции)
    # logger.info(f'Заявка {security_board}.{security_code} на покупку минимального лота по рыночной цене')
    # response = fp_provider.new_order(client_id, security_board, security_code, common.BUY_SELL_BUY, 1)
    # logger.debug(response)
    # transaction_id = response.transaction_id
    # logger.info(f'Номер заявки: {transaction_id}')

    # sleep(10)  # Ждем 10 секунд

    # Новая рыночная заявка (закрытие позиции)
    # logger.info(f'Заявка {security_board}.{security_code} на продажу минимального лота по рыночной цене')
    # response = fp_provider.new_order(client_id, security_board, security_code, common.BUY_SELL_SELL, 1)
    # logger.debug(response)
    # transaction_id = response.transaction_id
    # logger.info(f'Номер заявки: {transaction_id}')

    # sleep(10)  # Ждем 10 секунд

    # Новая лимитная заявка
    limit_price = ask * 0.99  # Лимитная цена на 1% ниже последней цены сделки
    limit_price = limit_price // min_step * min_step  # Округляем цену кратно минимальному шагу цены
    logger.info(f'Заявка {security_board}.{security_code} на покупку минимального лота по лимитной цене {limit_price}')
    response = fp_provider.new_order(client_id, security_board, security_code, common.BUY_SELL_BUY, 1, price=limit_price)  # Новая лимитная заявка
    logger.debug(response)
    transaction_id = response.transaction_id  # Номер заявки
    logger.info(f'Номер заявки: {transaction_id}')

    sleep(10)  # Ждем 10 секунд

    # Удаление существующей лимитной заявки
    logger.info(f'Удаление заявки: {transaction_id}')
    response = fp_provider.cancel_order(client_id, transaction_id)  # Удаление существующей лимитной заявки
    logger.info(f'Статус: {response}')

    sleep(10)  # Ждем 10 секунд

    # Новая стоп заявка
    stop_price = ask * 1.01  # Стоп цена на 1% выше последней цены сделки
    stop_price = stop_price // min_step * min_step  # Округляем цену кратно минимальному шагу цены
    logger.info(f'Заявка {security_board}.{security_code} на покупку минимального лота по стоп цене {stop_price}')
    quantity = StopQuantity(value=1, units=StopQuantityUnits.STOP_QUANTITY_UNITS_LOTS)  # Кол-во в лотах
    stop_loss = StopLoss(activation_price=stop_price, market_price=True, price=0, quantity=quantity)  # Стоп заявка
    response = fp_provider.new_stop(client_id, security_board, security_code, common.BUY_SELL_BUY, stop_loss)  # Новая стоп заявка
    logger.debug(response)
    stop_id = response.stop_id  # Номер стоп заявки
    logger.info(f'Номер стоп заявки: {stop_id}')

    sleep(10)  # Ждем 10 секунд

    # Удаление существующей стоп заявки
    logger.info(f'Удаление стоп заявки: {stop_id}')
    response = fp_provider.cancel_stop(client_id, stop_id)  # Удаление существующей стоп заявки
    logger.info(f'Статус: {response}')

    sleep(10)  # Ждем 10 секунд

    # Отмена подписок
    fp_provider.unsubscribe_order_trade(order_trade_request_id)  # Отмена подписки на заявки и сделки
    logger.info(f'Подписка на заявки и сделки {order_trade_request_id} отменена')
    fp_provider.unsubscribe_order_book(order_trade_request_id, security_code, security_board)  # Отмена подписки на стакан
    logger.info(f'Подписка на стакан {order_trade_request_id} тикера {security_board}.{security_code} отменена')

    # Сброс обработчиков подписок
    fp_provider.on_order = fp_provider.default_handler  # Заявки
    fp_provider.on_trade = fp_provider.default_handler  # Сделки
    fp_provider.on_order_book = fp_provider.default_handler  # Стакан
    fp_provider.on_portfolio = fp_provider.default_handler  # Портфель

    # Выход
    fp_provider.close_channel()  # Закрываем канал перед выходом
