import logging  # Выводим лог на консоль и в файл
from datetime import datetime, timedelta  # Дата и время
from time import sleep  # Задержка в секундах перед выполнением операций

from FinamPy import FinamPyOld  # Работа с сервером TRANSAQ


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPyOld.Transactions')  # Будем вести лог
    fp_provider = FinamPyOld()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Transactions.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    security_board = 'TQBR'  # Код режима торгов
    security_code = 'SBER'  # Тикер
    # security_board = 'FUT'  # Код режима торгов
    # security_code = 'SiM4'  # Тикер

    client_id = fp_provider.client_ids[0]  # Будем работать с первым торговым счетом
    si = fp_provider.get_symbol_info(security_board, security_code)  # Получаем информацию о тикере
    logger.debug(si)
    decimals = si.decimals  # Кол-во десятичных знаков
    min_step = round(10 ** -decimals * si.min_step, decimals)  # Минимальный шаг цены

    tommorrow = datetime.today() + timedelta(days=1)  # Завтрашняя дата
    last_bar = fp_provider.get_day_candles(
        security_board, security_code, fp_provider.proto_candles.DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1,
        fp_provider.proto_candles.DayCandleInterval(to=fp_provider.google_date(year=tommorrow.year, month=tommorrow.month, day=tommorrow.day), count=1))  # Последний бар (до завтра)
    last_price = fp_provider.decimal_to_float(last_bar.candles[0].close)  # Последняя цена сделки
    logger.info(f'Последняя цена сделки {security_board}.{security_code}: {last_price}')

    # Обработчики подписок
    fp_provider.on_order = lambda order_event: logger.info(f'Заявка - {order_event}')  # Заявки
    fp_provider.on_trade = lambda trade_event: logger.info(f'Сделка - {trade_event}')  # Сделки
    fp_provider.on_portfolio = lambda portfolio_event: logger.info(f'Портфель - {portfolio_event}')  # Портфель

    # Создание подписок
    order_trade_request_id = 'OrderTrade1'  # Код подписки на заявки и сделки
    fp_provider.subscribe_order_trade([client_id], request_id=order_trade_request_id)  # Подписка на заявки и сделки
    logger.info(f'Подписка на заявки и сделки {order_trade_request_id} создана')

    # Новая рыночная заявка (открытие позиции)
    # logger.info(f'Заявка {security_board}.{security_code} на покупку минимального лота по рыночной цене')
    # response = fp_provider.new_order(client_id, security_board, security_code, fp_provider.proto_common.BuySell.BUY_SELL_BUY, 1)
    # logger.debug(response)
    # transaction_id = response.transaction_id
    # logger.info(f'Номер заявки: {transaction_id}')
    #
    # sleep(10)  # Ждем 10 секунд

    # Новая рыночная заявка (закрытие позиции)
    # logger.info(f'Заявка {security_board}.{security_code} на продажу минимального лота по рыночной цене')
    # response = fp_provider.new_order(client_id, security_board, security_code, fp_provider.proto_common.BuySell.BUY_SELL_SELL, 1)
    # logger.debug(response)
    # transaction_id = response.transaction_id
    # logger.info(f'Номер заявки: {transaction_id}')
    #
    # sleep(10)  # Ждем 10 секунд

    # Новая лимитная заявка
    limit_price = last_price * 0.99  # Лимитная цена на 1% ниже последней цены сделки
    limit_price = limit_price // min_step * min_step  # Округляем цену кратно минимальному шагу цены
    logger.info(f'Заявка {security_board}.{security_code} на покупку минимального лота по лимитной цене {limit_price}')
    response = fp_provider.new_order(client_id, security_board, security_code, fp_provider.proto_common.BuySell.BUY_SELL_BUY, 1, price=limit_price)  # Новая лимитная заявка
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
    stop_price = last_price * 1.01  # Стоп цена на 1% выше последней цены сделки
    stop_price = stop_price // min_step * min_step  # Округляем цену кратно минимальному шагу цены
    logger.info(f'Заявка {security_board}.{security_code} на покупку минимального лота по стоп цене {stop_price}')
    quantity = fp_provider.proto_stops.StopQuantity(value=1, units=fp_provider.proto_stops.StopQuantityUnits.STOP_QUANTITY_UNITS_LOTS)  # Кол-во в лотах
    stop_loss = fp_provider.proto_stops.StopLoss(activation_price=stop_price, market_price=True, price=0, quantity=quantity)  # Стоп заявка
    response = fp_provider.new_stop(client_id, security_board, security_code, fp_provider.proto_common.BuySell.BUY_SELL_BUY, stop_loss)  # Новая стоп заявка
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

    # Сброс обработчиков подписок
    fp_provider.on_order = fp_provider.default_handler  # Заявки
    fp_provider.on_trade = fp_provider.default_handler  # Сделки
    fp_provider.on_portfolio = fp_provider.default_handler  # Портфель

    # Выход
    fp_provider.close_channel()  # Закрываем канал перед выходом
