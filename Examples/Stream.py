import logging  # Выводим лог на консоль и в файл
from threading import Thread  # Запускаем поток подписки
from datetime import datetime  # Дата и время
from time import sleep  # Подписка на события по времени

from FinamPy import FinamPy


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Stream')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Stream.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbol = 'SBER@MISX'  # Символ инструмента

    # Котировки
    sleep_secs = 5  # Кол-во секунд получения котировок
    logger.info(f'{sleep_secs} секунд котировок {symbol}')
    fp_provider.on_quote = lambda quote: logger.info(quote[0])  # Обработчик события прихода подписки на котировки
    Thread(target=fp_provider.subscribe_quote_thread, name='QuoteThread', args=((symbol,),)).start()  # Создаем и запускаем поток подписки на котировки
    sleep(sleep_secs)  # Ждем кол-во секунд получения котировок
    fp_provider.on_quote = fp_provider.default_handler  # Возвращаем обработчик событий по умолчанию

    # Стакан
    sleep_secs = 5  # Кол-во секунд получения стакана
    logger.info(f'{sleep_secs} секунд стакана {symbol}')
    fp_provider.on_order_book = lambda order_book: logger.info(order_book[0])  # Обработчик события прихода подписки на стакан
    Thread(target=fp_provider.subscribe_order_book_thread, name='OrderBookThread', args=(symbol,)).start()  # Создаем и запускаем поток подписки на стакан
    sleep(sleep_secs)  # Ждем кол-во секунд получения стакана
    fp_provider.on_order_book = fp_provider.default_handler  # Возвращаем обработчик событий по умолчанию

    # Сделки
    sleep_secs = 5  # Кол-во секунд получения сделок
    logger.info(f'{sleep_secs} секунд сделок {symbol}')
    fp_provider.on_latest_trades = lambda latest_trade: logger.info(latest_trade)  # Обработчик события прихода подписки на сделки
    Thread(target=fp_provider.subscribe_latest_trades_thread, name='LatestTradesThread', args=(symbol,)).start()  # Создаем и запускаем поток подписки на сделки
    sleep(sleep_secs)  # Ждем кол-во секунд получения сделок
    fp_provider.on_latest_trades = fp_provider.default_handler  # Возвращаем обработчик событий по умолчанию

    fp_provider.close_channel()  # Закрываем канал перед выходом
