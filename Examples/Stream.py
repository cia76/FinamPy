import logging  # Выводим лог на консоль и в файл
from threading import Thread  # Запускаем поток подписки
from datetime import datetime  # Дата и время
from time import sleep  # Подписка на события по времени

from FinamPy import FinamPy
from FinamPy.grpc.marketdata.marketdata_service_pb2 import SubscribeQuoteResponse  # Котировки
from FinamPy.grpc.marketdata.marketdata_service_pb2 import SubscribeOrderBookResponse  # Стакан
from FinamPy.grpc.marketdata.marketdata_service_pb2 import SubscribeLatestTradesResponse  # Сделки


def _on_quote(quote: SubscribeQuoteResponse): logger.info(f'Котировка - {quote.quote[0] if len(quote.quote) > 0 else "Нет котировки"}')


def _on_order_book(order_book: SubscribeOrderBookResponse): logger.info(f'Стакан - {order_book.order_book[0] if len(order_book.order_book) > 0 else "Нет стакана"}')


def _on_latest_trades(latest_trade: SubscribeLatestTradesResponse): logger.info(f'Сделка - {latest_trade.trades[0] if len(latest_trade.trades) > 0 else "Нет сделки"}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Stream')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Stream.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    dataname = 'TQBR.SBER'  # Тикер

    finam_board, ticker = fp_provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
    mic = fp_provider.get_mic(finam_board, ticker)  # Биржа тикера

    # Котировки
    sleep_secs = 5  # Кол-во секунд получения котировок
    logger.info(f'Секунд котировок: {sleep_secs}')
    fp_provider.on_quote.subscribe(_on_quote)  # Подписываемся на котировки
    Thread(target=fp_provider.subscribe_quote_thread, name='QuoteThread', args=((f'{ticker}@{mic}',),)).start()  # Создаем и запускаем поток подписки на котировки
    sleep(sleep_secs)  # Ждем кол-во секунд получения котировок
    fp_provider.on_quote.unsubscribe(_on_quote)  # Отменяем подписку на котировки

    # Стакан
    sleep_secs = 5  # Кол-во секунд получения стакана
    logger.info(f'Секунд стакана: {sleep_secs}')
    fp_provider.on_order_book.subscribe(_on_order_book)  # Подписываемся на стакан
    Thread(target=fp_provider.subscribe_order_book_thread, name='OrderBookThread', args=(f'{ticker}@{mic}',)).start()  # Создаем и запускаем поток подписки на стакан
    sleep(sleep_secs)  # Ждем кол-во секунд получения стакана
    fp_provider.on_order_book.unsubscribe(_on_order_book)  # Отменяем подписку на стакан

    # Сделки
    sleep_secs = 5  # Кол-во секунд получения сделок
    logger.info(f'Секунд сделок: {sleep_secs}')
    fp_provider.on_latest_trades.subscribe(_on_latest_trades)  # Подписываемся на сделки
    Thread(target=fp_provider.subscribe_latest_trades_thread, name='LatestTradesThread', args=(f'{ticker}@{mic}',)).start()  # Создаем и запускаем поток подписки на сделки
    sleep(sleep_secs)  # Ждем кол-во секунд получения сделок
    fp_provider.on_latest_trades.unsubscribe(_on_latest_trades)  # Отменяем подписку на сделки

    fp_provider.close_channel()  # Закрываем канал перед выходом
