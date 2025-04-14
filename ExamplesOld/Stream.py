import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время
from time import sleep  # Подписка на события по времени

from FinamPy import FinamPyOld  # Работа с сервером TRANSAQ


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPyOld.Stream')  # Будем вести лог
    fp_provider = FinamPyOld()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Stream.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    security_board = 'TQBR'  # Код режима торгов
    security_code = 'SBER'  # Тикер
    # security_board = 'FUT'  # Код режима торгов
    # security_code = 'SiM5'  # Тикер

    # Стакан
    sleep_secs = 5  # Кол-во секунд получения стакана
    logger.info(f'{sleep_secs} секунд стакана {security_board}.{security_code}')
    fp_provider.on_order_book = lambda order_book: logger.info(f'ask = {order_book.asks[0].price} bid = {order_book.bids[0].price}')  # Обработчик события прихода подписки на стакан
    request_id = 'orderbook1'  # Код подписки может быть любым
    fp_provider.subscribe_order_book(security_code, security_board, request_id)  # Подписываемся на стакан тикера
    logger.info(f'Подписка на стакан {request_id} тикера {security_board}.{security_code} создана')
    sleep(sleep_secs)  # Ждем кол-во секунд получения стакана
    logger.info(f'Подписка на стакан {request_id} тикера {security_board}.{security_code} отменена')
    fp_provider.unsubscribe_order_book(request_id, security_code, security_board)  # Отписываемся от стакана тикера

    # Выход
    fp_provider.close_channel()  # Закрываем канал перед выходом
