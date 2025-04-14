import logging  # Выводим лог на консоль и в файл
from threading import Thread  # Запускаем поток подписки
from datetime import datetime  # Дата и время

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import AssetsRequest, AssetsResponse  # Справочник всех тикеров


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Connect')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Connect.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbol = 'SBER@RUSX'  # Символ инструмента

    # Проверяем работу запрос/ответ
    # TODO Ждем от Финама получение времени на сервере
    logger.info(f'Данные тикера {symbol}')
    assets: AssetsResponse = fp_provider.call_function(fp_provider.assets_stub.Assets, AssetsRequest())  # Получаем справочник всех тикеров из провайдера
    si = next((asset for asset in assets.assets if asset.symbol == symbol), None)  # Пытаемся найти тикер в справочнике
    logger.info(f'Ответ от сервера: {si}' if si else f'Тикер {symbol} не найден')

    # Проверяем работу подписок
    # TODO Ждем от Финама подписку на бары
    logger.info(f'Подписка на стакан тикера: {symbol}')
    fp_provider.on_order_book = lambda order_book: logger.info(order_book[0])  # Обработчик события прихода подписки на стакан
    Thread(target=fp_provider.subscribe_order_book_thread, name='OrderBookThread', args=(symbol,)).start()  # Создаем и запускаем поток подписки на стакан тикера

    # Выход
    input('Enter - выход\n')
    fp_provider.close_channel()  # Закрываем канал перед выходом
