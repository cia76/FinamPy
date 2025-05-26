import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import AssetsRequest, AssetsResponse  # Справочник всех тикеров


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Ticker')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbols = ('SBER@MISX', 'VTBR@MISX', 'SIM5@RTSX', 'RIM5@RTSX', 'USDRUBF@RTSX', 'CNYRUBF@RTSX')  # Кортеж тикеров

    assets: AssetsResponse = fp_provider.call_function(fp_provider.assets_stub.Assets, AssetsRequest())  # Получаем справочник всех тикеров из провайдера
    for symbol in symbols:  # Пробегаемся по всем тикерам
        si = next((asset for asset in assets.assets if asset.symbol == symbol), None)  # Пытаемся найти тикер в справочнике
        if not si:  # Если тикер не найден
            logger.warning(f'Тикер {symbol} не найден')
            continue  # то переходим к следующему тикеру, дальше не продолжаем
        logger.info(f'Ответ от сервера: {si}')
        logger.info(f'Информация о тикере {si.symbol} ({si.name}, {si.type})')
        # TODO Ждем от Финама Лот, Кол-во десятичных знаков, Шаг цены
    fp_provider.close_channel()  # Закрываем канал перед выходом
