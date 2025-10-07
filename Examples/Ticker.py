import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import GetAssetRequest, GetAssetResponse  # Информация по тикеру


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Ticker')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbols = ('SBER@MISX', 'VTBR@MISX', 'SIZ5@RTSX', 'RIZ5@RTSX', 'USDRUBF@RTSX', 'CNYRUBF@RTSX')  # Кортеж тикеров формата ticker@mic

    for symbol in symbols:  # Пробегаемся по всем тикерам
        si: GetAssetResponse = fp_provider.call_function(fp_provider.assets_stub.GetAsset, GetAssetRequest(symbol=symbol, account_id=fp_provider.account_ids[0]))
        logger.info(f'Ответ от сервера: {si}')
        logger.info(f'Информация о тикере {si.board}.{si.ticker} ({si.name}, {si.type}) на бирже {si.mic}')
        logger.info(f'- Лот: {si.lot_size.value}')
        logger.info(f'- Шаг цены: {si.min_step}')
        logger.info(f'- Кол-во десятичных знаков: {si.decimals}')
    fp_provider.close_channel()  # Закрываем канал перед выходом
