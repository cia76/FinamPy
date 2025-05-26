import logging  # Выводим лог на консоль и в файл
from datetime import datetime, timedelta  # Дата и время

from FinamPy import FinamPy
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.interval_pb2 import Interval
from FinamPy.grpc.marketdata.marketdata_service_pb2 import BarsRequest, BarsResponse  # История


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Bars')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Bars.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    symbol = 'SBER@MISX'
    timeframe = BarsRequest.TIME_FRAME_D  # Дневной интервал

    min_date = datetime(2015, 6, 29)  # Первая дата, с которой можно получать историю
    max_date_range = {
        BarsRequest.TIME_FRAME_M1: timedelta(days=7),  # M1
        BarsRequest.TIME_FRAME_M5: timedelta(days=30),  # M5
        BarsRequest.TIME_FRAME_M15: timedelta(days=30),  # M15
        BarsRequest.TIME_FRAME_M30: timedelta(days=30),  # M30
        BarsRequest.TIME_FRAME_H1: timedelta(days=30),  # M60
        BarsRequest.TIME_FRAME_H2: timedelta(days=30),  # M120
        BarsRequest.TIME_FRAME_H4: timedelta(days=30),  # M240
        BarsRequest.TIME_FRAME_H8: timedelta(days=30),  # M480
        BarsRequest.TIME_FRAME_D: timedelta(days=365),  # D1
        BarsRequest.TIME_FRAME_W: timedelta(days=365*5),  # W1
        BarsRequest.TIME_FRAME_MN: timedelta(days=365*5),  # MN1
        BarsRequest.TIME_FRAME_QR: timedelta(days=365*5)  # MN3
    }  # Максимальный размер запроса в днях
    start_date = min_date  # Начинаем запрос с первой возможной даты
    while start_date <= datetime.now():  # Пока не дошли до текущей даты
        end_date = start_date + max_date_range[timeframe]  # Конечную дату запроса ставим на максимальный размер от даты начала
        logger.info(f'Запрос бар с {start_date} до {end_date}')
        start_time = Timestamp(seconds=int(datetime.timestamp(start_date)))  # Дату начала запроса переводим в Google Timestamp
        end_time = Timestamp(seconds=int(datetime.timestamp(end_date)))  # Дату окончания запроса переводим в Google Timestamp
        bars_response: BarsResponse = fp_provider.call_function(
            fp_provider.marketdata_stub.Bars,
            BarsRequest(symbol=symbol, timeframe=timeframe, interval=Interval(start_time=start_time, end_time=end_time))
        )  # Получаем историю тикера за период
        if len(bars_response.bars) == 0:  # Если за период бар нет
            logger.info('Бары не получены')
        else:
            logger.info(f'Первый бар    : {bars_response.bars[0]}')
            logger.info(f'Последний бар : {bars_response.bars[-1]}')
            logger.info(f'Получено бар  : {len(bars_response.bars)}')
        start_date = end_date  # Дату начала переносим на дату окончания
    fp_provider.close_channel()  # Закрываем канал перед выходом
