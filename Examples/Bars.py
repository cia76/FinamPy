import logging  # Выводим лог на консоль и в файл
from datetime import datetime, timedelta  # Дата и время

from google.protobuf.timestamp_pb2 import Timestamp
from google.type.interval_pb2 import Interval

from FinamPy import FinamPy
from FinamPy.grpc.marketdata.marketdata_service_pb2 import BarsRequest, BarsResponse  # История


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Bars')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Bars.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    dataname = 'TQBR.SBER'  # Тикер
    tf = 'D1'  # Временной интервал

    finam_board, ticker = fp_provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
    mic = fp_provider.get_mic(finam_board, ticker)  # Код биржи по ISO 10383
    finam_tf, tf_range, intraday = fp_provider.timeframe_to_finam_timeframe(tf)  # Временной интервал Финам
    start_dt = fp_provider.min_history_date   # Начинаем запрос с первой возможной даты
    while start_dt <= datetime.now():  # Пока не дошли до текущей даты
        end_dt = start_dt + tf_range  # Конечную дату запроса ставим на максимальный размер от даты начала
        logger.info(f'Запрос бар с {start_dt} до {end_dt}')
        start_time = Timestamp(seconds=int(datetime.timestamp(start_dt)))  # Дату начала запроса переводим в Google Timestamp
        end_time = Timestamp(seconds=int(datetime.timestamp(end_dt)))  # Дату окончания запроса переводим в Google Timestamp
        bars_response: BarsResponse = fp_provider.call_function(
            fp_provider.marketdata_stub.Bars,
            BarsRequest(symbol=f'{ticker}@{mic}', timeframe=finam_tf, interval=Interval(start_time=start_time, end_time=end_time))
        )  # Получаем историю тикера за период
        if len(bars_response.bars) == 0:  # Если за период бар нет
            logger.info('Бары не получены')
        else:
            logger.info(f'Получено бар  : {len(bars_response.bars)}')
            for bar in bars_response.bars:
                dt_bar = datetime.fromtimestamp(bar.timestamp.seconds, fp_provider.tz_msk)  # Дата/время полученного бара
                if not intraday:  # Для дневных временнЫх интервалов и выше
                    dt_bar = dt_bar.date()  # убираем время, оставляем только дату
                logger.info(f'{dt_bar} O:{bar.open.value} H:{bar.high.value} L:{bar.low.value} C:{bar.close.value} V:{int(float(bar.volume.value))}')
        start_dt = end_dt  # Дату начала переносим на дату окончания
    fp_provider.close_channel()  # Закрываем канал перед выходом
