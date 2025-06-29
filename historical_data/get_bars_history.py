import logging  # Выводим лог на консоль и в файл
from datetime import datetime, timedelta  # Дата и время
import os
import yaml
import pandas as pd
import numpy as np
#import pyarrow as pa
#import pyarrow.parquet as pq

from FinamPy import FinamPy
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.interval_pb2 import Interval
from FinamPy.grpc.marketdata.marketdata_service_pb2 import BarsRequest, BarsResponse, TimeFrame  # История


class BaseBars():
    def __init__(self, start_date: str, end_date: str, out_file: str | None = None, symbol: str = 'SBER@MISX', time_frame: str = 'M1'):
        self.__pwd = f'{os.getenv("HOME")}/my_repo/FinamPy/historical_data'
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.symbol = symbol
        self.out_file = out_file if out_file else f'{self.__pwd}/{self.symbol}_bars_{time_frame}_{start_date}_{end_date}.parquet'
        self.time_frame = getattr(TimeFrame, f'TIME_FRAME_{time_frame}')

        with open('token.sec') as f:
            token = yaml.load(f, Loader=yaml.FullLoader)
            self.fp_provider = FinamPy(token['token'])  # Подключаемся ко всем торговым счетам

        self.max_date_range = {
            TimeFrame.TIME_FRAME_M1: timedelta(days=7),  # M1
            TimeFrame.TIME_FRAME_M5: timedelta(days=30),  # M5
            TimeFrame.TIME_FRAME_M15: timedelta(days=30),  # M15
            TimeFrame.TIME_FRAME_M30: timedelta(days=30),  # M30
            TimeFrame.TIME_FRAME_H1: timedelta(days=30),  # M60
            TimeFrame.TIME_FRAME_H2: timedelta(days=30),  # M120
            TimeFrame.TIME_FRAME_H4: timedelta(days=30),  # M240
            TimeFrame.TIME_FRAME_H8: timedelta(days=30),  # M480
            TimeFrame.TIME_FRAME_D: timedelta(days=365),  # D1
            TimeFrame.TIME_FRAME_W: timedelta(days=365*5),  # W1
            TimeFrame.TIME_FRAME_MN: timedelta(days=365*5),  # MN1
            TimeFrame.TIME_FRAME_QR: timedelta(days=365*5)  # MN3
        }

        handler = logging.handlers.RotatingFileHandler(f'{self.__pwd}/logging_bars.log', maxBytes=1000000, mode='w')
        logger_filter = self._get_logger_filter()
        handler.addFilter(logger_filter)
        logging.basicConfig(level=logging.INFO, handlers=[handler],
                            format='%(name)s %(asctime)s %(levelname)s %(message)s')
        logger = logging.getLogger(__name__)
        self.logger = logger

    def _get_logger_filter(self, name: str = __name__):
        '''
        функция возвращает фильтр для логгера
        '''
        class LogFilter(logging.Filter):
            '''
            Фильтр для логгера, фильтрует info сообщения от сторонних программ (например спарка, который спамит info)
            '''
            def __init__(self, name: str):
                self._name = name

            def filter(self, record):
                return (getattr(logging, record.levelname) > logging.INFO) or (record.name == self._name)

        return LogFilter(name)

    def save_data(self, data_to_save):
        self.logger.info('Сохраняем данные')
        data_to_save.to_parquet(self.out_file, compression='zstd',
                                existing_data_behavior='delete_matching', partition_cols=['partition_date'])

    def read_data(self, dates_to_load: list[int]):
        return pd.read_parquet(self.out_file, filters=[[('partition_date', '>=', int(min(dates_to_load))),
                                                      ('partition_date', '<=', int(max(dates_to_load)))]])

    def convert_bars(self, bars):
        self.logger.info('Преобразовываем бары для сохранения')
        bars_length = len(bars)
        out = np.zeros((bars_length, 7))
        for ind, bar in enumerate(bars):
            out[ind][0] = bar.open.value
            out[ind][1] = bar.high.value
            out[ind][2] = bar.low.value
            out[ind][3] = bar.close.value
            out[ind][4] = bar.volume.value
            out[ind][5] = bar.timestamp.seconds
            date = datetime.fromtimestamp(bar.timestamp.seconds)
            out[ind][6] = date.year * 10000 + date.month * 100 + date.day

        out_table = pd.DataFrame(out, columns=['open', 'high', 'low', 'close', 'volume', 'ts', 'partition_date'])

        return out_table

    def get_hist_bars(self):
        self.logger.info(f"\n\n\n{10 * '='}Начало записи баров для {self.symbol}, интервал {self.start_date} - {self.end_date} {10 * '='}\n\n\n")
        max_date_range = self.max_date_range[self.time_frame]  # Конечную дату запроса ставим на максимальный размер от даты начала
        start_date = self.start_date
        while start_date < self.end_date:
            end_date = start_date + max_date_range
            self.logger.info(f'Запрос бар для {self.symbol} с {start_date} до {end_date}')
            start_time = Timestamp(seconds=int(datetime.timestamp(start_date)))  # Дату начала запроса переводим в Google Timestamp
            end_time = Timestamp(seconds=int(datetime.timestamp(end_date)))  # Дату окончания запроса переводим в Google Timestamp
            try:
                bars_response: BarsResponse = self.fp_provider.call_function(
                    self.fp_provider.marketdata_stub.Bars,
                    BarsRequest(symbol=self.symbol, timeframe=self.time_frame, interval=Interval(start_time=start_time, end_time=end_time))
                )  # Получаем историю тикера за период
                if len(bars_response.bars) == 0:  # Если за период бар нет
                    self.logger.info(f'Бары не получены для {self.symbol} период {start_date} - {end_date}.')
                else:
                    self.logger.info(f'Бары получены, первый бар    : {bars_response.bars[0]}')
                    self.logger.info(f'Последний бар : {bars_response.bars[-1]}')
                    self.logger.info(f'Получено всего бар  : {len(bars_response.bars)}')

                    data_to_write = self.convert_bars(bars_response.bars)
                    self.save_data(data_to_write)

                start_date = end_date  # Дату начала переносим на дату окончания
            except Exception as ex:
                self.logger.error(f'Не смогли загрузить данные для {self.symbol} период {start_date} - {end_date}. Ошибка: {ex}')
                self.logger.exception('message')

        self.fp_provider.close_channel()  # Закрываем канал перед выходом
