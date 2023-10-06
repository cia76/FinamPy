from datetime import datetime, timedelta
from time import time
import os.path

import pandas as pd
from FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации

from FinamPy.proto.tradeapi.v1.candles_pb2 import DayCandleTimeFrame, DayCandleInterval, IntradayCandleTimeFrame, IntradayCandleInterval
from google.type.date_pb2 import Date
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict


def save_candles_to_file(security_board='TQBR', security_codes=('SBER',), time_frame=DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1,
                         skip_first_date=False, skip_last_date=False, four_price_doji=False):
    """Получение баров, объединение с имеющимися барами в файле (если есть), сохранение баров в файл

    :param str security_board: Код площадки
    :param tuple security_codes: Коды тикеров в виде кортежа
    :param DayCandleTimeFrame|IntradayCandleTimeFrame time_frame: Временной интервал
    :param bool skip_first_date: Убрать бары на первую полученную дату
    :param bool skip_last_date: Убрать бары на последнюю полученную дату
    :param bool four_price_doji: Оставить бары с дожи 4-х цен
    """
    if time_frame == IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1:  # 1 минута
        tf = 'M1'
    elif time_frame == IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5:  # 5 минут
        tf = 'M5'
    elif time_frame == IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15:  # 15 минут
        tf = 'M15'
    elif time_frame == IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1:  # 1 час
        tf = 'M60'
    elif time_frame == DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1:  # 1 день
        tf = 'D1'
    elif time_frame == DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_W1:  # 1 неделя
        tf = 'W1'
    else:  # Если временной интервал задан неверно
        print('Временной интервал задан неверно')
        return  # то выходим, дальше не продолжаем
    intraday = tf.startswith('M')  # Внутридневные интервалы начинаются с M
    interval = IntradayCandleInterval(count=500) if intraday else DayCandleInterval(count=500)  # Нужно поставить максимальное кол-во баров. Максимум, можно поставить 500

    for security_code in security_codes:  # Пробегаемся по всем тикерам
        file_bars = None  # Дальше будем пытаться получить бары из файла
        file_name = f'{datapath}{security_board}.{security_code}_{tf}.txt'
        file_exists = os.path.isfile(file_name)  # Существует ли файл
        if file_exists:  # Если файл существует
            print(f'Получение файла {file_name}')
            file_bars = pd.read_csv(file_name, sep='\t')  # Считываем файл в DataFrame
            file_bars['datetime'] = pd.to_datetime(file_bars['datetime'], format='%d.%m.%Y %H:%M')  # Переводим дату/время в формат datetime
            file_bars.index = file_bars['datetime']  # Она и будет индексом
            last_date: datetime = file_bars.index[-1]  # Дата и время последнего бара
            print(f'- Первая запись файла: {file_bars.index[0]}')
            print(f'- Последняя запись файла: {last_date}')
            print(f'- Кол-во записей в файле: {len(file_bars)}')
            next_utc_bar_date = fp_provider.msk_to_utc_datetime(last_date + timedelta(minutes=1), True) if intraday else\
                last_date + timedelta(days=1)  # Смещаем время на возможный следующий бар по UTC
        else:  # Файл не существует
            print(f'Файл {file_name} не найден и будет создан')
            next_utc_bar_date = datetime(1990, 1, 1)  # Берем дату, когда никакой тикер еще не торговался
        print(f'Получение истории {security_board}.{security_code} {tf} из Finam')
        new_bars_list = []  # Список новых бар
        while True:  # Будем получать бары пока не получим все
            from_ = getattr(interval, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
            if intraday:  # Для интрадея datetime -> Timestamp
                date_from = Timestamp(seconds=int(next_utc_bar_date.timestamp()), nanos=next_utc_bar_date.microsecond * 1_000)
                from_.seconds = date_from.seconds
                from_.nanos = date_from.nanos
            else:  # Для дневных интервалов и выше datetime -> Date
                date_from = Date(year=next_utc_bar_date.year, month=next_utc_bar_date.month, day=next_utc_bar_date.day)
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
            new_bars_dict = MessageToDict(fp_provider.get_intraday_candles(security_board, security_code, time_frame, interval) if intraday else
                                          fp_provider.get_day_candles(security_board, security_code, time_frame, interval),
                                          including_default_value_fields=True)['candles']  # Получаем бары, переводим в словарь/список
            if len(new_bars_dict) == 0:  # Если новых бар нет
                break  # то выходим из цикла получения баров
            for new_bar in new_bars_dict:  # Пробегаемся по всем полученным барам
                # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
                # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
                dt = fp_provider.utc_to_msk_datetime(datetime.fromisoformat(new_bar['timestamp'][:-1])) if intraday else \
                    datetime(new_bar['date']['year'], new_bar['date']['month'], new_bar['date']['day'])  # Дату/время переводим из UTC в МСК
                open_ = round(int(new_bar['open']['num']) * 10 ** -int(new_bar['open']['scale']), int(new_bar['open']['scale']))
                high = round(int(new_bar['high']['num']) * 10 ** -int(new_bar['high']['scale']), int(new_bar['high']['scale']))
                low = round(int(new_bar['low']['num']) * 10 ** -int(new_bar['low']['scale']), int(new_bar['low']['scale']))
                close = round(int(new_bar['close']['num']) * 10 ** -int(new_bar['close']['scale']), int(new_bar['close']['scale']))
                volume = new_bar['volume']
                new_bars_list.append({'datetime': dt, 'open': open_, 'high': high, 'low': low, 'close': close, 'volume': volume})
            print(new_bars_list[-1]['datetime'])  # Последняя дата и время полученных баров
            next_utc_bar_date = fp_provider.msk_to_utc_datetime(new_bars_list[-1]['datetime'] + timedelta(minutes=1), True) if intraday else\
                new_bars_list[-1]['datetime'] + timedelta(days=1)  # Смещаем время на возможный следующий бар по UTC
        if len(new_bars_list) == 0:  # Если новых записей нет
            print('Новых записей нет')
            continue  # то переходим к следующему тикеру, дальше не продолжаем
        pd_bars = pd.DataFrame(new_bars_list)  # Список новых бар -> DataFrame
        pd_bars.index = pd_bars['datetime']  # В индекс ставим дату/время
        pd_bars = pd_bars[['datetime', 'open', 'high', 'low', 'close', 'volume']]  # Отбираем нужные колонки. Дата и время нужна, чтобы не удалять одинаковые OHLCV на разное время
        if not file_exists and skip_first_date:  # Если файла нет, и убираем бары на первую дату
            len_with_first_date = len(pd_bars)  # Кол-во баров до удаления на первую дату
            first_date = pd_bars.index[0].date()  # Первая дата
            pd_bars.drop(pd_bars[(pd_bars.index.date == first_date)].index, inplace=True)  # Удаляем их
            print(f'- Удалено баров на первую дату {first_date}: {len_with_first_date - len(pd_bars)}')
        if skip_last_date:  # Если убираем бары на последнюю дату
            len_with_last_date = len(pd_bars)  # Кол-во баров до удаления на последнюю дату
            last_date = pd_bars.index[-1].date()  # Последняя дата
            pd_bars.drop(pd_bars[(pd_bars.index.date == last_date)].index, inplace=True)  # Удаляем их
            print(f'- Удалено баров на последнюю дату {last_date}: {len_with_last_date - len(pd_bars)}')
        if not four_price_doji:  # Если удаляем дожи 4-х цен
            len_with_doji = len(pd_bars)  # Кол-во баров до удаления дожи
            pd_bars.drop(pd_bars[(pd_bars.high == pd_bars.low)].index, inplace=True)  # Удаляем их по условия High == Low
            print('- Удалено дожи 4-х цен:', len_with_doji - len(pd_bars))
        if len(pd_bars) == 0:  # Если нечего объединять
            print('Новых записей нет')
            continue  # то переходим к следующему тикеру, дальше не продолжаем
        print(f'- Первая запись в Finam: {pd_bars.index[0]}')
        print(f'- Последняя запись в Finam: {pd_bars.index[-1]}')
        print(f'- Кол-во записей в Finam: {len(pd_bars)}')
        if file_exists:  # Если файл существует
            pd_bars = pd.concat([file_bars, pd_bars]).drop_duplicates(keep='last').sort_index()  # Объединяем файл с данными из Finam, убираем дубликаты, сортируем заново
        pd_bars = pd_bars[['open', 'high', 'low', 'close', 'volume']]  # Отбираем нужные колонки. Дата и время будет экспортирована как индекс
        pd_bars.to_csv(file_name, sep='\t', date_format='%d.%m.%Y %H:%M')
        print(f'- В файл {file_name} сохранено записей: {len(pd_bars)}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    start_time = time()  # Время начала запуска скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Провайдер работает со всеми счетами по токену (из файла Config.py)

    security_board = 'TQBR'  # Акции ММВБ
    security_codes = ('SBER', 'VTBR', 'GAZP', 'MTLR', 'LKOH', 'PLZL', 'SBERP', 'BSPB', 'POLY', 'RNFT',
                      'GMKN', 'AFLT', 'NVTK', 'TATN', 'YNDX', 'MGNT', 'ROSN', 'AFKS', 'NLMK', 'ALRS',
                      'MOEX', 'SMLT', 'MAGN', 'CHMF', 'CBOM', 'MTLRP', 'SNGS', 'BANEP', 'MTSS', 'IRAO',
                      'SNGSP', 'SELG', 'UPRO', 'RUAL', 'TRNFP', 'FEES', 'SGZH', 'BANE', 'PHOR', 'PIKK')  # TOP 40 акций ММВБ
    # security_codes = ('SBER',)  # Для тестов
    datapath = os.path.join('..', '..', 'DataFinam', '')  # Путь сохранения файлов для Windows/Linux

    skip_last_date = True  # Если получаем данные внутри сессии, то не берем бары за дату незавершенной сессии
    # skip_last_date = False  # Если получаем данные, когда рынок не работает, то берем все бары
    save_candles_to_file(security_board, security_codes, four_price_doji=True)  # Дневные бары получаем всегда все, т.к. выдаются только завершенные бары
    save_candles_to_file(security_board, security_codes, IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1, skip_last_date=skip_last_date)  # часовые бары
    save_candles_to_file(security_board, security_codes, IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15, skip_last_date=skip_last_date)  # 15-и минутные бары
    save_candles_to_file(security_board, security_codes, IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5, skip_last_date=skip_last_date)  # 5-и минутные бары
    save_candles_to_file(security_board, security_codes, IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1, skip_last_date=skip_last_date, four_price_doji=True)  # минутные бары

    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
    fp_provider.close_channel()  # Закрываем канал перед выходом
