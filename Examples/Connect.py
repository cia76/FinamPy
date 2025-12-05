import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время
from threading import Thread  # Запускаем поток подписки

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import ClockRequest, ClockResponse  # Время на сервере
from FinamPy.grpc.marketdata.marketdata_service_pb2 import SubscribeBarsResponse, Bar  # Подписка на минутные бары тикера
from FinamPy.grpc.marketdata.marketdata_service_pb2 import TimeFrame  # Временной интервал Финама


def on_new_bar(bars: SubscribeBarsResponse, finam_timeframe: TimeFrame.ValueType):  # Обработчик события прихода нового бара
    global last_bar, dt_last_bar  # Последний полученный бар и его дата/время
    timeframe, _, _ = fp_provider.finam_timeframe_to_timeframe(finam_timeframe)  # Временной интервал пришедших баров
    for bar in bars.bars:  # Пробегаемся по всем полученным барам
        dt_bar = datetime.fromtimestamp(bar.timestamp.seconds, fp_provider.tz_msk)  # Дата/время полученного бара
        if dt_last_bar is not None and dt_last_bar < dt_bar:  # Если время бара стало больше (предыдущий бар закрыт, новый бар открыт)
            logger.info(f'{dt_last_bar:%d.%m.%Y %H:%M:%S} '
                        f'O:{last_bar.open.value} '
                        f'H:{last_bar.high.value} '
                        f'L:{last_bar.low.value} '
                        f'C:{last_bar.close.value} '
                        f'V:{int(float(last_bar.volume.value))}')
        last_bar = bar  # Запоминаем бар
        dt_last_bar = dt_bar  # Запоминаем дату и время бара


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Connect')  # Будем вести лог
    # fp_provider = FinamPy('<Токен>')  # При первом подключении нужно передать токен
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Connect.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    # Проверяем работу запрос/ответ
    dt_local = datetime.now(fp_provider.tz_msk)  # Текущее время
    clock: ClockResponse = fp_provider.call_function(fp_provider.assets_stub.Clock, ClockRequest())  # Получаем время на сервере в виде google.protobuf.Timestamp
    dt_server = datetime.fromtimestamp(clock.timestamp.seconds + clock.timestamp.nanos / 1e9, fp_provider.tz_msk)  # Переводим google.protobuf.Timestamp в datetime
    td = dt_server - dt_local  # Разница во времени в виде timedelta
    logger.info(f'Локальное время МСК : {dt_local:%d.%m.%Y %H:%M:%S}')
    logger.info(f'Время на сервере    : {dt_server:%d.%m.%Y %H:%M:%S}')
    logger.info(f'Разница во времени  : {td}')

    # Проверяем работу подписок
    dataname = 'TQBR.SBER'  # Тикер
    tf = 'M1'  # Временной интервал

    logger.info(f'Подписка на {tf} бары тикера {dataname}')
    fp_provider.on_new_bar.subscribe(on_new_bar)  # Обработчик события прихода нового бара
    finam_board, ticker = fp_provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
    mic = fp_provider.get_mic(finam_board, ticker)  # Биржа тикера
    finam_tf, _, _ = fp_provider.timeframe_to_finam_timeframe(tf)  # Временной интервал Финам
    last_bar: Bar  # Последнего полученного бара пока нет
    dt_last_bar = None  # И даты/времени у него пока нет
    Thread(target=fp_provider.subscribe_bars_thread, name='BarsThread', args=(f'{ticker}@{mic}', finam_tf)).start()  # Создаем и запускаем поток подписки на новые бары

    # Выход
    input('Enter - выход\n')
    fp_provider.on_new_bar.unsubscribe(on_new_bar)  # Отменяем подписку на новые бары
    fp_provider.close_channel()  # Закрываем канал перед выходом
