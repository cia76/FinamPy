import logging  # Выводим лог на консоль и в файл
from threading import Thread  # Запускаем поток подписки
from datetime import datetime  # Дата и время

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import ClockRequest, ClockResponse  # Время на сервере
from FinamPy.grpc.marketdata.marketdata_service_pb2 import SubscribeBarsRequest, SubscribeBarsResponse, TIME_FRAME_M1, Bar  # Подписка на минутные бары тикера


def on_new_bar(bars: SubscribeBarsResponse):  # Обработчик события прихода нового бара
    global last_bar, dt_last_bar  # Последний полученный бар и его дата/время
    for bar in bars.bars:  # Пробегаемся по всем полученным барам
        dt_bar = datetime.fromtimestamp(bar.timestamp.seconds, fp_provider.tz_msk)  # Дата/время полученного бара
        if dt_last_bar is not None and dt_last_bar < dt_bar:  # Если время бара стало больше (предыдущий бар закрыт, новый бар открыт)
            logger.info(f'{dt_last_bar} O:{last_bar.open.value} H:{last_bar.high.value} L:{last_bar.low.value} C:{last_bar.close.value} V:{int(float(last_bar.volume.value))}')
        last_bar = bar  # Запоминаем бар
        dt_last_bar = dt_bar  # Запоминаем дату и время бара


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Connect')  # Будем вести лог
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
    logger.info(f'Локальное время МСК : {dt_local}')
    logger.info(f'Время на сервере    : {dt_server}')
    logger.info(f'Разница во времени  : {td}')

    # Проверяем работу подписок
    symbol = 'SBER@MISX'  # Символ инструмента
    logger.info(f'Подписка на минутные бары тикера: {symbol} с 5-и часовой историей')
    last_bar: Bar  # Последнего полученного бара пока нет
    dt_last_bar = None  # И даты/времени у него пока нет
    fp_provider.on_new_bar = on_new_bar  # Обработчик события прихода нового бара
    Thread(target=fp_provider.subscribe_bars_thread, name='BarsThread', args=(symbol, TIME_FRAME_M1)).start()  # Создаем и запускаем поток подписки на новые минутные бары

    # Выход
    input('Enter - выход\n')
    fp_provider.close_channel()  # Закрываем канал перед выходом
