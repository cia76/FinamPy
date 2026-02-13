import logging  # Выводим лог на консоль и в файл
from datetime import date, timedelta, datetime  # Дата и время

from FinamPy import FinamPy


def get_future_on_date(base, future_date=date.today()):  # Фьючерсный контракт на дату
    if future_date.day > 15 and future_date.month in (3, 6, 9, 12):  # Если нужно переходить на следующий фьючерс
        future_date += timedelta(days=30)  # то добавляем месяц к дате
    period = 'H' if future_date.month <= 3 else 'M' if future_date.month <= 6 else 'U' if future_date.month <= 9 else 'Z'  # Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    digit = future_date.year % 10  # Последняя цифра года
    return f'SPBFUT.{base}{period}{digit}'


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Ticker')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    datanames = ('TQBR.SBER', 'TQBR.HYDR', get_future_on_date("Si"), get_future_on_date("RI"), 'SPBFUT.CNYRUBF', 'SPBFUT.IMOEXF')  # Кортеж тикеров

    for dataname in datanames:  # Пробегаемся по всем тикерам
        finam_board, ticker = fp_provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
        mic = fp_provider.get_mic(finam_board, ticker)  # Биржа тикера
        si = fp_provider.get_symbol_info(ticker, mic)  # Спецификация тикера
        logger.info(f'Информация о тикере {si.board}.{si.ticker} ({si.name}, {si.type}) на бирже {si.mic}')
        logger.info(f'- Лот: {int(float(si.lot_size.value))}')
        logger.info(f'- Шаг цены: {si.min_step / (10 ** si.decimals)}')
        logger.info(f'- Кол-во десятичных знаков: {si.decimals}')
    fp_provider.close_channel()  # Закрываем канал перед выходом
