import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy.FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации


logger = logging.getLogger('FinamPy.Ticker')  # Будем вести лог


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Подключаемся

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    board_code = (('TQBR', 'SBER'), ('TQBR', 'VTBR'), ('FUT', 'SiH4'), ('FUT', 'RIH4'))  # Кортеж тикеров в виде (код режима торгов, код тикера)

    securities = fp_provider.symbols  # Получаем справочник всех тикеров из провайдера
    if securities:  # Если получили тикеры
        for security_board, security_code in board_code:  # Пробегаемся по всем тикерам
            si = next((item for item in securities.securities if item.board == security_board and item.code == security_code), None)  # Пытаемся найти тикер в справочнике
            logger.info(f'Ответ от сервера: {si}' if si else f'Тикер {security_board}.{security_code} не найден')
            logger.info(f'Информация о тикере {si.board}.{si.code} ({si.short_name}, {fp_provider.markets[si.market]})')
            logger.info(f'- Валюта: {si.currency}')
            logger.info(f'- Лот: {si.lot_size}')
            decimals = si.decimals  # Кол-во десятичных знаков
            logger.info(f'- Кол-во десятичных знаков: {decimals}')
            min_step = round(10 ** -decimals * si.min_step, decimals)  # Шаг цены
            logger.info(f'- Шаг цены: {min_step}')
    else:  # Тикеры не получили
        print('Тикеры не получены')

    fp_provider.close_channel()  # Закрываем канал перед выходом
