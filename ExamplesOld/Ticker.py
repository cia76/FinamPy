import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy.FinamPyOld import FinamPyOld  # Работа с сервером TRANSAQ


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPyOld.Ticker')  # Будем вести лог
    fp_provider = FinamPyOld()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    datanames = ('TQBR.SBER', 'TQBR.VTBR', 'FUT.SiM5', 'FUT.RIM5')  # Кортеж тикеров

    securities = fp_provider.symbols  # Получаем справочник всех тикеров из провайдера
    for dataname in datanames:  # Пробегаемся по всем тикерам
        security_board, security_code = fp_provider.dataname_to_finam_board_symbol(dataname)  # Код режима торгов и тикер
        si = next((security for security in securities.securities if security.board == security_board and security.code == security_code), None)  # Пытаемся найти тикер в справочнике
        if not si:  # Если тикер не найден
            logger.warning(f'Тикер {security_board}.{security_code} не найден')
            continue  # то переходим к следующему тикеру, дальше не продолжаем
        logger.info(f'Ответ от сервера: {si}')
        logger.info(f'Информация о тикере {si.board}.{si.code} ({si.short_name}, {fp_provider.markets[si.market]})')
        logger.info(f'- Валюта: {si.currency}')
        logger.info(f'- Лот: {si.lot_size}')
        decimals = si.decimals  # Кол-во десятичных знаков
        logger.info(f'- Кол-во десятичных знаков: {decimals}')
        min_step = round(10 ** -decimals * si.min_step, decimals)  # Шаг цены
        logger.info(f'- Шаг цены: {min_step}')

    fp_provider.close_channel()  # Закрываем канал перед выходом
