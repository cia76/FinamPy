import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import GetAssetRequest, GetAssetResponse  # Информация по тикеру


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Ticker')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Ticker.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    datanames = ('TQBR.SBER', 'TQBR.HYDR', 'SPBFUT.SiZ5', 'SPBFUT.RIZ5', 'SPBFUT.BRZ5', 'SPBFUT.CNYRUBF')  # Кортеж тикеров

    for dataname in datanames:  # Пробегаемся по всем тикерам
        finam_board, ticker = fp_provider.dataname_to_finam_board_ticker(dataname)  # Код режима торгов Финама и тикер
        mic = fp_provider.get_mic(finam_board, ticker)  # Биржа тикера
        si: GetAssetResponse = fp_provider.call_function(fp_provider.assets_stub.GetAsset, GetAssetRequest(symbol=f'{ticker}@{mic}', account_id=fp_provider.account_ids[0]))
        logger.info(f'Информация о тикере {si.board}.{si.ticker} ({si.name}, {si.type}) на бирже {si.mic}')
        logger.info(f'- Лот: {int(float(si.lot_size.value))}')
        logger.info(f'- Шаг цены: {si.min_step / (10 ** si.decimals)}')
        logger.info(f'- Кол-во десятичных знаков: {si.decimals}')
    fp_provider.close_channel()  # Закрываем канал перед выходом
