import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy.FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации


logger = logging.getLogger('FinamPy.Accounts')  # Будем вести лог


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Подключаемся

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.DEBUG,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Accounts.log'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    for client_id in Config.ClientIds:  # Пробегаемся по всем счетам
        logger.info(f'Учетная запись {client_id}')
        portfolio = fp_provider.get_portfolio(client_id)  # Получаем портфель
        for position in portfolio.positions:  # Пробегаемся по всем позициям
            si = fp_provider.get_symbol_info(position.market, position.security_code)  # Ищем тикер в справочнике по рынку (не по площадке)
            entry_price = fp_provider.finam_price_to_price(si.board, position.security_code, position.average_price)  # Цена входа
            last_price = fp_provider.finam_price_to_price(si.board, position.security_code, position.current_price)  # Последняя цена
            logger.info(f'- Позиция {si.board}.{position.security_code} ({si.short_name}) {position.balance} @ {entry_price} / {last_price}')
        logger.info('- Свободные средства:')
        for m in portfolio.money:
            logger.info(f'  - {m.balance} {m.currency}')
        logger.info('- Портфель:')
        for currency in portfolio.currencies:
            logger.info(f'  - {currency.balance} {currency.name}')
        orders = fp_provider.get_orders(client_id).orders  # Получаем заявки
        for order in orders:  # Пробегаемся по всем заявкам
            logger.info(f'- Заявка номер {order.order_no} {"Покупка" if order.buy_sell == "Buy" else "Продажа"} {order.security_board}.{order.security_code} {order.quantity} @ {order.price}')
        stop_orders = fp_provider.get_stops(client_id).stops  # Получаем стоп заявки
        for stop_order in stop_orders:  # Пробегаемся по всем стоп заявкам
            logger.info(f'- Стоп заявка номер {stop_order.stop_id} {"Покупка" if stop_order.buy_sell == "Buy" else "Продажа"} {stop_order.security_board}.{stop_order.security_code} {stop_order.stop_loss.quantity} @ {stop_order.stop_loss.price}')

    fp_provider.close_channel()  # Закрываем канал перед выходом
