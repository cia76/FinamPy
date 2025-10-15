import logging  # Выводим лог на консоль и в файл
from datetime import datetime  # Дата и время

from FinamPy import FinamPy
from FinamPy.grpc.assets.assets_service_pb2 import GetAssetRequest, GetAssetResponse  # Информация по тикеру
from FinamPy.grpc.accounts.accounts_service_pb2 import GetAccountRequest, GetAccountResponse  # Счет
from FinamPy.grpc.orders.orders_service_pb2 import OrdersRequest, OrdersResponse, ORDER_STATUS_NEW, ORDER_TYPE_LIMIT  # Заявки
from FinamPy.grpc.side_pb2 import SIDE_BUY  # Направление заявки


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    logger = logging.getLogger('FinamPy.Accounts')  # Будем вести лог
    fp_provider = FinamPy()  # Подключаемся ко всем торговым счетам

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Формат сообщения
                        datefmt='%d.%m.%Y %H:%M:%S',  # Формат даты
                        level=logging.INFO,  # Уровень логируемых событий NOTSET/DEBUG/INFO/WARNING/ERROR/CRITICAL
                        handlers=[logging.FileHandler('Accounts.log', encoding='utf-8'), logging.StreamHandler()])  # Лог записываем в файл и выводим на консоль
    logging.Formatter.converter = lambda *args: datetime.now(tz=fp_provider.tz_msk).timetuple()  # В логе время указываем по МСК

    for account_id in fp_provider.account_ids:  # Пробегаемся по всем счетам
        logger.info(f'Номер счета {account_id}')
        account: GetAccountResponse = fp_provider.call_function(fp_provider.accounts_stub.GetAccount, GetAccountRequest(account_id=account_id))  # Получаем счет

        for position in account.positions:  # Пробегаемся по всем позициям
            si: GetAssetResponse = fp_provider.call_function(fp_provider.assets_stub.GetAsset, GetAssetRequest(symbol=position.symbol, account_id=fp_provider.account_ids[0]))
            logger.info(f'- Позиция {si.board}.{si.ticker} ({si.name}) {int(float(position.quantity.value))} @ {float(position.average_price.value)} / {float(position.current_price.value)}')

        logger.info('- Свободные средства:')
        for cash in account.cash:
            logger.info(f'  - {round(cash.units + cash.nanos * 10**-9, 2)} {cash.currency_code}')
        logger.info(f'- Нереализованная прибыль: {round(float(account.unrealized_profit.value), 2)}')
        logger.info(f'- Итого: {round(float(account.equity.value), 2)}')

        orders: OrdersResponse = fp_provider.call_function(fp_provider.orders_stub.GetOrders, OrdersRequest(account_id=account_id))  # Получаем заявки
        for order in orders.orders:  # Пробегаемся по всем заявкам
            if order.status == ORDER_STATUS_NEW:  # Если заявка еще не исполнилась
                si: GetAssetResponse = fp_provider.call_function(fp_provider.assets_stub.GetAsset, GetAssetRequest(symbol=order.order.symbol, account_id=fp_provider.account_ids[0]))
                order_type = 'Заявка' if order.order.type == ORDER_TYPE_LIMIT else 'Стоп заявка'
                price = float(order.order.limit_price.value) if order.order.type == ORDER_TYPE_LIMIT else float(order.order.stop_price)  # Цена для лимитной и стоп заявок
                order_side = "Покупка" if order.order.side.buy_sell == SIDE_BUY else "Продажа"
                logger.info(f'- {order_type} номер {order.order_id} {order_side} {si.board}.{si.ticker} ({si.name}) {order.order.quantity} @ {price}')

    fp_provider.close_channel()  # Закрываем канал перед выходом
