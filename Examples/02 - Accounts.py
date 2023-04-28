from FinamPy.FinamPy import FinamPy
from FinamPy.Config import Config

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Подключаемся к торговому счету
    for client_id in Config.ClientIds:  # Пробегаемся по всем счетам
        print(f'Учетная запись: {client_id}')
        portfolio = fp_provider.get_portfolio(client_id)  # Получаем портфель
        for position in portfolio.positions:  # Пробегаемся по всем позициям
            print(f'  - Позиция ({position.security_code}) {position.balance} @ {position.average_price:.2f} / {position.current_price:.2f}')
        print('  - Позиции + Свободные средства:')
        for currency in portfolio.currencies:
            print(f'    - {currency.balance:.2f} {currency.name}')
        print('  - Свободные средства:')
        for m in portfolio.money:
            print(f'    - {m.balance:.2f} {m.currency}')
        orders = fp_provider.get_orders(client_id).orders  # Получаем заявки
        for order in orders:  # Пробегаемся по всем заявкам
            print(f'  - Заявка номер {order.order_no} {"Покупка" if order.buy_sell == "Buy" else "Продажа"} {order.security_board}.{order.security_code} {order.quantity} @ {order.price}')
        stop_orders = fp_provider.get_stops(client_id).stops  # Получаем стоп заявки
        for stop_order in stop_orders:  # Пробегаемся по всем стоп заявкам
            print(f'  - Стоп заявка номер {stop_order.stop_id} {"Покупка" if stop_order.buy_sell == "Buy" else "Продажа"} {stop_order.security_board}.{stop_order.security_code} {stop_order.stop_loss.quantity} @ {stop_order.stop_loss.price}')

    fp_provider.close_channel()  # Закрываем канал перед выходом
