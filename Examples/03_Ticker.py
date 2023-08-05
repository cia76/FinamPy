from FinamPy.FinamPy import FinamPy
from FinamPy.Config import Config


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Подключаемся

    symbols = (('TQBR', 'SBER'), ('TQBR', 'VTBR'), ('FUT', 'SiU3'), ('FUT', 'RIU3'))  # Кортеж тикеров в виде (код площадки, код тикера)

    print('Получаем информацию обо всех тикерах (займет несколько секунд)...')
    securities = fp_provider.get_securities()  # Получаем информацию обо всех тикерах
    # print('Ответ от сервера:', securities)
    if securities:  # Если получили тикеры
        for board, code in symbols:  # Пробегаемся по всем тикерам
            try:  # Пытаемся найти тикер
                si = next(item for item in securities.securities if item.board == board and item.code == code)
                # print('Ответ от сервера:', si)
                print(f'\nИнформация о тикере {si.board}.{si.code} ({si.short_name}, {fp_provider.markets[si.market]}):')
                print(f'Валюта: {si.currency}')
                print(f'Лот: {si.lot_size}')
                decimals = si.decimals  # Кол-во десятичных знаков
                print(f'Кол-во десятичных знаков: {decimals}')
                min_step = 10 ** -decimals * si.min_step  # Шаг цены
                print(f'Шаг цены: {min_step}')
            except StopIteration:  # Если тикер не найден
                print(f'\nТикер {board}.{code} не найден')
    else:  # Тикеры не получили
        print('Тикеры не получены')
    fp_provider.close_channel()  # Закрываем канал перед выходом
