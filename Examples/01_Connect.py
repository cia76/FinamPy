from FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Провайдер работает со всеми счетами по токену (из файла Config.py)

    board = 'TQBR'  # Код площадки
    code = 'SBER'  # Тикер

    # Проверяем работу запрос/ответ
    # TODO Ждем от Финама функцию получения времени на сервере. Пока выдаем информацию о тикере
    print(f'\nДанные тикера: {board}.{code}')
    securities = fp_provider.symbols  # Получаем информацию обо всех тикерах
    # print('Ответ от сервера:', securities)
    try:  # Пытаемся найти тикер
        si = next(item for item in securities.securities if item.board == board and item.code == code)
        print(si)
    except StopIteration:  # Если тикер не найден
        print(f'\nТикер {board}.{code} не найден')

    # Проверяем работу подписок
    print(f'\nПодписка на стакан тикера: {board}.{code}')
    print('ask - минимальная цена покупки, bid - максимальная цена продажи')
    fp_provider.on_order_book = lambda order_book: print('ask:', order_book.asks[0].price, 'bid:', order_book.bids[0].price)  # Обработчик события прихода подписки на стакан
    fp_provider.subscribe_order_book(code, board, 'orderbook1')  # Подписываемся на стакан тикера

    # Выход
    input('Enter - выход\n')
    fp_provider.unsubscribe_order_book('orderbook1', 'SBER', 'TQBR')  # Отписываемся от стакана тикера
    fp_provider.close_channel()  # Закрываем канал перед выходом
