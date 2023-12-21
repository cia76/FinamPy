from time import time
import os.path
from google.protobuf.json_format import MessageToJson, Parse  # Будем хранить справочник в файле в формате JSON

from FinamPy import FinamPy  # Работа с сервером TRANSAQ
from FinamPy.Config import Config  # Файл конфигурации
from FinamPy.grpc.tradeapi.v1.securities_pb2 import GetSecuritiesResult  # Тип списка инструментов

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    start_time = time()  # Время начала запуска скрипта
    fp_provider = FinamPy(Config.AccessToken)  # Провайдер работает со всеми счетами по токену (из файла Config.py)
    datapath = os.path.join('..', '..', 'DataFinam', '')  # Путь сохранения файлов для Windows/Linux

    with open(f'{datapath}FINAM.txt', 'w', encoding='UTF-8') as f:  # Открываем файл на запись
        f.write(MessageToJson(fp_provider.symbols))  # Сохраняем список инструментов в файл

    symbols = GetSecuritiesResult()  # Тип списка инструментов
    with open(f'{datapath}FINAM.txt', 'r', encoding='UTF-8') as f:  # Открываем файл на чтение
        Parse(f.read(), symbols)  # Получаем список инструментов из файла, приводим к типу

    print('Список инструментов с Финама и файл списка совпадают:', fp_provider.symbols == symbols)  # Список инструментов и файл должны совпадать

    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
    fp_provider.close_channel()  # Закрываем канал перед выходом
