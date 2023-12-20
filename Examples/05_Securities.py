from FinamPy.Config import Config
from FinamPy.FinamRestPy import FinamRestPy


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    frp_provider = FinamRestPy(Config.ClientIds[0], Config.AccessToken)  # Подключаемся
    securities = frp_provider.get_securities()  # Получаем список инструментов
    print(securities)

    with open("securities.txt", "w", encoding="UTF-8") as f:
        f.write(str(securities))
