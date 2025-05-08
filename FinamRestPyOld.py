from typing import Any

from requests import post, get, delete  # Запросы/ответы от сервера запросов
from json import loads  # Ответы принимаются в виде JSON сообщений


class FinamRestPy:
    """Работа с Comon Trade REST Api из Python https://finamweb.github.io/trade-api-docs/category/rest-api"""
    server = 'https://trade-api.finam.ru'  # Сервер для исполнения вызовов

    def default_handler(self, response=None):
        """Пустой обработчик события по умолчанию. Его можно заменить на пользовательский"""
        pass

    # Функции для запросов/ответов

    def get_headers(self):
        """Получение хедеров для запросов"""
        return {'accept': 'text/plain', 'X-Api-Key': self.access_token}

    def check_result(self, response):
        """Анализ результата запроса

        :param response response: Результат запроса
        :return: Справочник из JSON, текст, None в случае веб ошибки
        """
        if response.status_code != 200:  # Если статус ошибки
            self.OnError(f'Ошибка сервера: {response.status_code} {response.content.decode("utf-8")} {response.request}')  # Событие ошибки
            return None  # то возвращаем пустое значение
        content = loads(response.content.decode('utf-8'))  # Декодируем полученное значение JSON в справочник
        error = content['error']  # Данные об ошибке
        if error:  # Если произошла ошибка
            self.OnError(f'Ошибка запроса: {error["code"]} {error["message"]} {error["data"]} {response.request}')  # Событие ошибки
            return None  # то возвращаем пустое значение
        return content['data']  # Возвращаем полученное значение

    # Инициализация и вход

    def __init__(self, client_id, access_token):
        """Инициализация

        :param str client_id: Идентификатор торгового счёта
        :param str access_token: Торговый токен доступа
        """
        self.client_id = client_id  # Идентификатор торгового счёта
        self.access_token = access_token  # Торговый токен доступа
        self.OnError = self.default_handler  # Ошибка

    def __enter__(self):
        """Вход в класс, например, с with"""
        return self

    # AccessTokens

    def check_access_token(self):
        """Проверка токена"""
        return self.check_result(get(url=f'{self.server}/api/v1/access-tokens/check', headers=self.get_headers()))

    # Заявки / Orders (https://finamweb.github.io/trade-api-docs/rest-api/orders)

    def create_order(self, security_board, security_code, buy_sell, quantity, use_credit, price, property,
                     condition_type, condition_price, condition_time, valid_type, valid_time):
        """Создать новую заявку

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param str buy_sell: Направление сделки
            'Buy' - покупка
            'Sell' - продажа
        :param int quantity: Количество лотов инструмента для заявки
        :param bool use_credit: Использовать кредит. Недоступно для срочного рынка
        :param float price: Цена заявки. 0 для рыночной заявки
        :param str property: Поведение заявки при выставлении в стакан
            'PutInQueue' - Неисполненная часть заявки помещается в очередь заявок Биржи
            'CancelBalance' - (FOK) Сделки совершаются только в том случае, если заявка может быть удовлетворена полностью
            'ImmOrCancel' - (IOC) Неисполненная часть заявки снимается с торгов
        :param str condition_type: Типы условных ордеров
            'Bid' - Лучшая цена покупки
            'BidOrLast' - Лучшая цена покупки или сделка по заданной цене и выше
            'Ask' - Лучшая цена продажи
            'AskOrLast' - Лучшая цена продажи или сделка по заданной цене и ниже
            'Time' - По времени (valid_type)
            'CovDown' - Обеспеченность ниже заданной
            'CovUp' - Обеспеченность выше заданной
            'LastUp' - Сделка на рынке по заданной цене или выше
            'LastDown' - Сделка на рынке по заданной цене или ниже
        :param float condition_price: Значение цены для условия
        :param str condition_time: Время, когда заявка была отменена на сервере. В UTC
        :param str valid_type: Установка временнЫх рамок действия заявки
            'TillEndSession' - До окончания текущей сессии
            'TillCancelled' - До отмены
            'ExactTime' - До заданного времени (valid_time)
        :param str valid_time: Время, когда заявка была отменена на сервере. В UTC
        """
        params = {'clientId': self.client_id,
                  'securityBoard': security_board,
                  'securityCode': security_code,
                  'buySell': buy_sell,
                  'quantity': quantity,
                  'useCredit': use_credit,
                  'price': price,
                  'property': property,
                  'condition':
                      {'type': condition_type,
                       'price': condition_price,
                       'time': condition_time},
                  'validBefore':
                      {'type': valid_type,
                       'time': valid_time}}
        return self.check_result(post(url=f'{self.server}/api/v1/orders', params=params, headers=self.get_headers()))

    def delete_order(self, transaction_id):
        """Отменяет заявку

        :param int transaction_id: Идентификатор транзакции, который может быть использован для отмены заявки или определения номера заявки в сервисе событий
        """
        params = {'ClientId': self.client_id, 'TransactionId': transaction_id}
        return self.check_result(delete(url=f'{self.server}/api/v1/orders', params=params, headers=self.get_headers()))

    def get_orders(self, include_matched=True, include_canceled=True, include_active=True):
        """Возвращает список заявок

        :param bool include_matched: Вернуть исполненные заявки
        :param bool include_canceled: Вернуть отмененные заявки
        :param bool include_active: Вернуть активные заявки
        """
        params = {'ClientId': self.client_id,
                  'IncludeMatched': include_matched,
                  'IncludeCanceled': include_canceled,
                  'IncludeActive': include_active}
        return self.check_result(get(url=f'{self.server}/api/v1/orders', params=params, headers=self.get_headers()))

    # Инструменты / Securities (https://finamweb.github.io/trade-api-docs/rest-api/securities)

    def get_securities(self):
        """Справочник инструментов"""
        return self.check_result(get(url=f'{self.server}/api/v1/securities', headers=self.get_headers()))

    # Портфели / Portfolio (https://finamweb.github.io/trade-api-docs/rest-api/portfolios)

    def get_portfolio(self, include_currencies=True, include_money=True, include_positions=True, include_max_buy_sell=True):
        """Возвращает портфель

        :param bool include_currencies: Валютные позиции
        :param bool include_money: Денежные позиции
        :param bool include_positions: Позиции DEPO
        :param bool include_max_buy_sell: Лимиты покупки и продажи
        """
        params = {'ClientId': self.client_id,
                  'Content.IncludeCurrencies': include_currencies,
                  'Content.IncludeMoney': include_money,
                  'Content.IncludePositions': include_positions,
                  'Content.IncludeMaxBuySell': include_max_buy_sell}
        return self.check_result(get(url=f'{self.server}/api/v1/portfolio', params=params, headers=self.get_headers()))

    # Стоп-заявки / Stops (https://finamweb.github.io/trade-api-docs/rest-api/stops)

    def create_stop_order(self, security_board, security_code, buy_sell,
                          sl_activation_price, sl_price, sl_market_price, sl_value, sl_units, sl_time, sl_use_credit,
                          tp_activation_price, tp_correction_price_value, tp_correction_price_units, tp_spread_price_value, tp_spread_price_units,
                          tp_market_price, tp_quantity_value, tp_quantity_units, tp_time, tp_use_credit,
                          expiration_date, link_order, valid_type, valid_time):
        """Выставляет стоп-заявку

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param str buy_sell: Направление сделки
            'Buy' - покупка
            'Sell' - продажа
        :param float sl_activation_price: Цена активации
        :param float sl_price: Цена заявки
        :param bool sl_market_price: По рынку
        :param float sl_value: Значение объема стоп-заявки
        :param str sl_units: Единицы объема стоп-заявки
            'Percent' - Процент
            'Lots' - Лоты
        :param int sl_time: Защитное время, сек.
        :param bool sl_use_credit: Использовать кредит
        :param float tp_activation_price: Цена активации
        :param float tp_correction_price_value: Значение цены стоп-заявки
        :param str tp_correction_price_units: Единицы цены стоп-заявки
            'Percent' - Процент
            'Pips' - Шаги цены
        :param float tp_spread_price_value: Значение цены стоп-заявки
        :param str tp_spread_price_units: Единицы цены стоп-заявки
            'Percent' - Процент
            'Pips' - Шаги цены
        :param bool tp_market_price: По рынку
        :param float tp_quantity_value: Значение объема стоп-заявки
        :param str tp_quantity_units: Единицы объема стоп-заявки
            'Percent' - Процент
            'Lots' - Лоты
        :param int tp_time: Защитное время, сек.
        :param bool tp_use_credit: Использовать кредит
        :param str expiration_date: Время, когда заявка была отменена на сервере. В UTC
        :param int link_order: Биржевой номер связанной (активной) заявки
        :param str valid_type: Установка временнЫх рамок действия заявки
            'TillEndSession' - До окончания текущей сессии
            'TillCancelled' - До отмены
            'ExactTime' - До заданного времени (valid_time)
        :param str valid_time: Время, когда заявка была отменена на сервере. В UTC
        """
        params = {'clientId': self.client_id,
                  'securityBoard': security_board,
                  'securityCode': security_code,
                  'buySell': buy_sell,
                  'stopLoss':
                      {'activationPrice': sl_activation_price,
                       'price': sl_price,
                       'marketPrice': sl_market_price,
                       'quantity':
                           {'value': sl_value,
                            'units': sl_units},
                       'time': sl_time,
                       'useCredit': sl_use_credit},
                  'takeProfit':
                      {'activationPrice': tp_activation_price,
                       'correctionPrice':
                           {'value': tp_correction_price_value,
                            'units': tp_correction_price_units},
                       'spreadPrice':
                           {'value': tp_spread_price_value,
                            'units': tp_spread_price_units},
                       'marketPrice': tp_market_price,
                       'quantity':
                           {'value': tp_quantity_value,
                            'units': tp_quantity_units},
                       'time': tp_time,
                       'useCredit': tp_use_credit},
                  'expirationDate': expiration_date,
                  'linkOrder': link_order,
                  'validBefore':
                      {'type': valid_type,
                       'time': valid_time}}
        return self.check_result(post(url=f'{self.server}/api/v1/orders', params=params, headers=self.get_headers()))

    def delete_stop_order(self, stop_id):
        """Снимает стоп-заявку

        :param int stop_id: Идентификатор стоп-заявки
        """
        params = {'ClientId': self.client_id, 'StopId': stop_id}
        return self.check_result(delete(url=f'{self.server}/api/v1/stops', params=params, headers=self.get_headers()))

    def get_stop_orders(self, include_executed=True, include_canceled=True, include_active=True):
        """Возвращает список стоп-заявок

        :param bool include_executed: Вернуть исполненные стоп-заявки
        :param bool include_canceled: Вернуть отмененные стоп-заявки
        :param bool include_active: Вернуть активные стоп-заявки
        """
        params = {'ClientId': self.client_id,
                  'IncludeExecuted': include_executed,
                  'IncludeCanceled': include_canceled,
                  'IncludeActive': include_active}
        return self.check_result(get(url=f'{self.server}/api/v1/orders', params=params, headers=self.get_headers()))

    # Свечи / Candles (https://finamweb.github.io/trade-api-docs/rest-api/candles)

    def get_day_candles(self, security_board, security_code, time_frame, date_from=None, date_to=None, count=None):
        """Запрос дневных/недельных свечей

        - Максимальный интервал: 365 дней
        - Максимальное кол-во запросов в минуту: 120

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param str time_frame: Временной интервал дневной свечи. 'D1' - 1 день, 'W1' - 1 неделя
        :param datetime date_from: Дата начала в формате yyyy-MM-dd в часовом поясе UTC
        :param datetime date_to: Дата окончания в формате yyyy-MM-dd в часовом поясе UTC
        :param int count: Кол-во свечей. Максимум 500
        """
        params: dict[str, Any] = {'securityBoard': security_board, 'securityCode': security_code, 'timeFrame': time_frame}
        if date_from:  # Если указана дата начала
            params['from'] = date_from  # то выставляем ее
        if date_to:  # Если указана дата окончания
            params['to'] = date_to  # то выставляем ее
        if count:  # Если указано кол-во свечей
            params['count'] = count  # то выставляем их
        return self.check_result(get(url=f'{self.server}/api/v1/day-candles', params=params, headers=self.get_headers()))

    def get_intraday_candles(self, security_board, security_code, time_frame, date_from=None, date_to=None, count=None):
        """Запрос внутридневных свечей

        - Максимальный интервал: 30 дней
        - Максимальное кол-во запросов в минуту: 120

        :param str security_board: Режим торгов
        :param str security_code: Тикер инструмента
        :param str time_frame: Временной интервал внутридневной свечи 'M1' - 1 минута, 'M5' - 5 минут, 'M15' - 15 минут, 'H1' - 1 час
        :param datetime date_from: Дата начала в формате yyyy-MM-ddTHH:mm:ssZ в часовом поясе UTC
        :param datetime date_to: Дата окончания в формате yyyy-MM-ddTHH:mm:ssZ в часовом поясе UTC
        :param int count: Кол-во свечей. Максимум 500
        """
        params: dict[str, Any] = {'securityBoard': security_board, 'securityCode': security_code, 'timeFrame': time_frame}
        if date_from:  # Если указана дата начала
            params['from'] = date_from  # то выставляем ее
        if date_to:  # Если указана дата окончания
            params['to'] = date_to  # то выставляем ее
        if count:  # Если указано кол-во свечей
            params['count'] = count  # то выставляем их
        return self.check_result(
            get(url=f'{self.server}/api/v1/intraday-candles', params=params, headers=self.get_headers()))
