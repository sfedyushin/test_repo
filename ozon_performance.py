import requests
import json
from datetime import datetime
from datetime import timedelta
# from datetime import date
import time
import os

import pandas as pd
import numpy as np
import glob
import zipfile
import psycopg2
from sqlalchemy import create_engine
# from contextlib import closing


class OzonPerformance:
    def __init__(self, account_id, client_id, client_secret,
                 day_lim=2,
                 camp_lim=2):
        self.account_id = account_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.methods = {'statistics': 'https://performance.ozon.ru:443/api/client/statistics',
                        'phrases': 'https://performance.ozon.ru:443/api/client/statistics/phrases',
                        'attribution': 'https://performance.ozon.ru:443/api/client/statistics/attribution',
                        'media': 'https://performance.ozon.ru:443/api/client/statistics/campaign/media',
                        'product': 'https://performance.ozon.ru:443/api/client/statistics/campaign/product',
                        'daily': 'https://performance.ozon.ru:443/api/client/statistics/daily',
                        'traffic': 'https://performance.ozon.ru:443/api/client/vendors/statistics'}
        self.day_lim = day_lim
        self.camp_lim = camp_lim
        #         self.date_to = str(date.today())
        #         self.date_to = '2022-06-28'
        #         self.date_from = '2022-07-01'
        try:
            self.auth = self.get_token()
        except:
            print('Нет доступа к серверу')

        try:
            self.campaigns = [camp['id'] for camp in self.get_campaigns()]
            self.objects = {}
            for camp in self.campaigns:
                self.objects[camp] = [obj['id'] for obj in self.get_objects(campaign_id=camp)]
        except:
            print('Ошибка при получении кампаний')

        self.st_camp = []
        self.st_ph = []
        self.st_attr = []
        self.st_med = None
        self.st_pr = None
        self.st_dai = None

    def get_token(self):
        url = 'https://performance.ozon.ru/api/client/token'
        head = {"Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials"
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Подключение успешно, токен получен')
            return response.json()
        else:
            print(response.text)

    def get_campaigns(self):
        """
        Возвращает список кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign'
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            print(f"Найдено {len(response.json()['list'])} кампаний")
            return response.json()['list']
        else:
            print(response.text)

    def get_objects(self, campaign_id):
        """
        Возвращает список рекламируемых объектов в кампании
        """
        url = f"https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/objects"
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response.json()['list']
        else:
            print(response.text)

    def split_data(self, camp_lim):
        """
        Разбивает данные в соответствии с ограничениями Ozon
        """
        if len(self.objects) > camp_lim:
            data = []
            for i in range(0, len(self.objects), camp_lim):
                data.append(dict(list(self.objects.items())[i:i + camp_lim]))
        else:
            data = self.objects
        return data

    def split_time(self, date_from, date_to, day_lim):
        """
        Разбивает временной промежуток в соответствии с лимитом Ozon
        """
        delta = datetime.strptime(date_to, '%Y-%m-%d') - datetime.strptime(date_from, '%Y-%m-%d')
        if delta.days > day_lim:
            tms = []
            for t in range(0, delta.days, day_lim):
                dt_fr = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t)).date())
                if (datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t + day_lim - 1)).date() >= \
                        (datetime.strptime(date_to, '%Y-%m-%d')).date():
                    dt_to = str((datetime.strptime(date_to, '%Y-%m-%d')).date())
                else:
                    dt_to = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t + day_lim - 1)).date())
                tms.append([dt_fr, dt_to])
        else:
            tms = [[date_from, date_to]]

        return tms

    def get_statistics(self, campaigns,
                       t_date_from=None,
                       t_date_to=None,
                       group_by="DATE",
                       n_attempts=5,
                       delay=3):
        """
        Возвращает статистику по кампании

        DATE — группировка по дате (по дням);
        START_OF_WEEK — группировка по неделям;
        START_OF_MONTH — группировка по месяцам.

        """
        url = self.methods['statistics']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"campaigns": campaigns,
                "dateFrom": t_date_from,
                "dateTo": t_date_to,
                "groupBy": group_by
                }

        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Статистика по кампаниям получена')
            if len(campaigns) == 1:
                return [response.json()['UUID'], 'csv']
            else:
                return [response.json()['UUID'], 'zip']
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print('statistics, статус', response.status_code)
                if response.status_code == 200:
                    print('Статистика по кампаниям получена')
                    if len(campaigns) == 1:
                        return [response.json()['UUID'], 'csv']
                    else:
                        return [response.json()['UUID'], 'zip']
                    break
                else:
                    n += 1
        else:
            print(response.text)

    def get_phrases(self, objects,
                    t_date_from=None,
                    t_date_to=None,
                    group_by="DATE",
                    n_attempts=5,
                    delay=3):
        """
        Возвращает отчет по фразам
        """
        url = self.methods['phrases']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        res = []
        for camp, obj in objects.items():
            if len(obj) != 0:
                body = {"campaigns": [camp],
                        "objects": obj,
                        "dateFrom": t_date_from,
                        "dateTo": t_date_to,
                        "groupBy": group_by
                        }
                response = requests.post(url, headers=head, data=json.dumps(body))
                if response.status_code == 200:
                    print('Статистика по фразам получена')
                    res.append([response.json()['UUID'], 'csv'])
                elif response.status_code == 429:
                    n = 0
                    while n < n_attempts:
                        time.sleep(delay)
                        response = requests.post(url, headers=head, data=json.dumps(body))
                        print('phrases, статус', response.status_code)
                        if response.status_code == 200:
                            print('Статистика по фразам получена')
                            res.append([response.json()['UUID'], 'csv'])
                            break
                        else:
                            n += 1
                else:
                    print(response.text)
        return res

    def get_attribution(self, campaigns,
                        t_date_from=None,
                        t_date_to=None,
                        group_by="DATE",
                        n_attempts=5,
                        delay=3):
        """
        Возвращает отчёт по заказам
        """
        url = self.methods['attribution']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"campaigns": campaigns,
                "dateFrom": t_date_from,
                "dateTo": t_date_to,
                "groupBy": group_by
                }
        time.sleep(delay)
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Статистика по заказам получена')
            if len(campaigns) == 1:
                return [response.json()['UUID'], 'csv']
            else:
                return [response.json()['UUID'], 'zip']
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print('attribution, статус', response.status_code)
                if response.status_code == 200:
                    print('Статистика по заказам получена')
                    if len(campaigns) == 1:
                        return [response.json()['UUID'], 'csv']
                    else:
                        return [response.json()['UUID'], 'zip']
                    break
                else:
                    n += 1
        else:
            print(response.text)

    def get_media(self, campaigns,
                  t_date_from=None,
                  t_date_to=None):
        """
        Возвращает статистику по медийным кампаниям
        """
        url = self.methods['media']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                  }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика по медиа получена')
            return response
        else:
            print(response.text)

    def get_product(self, campaigns,
                    t_date_from=None,
                    t_date_to=None):
        """
        Возвращает статистику по продуктовым кампаниям
        """
        url = self.methods['product']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                  }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика продуктовая получена')
            return response
        else:
            print(response.text)

    def get_daily(self, campaigns,
                  t_date_from=None,
                  t_date_to=None):
        """
        Возвращает дневную статистику по кампаниям
        """
        url = self.methods['daily']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json"
                }
        params = {"campaigns": campaigns,
                  "dateFrom": t_date_from,
                  "dateTo": t_date_to
                  }
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            print('Статистика дневная получена')
            return response
        else:
            print(response.text)

    def get_traffic(self, t_date_from, t_date_to, type_="TRAFFIC_SOURCES"):
        """
        Метод для запуска формирования отчёта с аналитикой внешнего трафика
        TRAFFIC_SOURCES — отчёт по источникам трафика
        ORDERS — отчёт по заказам
        """
        url = self.methods['traffic']
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"dateFrom": t_date_from,
                "dateTo": t_date_to,
                "type": type_
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Аналитика трафика получена')
            return response.json()['UUID']
        else:
            print(response.text)

    def status_traffic(self, uuid):
        """
        Возвращает информацию об отчёте
        """
        url = 'https://performance.ozon.ru:443/api/client/vendors/statistics/' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                # "Content-Type": "application/json",
                # "Accept": "application/json"
                }
        params = {'vendor': 'true'}
        response = requests.get(url, headers=head, params=params)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def get_tr_rep(self, uuid):
        """
        Получить файл отчета
        """
        # url = 'https://performance.ozon.ru:443/api/client/vendors/statistics/report?UUID=' + uuid
        url = 'https://performance.ozon.ru:443/external/api/statistics/report?UUID=' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']}
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def status_report(self, uuid):
        """
        Возвращает статус отчета
        """
        url = 'https://performance.ozon.ru:443/api/client/statistics/' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def get_report(self, uuid):
        """
        Получить файл отчета
        """
        url = 'https://performance.ozon.ru:443/api/client/statistics/report?UUID=' + uuid
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']}
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)

    def collect_data(self, date_from, date_to,
                     statistics=False, phrases=False, attribution=False, media=False, product=False, daily=False):
        data = self.split_data(camp_lim=self.camp_lim)
        time_ = self.split_time(date_from=date_from, date_to=date_to, day_lim=self.day_lim)
        self.time = time_
        self.date_from = date_from
        self.date_to = date_to
        if statistics is True:
            self.st_camp = []
        if phrases is True:
            self.st_ph = []
        if attribution is True:
            self.st_attr = []
        if media is True:
            self.st_med = self.get_media(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if product is True:
            self.st_pr = self.get_product(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        if daily is True:
            self.st_dai = self.get_daily(self.campaigns, t_date_from=date_from, t_date_to=date_to)
        try:
            for d in data:
                for t in time_:
                    if statistics is True:
                        self.st_camp.append(self.get_statistics(list(d.keys()), t_date_from=t[0], t_date_to=t[1]))
                    if phrases is True:
                        self.st_ph.append(self.get_phrases(d, t_date_from=t[0], t_date_to=t[1]))
                    if attribution is True:
                        self.st_attr.append(self.get_attribution(list(d.keys()), t_date_from=t[0], t_date_to=t[1]))
        except TimeoutError:
            print('Нет ответа от сервера')

    def save_data(self, path_,
                  statistics=False, phrases=False, attribution=False, media=False, product=False, daily=False):
        #         folder = path_
        folder = path_ + f'{self.account_id}-{self.client_id}/'
        if not os.path.isdir(folder):
            os.mkdir(folder)
        if media is True:
            if not os.path.isdir(folder + 'media'):
                os.mkdir(folder + 'media')
            name = folder + r'media/' + f"media_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_med.content)
            file.close()
            print('Сохранен', name)
        if product is True:
            if not os.path.isdir(folder + 'product'):
                os.mkdir(folder + 'product')
            name = folder + r'product/' + f"product_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_pr.content)
            file.close()
            print('Сохранен', name)
        if daily is True:
            if not os.path.isdir(folder + 'daily'):
                os.mkdir(folder + 'daily')
            name = folder + r'daily/' + f"daily_{self.date_from}-{self.date_to}.csv"
            file = open(name, 'wb')
            file.write(self.st_dai.content)
            file.close()
            print('Сохранен', name)
        if statistics is True:
            if not os.path.isdir(folder + 'statistics'):
                os.mkdir(folder + 'statistics')
            for num, camp in enumerate(self.st_camp):
                try:
                    status = ''
                    while status != 'OK':
                        time.sleep(1)
                        status = self.status_report(uuid=camp[0]).json()['state']
                        print(status)
                    report = self.get_report(uuid=camp[0])
                    name = folder + r'statistics/' + f"campaigns_{num}.{camp[1]}"
                    file = open(name, 'wb')
                    file.write(report.content)
                    file.close()
                    print('Сохранен', name)
                except:
                    continue
        if phrases is True:
            if not os.path.isdir(folder + 'phrases'):
                os.mkdir(folder + 'phrases')
            for num, ph in enumerate(self.st_ph):
                try:
                    for n_camp, phrases in enumerate(ph):
                        try:
                            status = ''
                            while status != 'OK':
                                time.sleep(1)
                                status = self.status_report(uuid=phrases[0]).json()['state']
                                print(status)
                            report = self.get_report(uuid=phrases[0])
                            name = folder + r'phrases/' + f"phrases_{num}_{n_camp}.{phrases[1]}"
                            file = open(name, 'wb')
                            file.write(report.content)
                            file.close()
                            print('Сохранен', name)
                        except:
                            continue
                except:
                    continue
        if attribution is True:
            if not os.path.isdir(folder + 'attribution'):
                os.mkdir(folder + 'attribution')
            for num, attr in enumerate(self.st_attr):
                try:
                    status = ''
                    while status != 'OK':
                        time.sleep(1)
                        status = self.status_report(uuid=attr[0]).json()['state']
                        print(status)
                    report = self.get_report(uuid=attr[0])
                    name = folder + r'attribution/' + f"attr_{num}.{attr[1]}"
                    file = open(name, 'wb')
                    file.write(report.content)
                    file.close()
                    print('Сохранен', name)
                except:
                    continue

    def get_camp_modes(self):
        """
        Доступные режимы создания рекламных кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign/available'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)

    def create_camp(self, title, from_date, to_date, daily_budget,
                    exp_strategy="DAILY_BUDGET",
                    placement="PLACEMENT_INVALID",
                    pcm="PRODUCT_CAMPAIGN_MODE_AUTO"):
        """
        Метод для создания товарной рекламной кампании с моделью оплаты за показы
        https://docs.ozon.ru/api/performance/#operation/CreateProductCampaignCPM
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign/cpm/product'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"title": title,
                "fromDate": from_date,
                "toDate": to_date,
                "dailyBudget": str(daily_budget),
                "expenseStrategy": exp_strategy,
                "placement": placement,
                "productCampaignMode": pcm
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Кампания создана')
            return response.json()
        else:
            return response.text

    def camp_activate(self, campaign_id):
        """
        Активировать рекламную кампанию
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/activate'
        response = requests.post(url, headers=head)
        if response.status_code == 200:
            print('Кампания активирована')
            return response.json()
        else:
            return response.text

    def camp_deactivate(self, campaign_id):
        """
        Деактивировать рекламную кампанию
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }

        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/deactivate'
        response = requests.post(url, headers=head)
        if response.status_code == 200:
            print('Кампания деактивирована')
            return response.json()
        else:
            return response.text

    def camp_period(self, campaign_id, date_from, date_to
                    #                     daily_budget,
                    #                     exp_str='DAILY_BUDGET'
                    ):
        """
        Метод для изменения сроков проведения кампании
        Способ распределения бюджета:
        DAILY_BUDGET — бюджет равномерно распределяется по дням;
        ASAP — быстрая открутка, бюджет не ограничен по дням.
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/period'
        body = {"fromDate": date_from,
                "toDate": date_to
                #                 "dailyBudget": daily_budget,
                #                 "expenseStrategy": exp_str
                }
        response = requests.put(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Сроки кампании обновлены')
            return response.json()
        else:
            return response.text

    def camp_budget(self, campaign_id,
                    #                     date_from,
                    #                     date_to,
                    daily_budget,
                    exp_str='DAILY_BUDGET'
                    ):
        """
        Метод для изменения ограничения дневного бюджета кампании
        Способ распределения бюджета:
        DAILY_BUDGET — бюджет равномерно распределяется по дням;
        ASAP — быстрая открутка, бюджет не ограничен по дням.
        """
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/daily_budget'
        body = {
            #                 "fromDate": date_from,
            #                 "toDate": date_to,
            "dailyBudget": daily_budget,
            "expenseStrategy": exp_str
        }
        response = requests.put(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Дневной бюджет кампании обновлен')
            return response.json()
        else:
            return response.text

    @staticmethod
    def card_bids(sku_list: list, bids_list: list, lim=500):
        """
        Для добавления в кампанию товаров с размещением в карточке товара
        Для обновления ставок у товаров в рекламной кампании с размещением в карточке товара
        """
        sku_list = sku_list[:lim]
        bids_list = bids_list[:lim]
        if len(bids_list) == 0:
            return [{'sku': a, 'bid': ''} for a in sku_list]
        elif len(sku_list) == len(bids_list):
            return [{'sku': a, 'bid': b} for a, b in zip(sku_list, bids_list)]
        else:
            print('Не правильный формат данных')

    @staticmethod
    def group_bids(sku_list, groups_list, lim=500):
        """
        Для добавления в кампанию товаров в ранее созданные группы с размещением на страницах каталога и поиска
        """
        sku_list = sku_list[:lim]
        groups_list = groups_list[:lim]
        if len(sku_list) == len(groups_list):
            return [{'sku': a, 'bid': b} for a, b in zip(sku_list, groups_list)]
        else:
            print('Не правильный формат данных')

    @staticmethod
    def phrases_bids(sku_list: list, st_w_lists: list, phrases_list: list):
        """
        Для добавления в кампанию товаров без группы с размещением на страницах каталога и поиска
        Для обновления ставок в рекламной кампании у товаров без группы с размещением на страницах каталога и поиска
        (требует доработки, не полная информация в документации)
        """
        if len(sku_list) == len(st_w_lists) == len(phrases_list):
            return [{'sku': a, 'stopWords': b, 'phrases': c} for a, b, c in zip(sku_list, st_w_lists, phrases_list)]
        else:
            print('Не правильный формат данных')

    def add_products(self, campaign_id, bids):
        """
        Добавить товары в кампанию
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"bids": bids}
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Товары добавлены')
            return response.json()
        else:
            return response.text

    def upd_bids(self, campaign_id, bids):
        """
        Обновить ставки товаров
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"bids": bids}
        response = requests.put(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Ставки обновлены')
            return response.json()
        else:
            return response.text

    def prod_list(self, campaign_id):
        """
        Список товаров кампании
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Accept": "application/json"
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)

    def del_products(self, campaign_id, sku_list: list):
        """
        Удалить товары из кампании
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/products/delete'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"sku": sku_list}
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Товар удален из кампании')
            return response.json()
        else:
            return response.text

    def add_group(self, campaign_id: str, title: str,
                  stopwords=None,
                  phrases=None
                  ):
        """
        Создать группу
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/group'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"title": title,
                "stopWords": stopwords,
                "phrases": phrases
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Группа создана')
            return response.json()
        else:
            return response.text

    def edit_group(self, campaign_id: str, group_id: str, title: str, stopwords=None, phrases=None):
        """
        Редактировать группу
        """
        url = f'https://performance.ozon.ru:443/api/client/campaign/{campaign_id}/group/{group_id}'
        head = {"Authorization": self.auth['token_type'] + ' ' + self.auth['access_token'],
                "Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"title": title,
                "stopWords": stopwords,
                "phrases": phrases
                }
        response = requests.put(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Группа обновлена')
            return response.json()
        else:
            return response.text


class DbWorking:
    def __init__(self, db_access="""host=rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net\
                                    port=6432\
                                    sslmode=verify-full\
                                    dbname=market_db\
                                    user=sfedyusnin\
                                    password=Qazwsx123Qaz\
                                    target_session_attrs=read-write"""):
        self.db_access = db_access
        # необходимые запросы к БД
        self.api_keys_resp = 'SELECT * FROM account_list'
        self.keys_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'account_list'"
        self.an_dt_resp = 'SELECT * FROM analitics_data2'
        self.an_dt_cols_resp = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'analitics_data2'"
        self.api_perf_keys_resp = "select max(id),foo.client_id_performance, client_secret_performance\
                                    from (select distinct(client_id_performance) from account_list) as foo\
                                    join account_list\
                                    on foo.client_id_performance = account_list.client_id_performance\
                                    where mp_id = 1\
                                    group by foo.client_id_performance, client_secret_performance\
                                    order by client_id_performance"
        self.product_list_resp = 'SELECT * FROM product_list'

    # db_access = """host=rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net\
    #                                    port=6432\
    #                                    sslmode=verify-full\
    #                                    dbname=market_db\
    #                                    user=sfedyusnin\
    #                                    password=Qazwsx123Qaz\
    #                                    target_session_attrs=read-write"""

    #         self.db_data = self.get_analitics_data()

    def test_db_connection(self):
        """
        Проверка доступа к БД
        """
        conn = psycopg2.connect(self.db_access)
        q = conn.cursor()
        q.execute('SELECT version()')
        print(q.fetchone())
        conn.close()

    def get_analitics_data(self):
        """
        Загружает таблицу из базы
        """
        self.db_data = pd.read_sql(self.an_dt_resp, psycopg2.connect(self.db_access))
        print('Загружена analitics_data')

    #         return self.db_data

    def get_last_date(self):
        """
        Возвращает последнюю дату записи в базе
        """
        db_data = self.db_data
        return db_data['data'].sort_values(ascending=False).values[0]

    def get_keys(self):
        """
        Загружает из базы таблицу ключей
        """
        try:
            df = pd.read_sql(self.api_keys_resp, psycopg2.connect(self.db_access))
            print('Загружены api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')

    def get_perf_keys(self):
        """
        Загружает ключи performance
        """
        try:
            df = pd.read_sql(self.api_perf_keys_resp, psycopg2.connect(self.db_access))
            print('Загружены performance_api_keys')
            return df
        except:
            print('Доступ к таблице запрещен')

    def extract_zips(self, path_, rem=False):
        """
        Распаковывает все zip в папках statistics папок аккаунтов
        """
        for folder in os.listdir(path_):
            zip_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.zip"))
            for file in zip_files:
                print(f'Распаковка {file}')
                with zipfile.ZipFile(file) as zf:
                    zf.extractall(path_ + folder + r'/statistics')
                if rem is True:
                    os.remove(file)
                    print(f'Удаление {file}')

    def stat_read_trans(self, file, api_id=None, account_id=None):
        """
        Обрабатывает датасет
        """
        data = pd.read_csv(file, sep=';')
        data = data.reset_index()

        camp = data.keys()[-1].split(',')[0].split()[-1]
        data.columns = data[0:1].values.tolist()[0]
        data.drop(index=0, inplace=True)
        data.drop(data.tail(1).index, inplace=True)

        data['api_id'] = api_id
        data['account_id'] = account_id
        data['Кампания'] = camp

        data = data[data.columns[-1:].tolist() + data.columns[:-1].tolist()]
        data = data[data.columns.dropna()]
        data = data.dropna(axis=0, how='any', thresh=10)
        return data

    def make_dataset(self, path_):
        """
        Собирает датасет
        """
        stat_data = []
        for folder in os.listdir(path_):
            csv_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.csv"))
            for file in csv_files:
                try:
                    account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
                    api_id = os.path.dirname(file).split('/')[-2].split('-')[1]
                    stat_data.append(self.stat_read_trans(file, api_id=api_id, account_id=account_id))
                except IndexError:
                    continue
        dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)
        dataset['data'] = dataset[['Дата', 'День']].fillna('nan').apply(lambda x: x[0] if x[1] == 'nan' else x[1],
                                                                        axis=1)
        dataset.drop(columns=['Дата', 'День'], inplace=True)
        dataset['name'] = dataset[['Наименование', 'Название товара']].fillna('nan').apply(
            lambda x: x[0] if x[1] == 'nan' else x[1], axis=1)
        dataset.drop(columns=['Наименование', 'Название товара'], inplace=True)
        dataset['orders'] = dataset[['Количество', 'Заказы']].fillna('').apply(lambda x: x[0] if x[1] == '' else x[1],
                                                                               axis=1)
        dataset.drop(columns=['Количество', 'Заказы'], inplace=True)
        dataset['price'] = dataset[['Цена продажи', 'Цена товара (руб.)']].fillna('').apply(
            lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Цена продажи', 'Цена товара (руб.)'], inplace=True)
        dataset['revenue'] = dataset[['Выручка (руб.)', 'Стоимость, руб.']].fillna('').apply(
            lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Выручка (руб.)', 'Стоимость, руб.'], inplace=True)
        dataset['expense'] = dataset[['Расход (руб., с НДС)', 'Расход, руб.']].fillna('').apply(
            lambda x: x[0] if x[1] == '' else x[1], axis=1)
        dataset.drop(columns=['Расход (руб., с НДС)', 'Расход, руб.'], inplace=True)

        dataset.rename(columns={'ID заказа': 'order_id', 'Номер заказа': 'order_number', 'Ozon ID': 'ozon_id',
                                'Ozon ID рекламируемого товара': 'ozon_id_ad_sku', 'Артикул': 'articul',
                                'Ставка, %': 'search_price_perc', 'Ставка, руб.': 'search_price_rur',
                                'Тип страницы': 'pagetype', 'Условие показа': 'viewtype', 'Показы': 'views',
                                'Клики': 'clicks', 'CTR (%)': 'ctr', 'Средняя ставка за 1000 показов (руб.)': 'cpm',
                                'Заказы модели': 'orders_model', 'Выручка с заказов модели (руб.)': 'revenue_model',
                                'Тип условия': 'request_type', 'Платформа': 'platfrom', 'Охват': 'audience',
                                'Баннер': 'banner', 'Средняя ставка (руб.)': 'avrg_bid', 'Кампания': 'actionnum',
                                'Расход за минусом бонусов (руб., с НДС)': 'exp_bonus'}, inplace=True)

        dataset['data'] = dataset['data'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
        #        dataset['sku'] = dataset['sku'].fillna('nan')

        for col in dataset.columns:
            if self.db_data[col].dtypes == 'float64' or self.db_data[col].dtypes == 'int64':
                dataset[col] = dataset[col].str.replace(',', '.')
                dataset[col] = dataset[col].replace(r'^\s*$', np.nan, regex=True)
                dataset[col] = dataset[col].astype(self.db_data[col].dtypes)

        return dataset

    def rem_csv(self, path_):
        """
        Удаляет файлы
        """
        for folder in os.listdir(path_):
            csv_files = glob.glob(os.path.join(path_ + folder + r'/statistics', "*.csv"))
            for file in csv_files:
                os.remove(file)
                print(f'Удаление {file}')

    def upl_to_db(self, dataset,
                  db_params='postgresql://sfedyusnin:Qazwsx123Qaz@rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net:6432/\
                  market_db',
                  table_name='analitics_data2'):
        """
        Загружает данные в БД
        Параметры подключения 'postgresql://username:password@localhost:5432/mydatabase'
        """
        engine = create_engine(db_params)
        data = dataset.drop('id', axis=1)
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print('Данные записаны в БД')

    def get_products_list(self):
        """
        Загружает из базы таблицу со списком продуктов
        """
        try:
            df = pd.read_sql(self.product_list_resp, psycopg2.connect(self.db_access))
            print('Загружена product_list')
            return df
        except:
            print('Доступ к таблице запрещен')
