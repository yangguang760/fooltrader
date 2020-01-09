# -*- coding: utf-8 -*-
import os

from fooltrader import get_exchange_cache_dir
from fooltrader.api.technical import get_trading_calendar
from fooltrader.datamanager import process_crawl
from fooltrader.spiders.chinafuture.future_shfe_spider import FutureShfeSpider
from fooltrader.spiders.chinafuture.future_ine_spider import FutureIneSpider
from fooltrader.spiders.chinafuture.future_cffex_spider import FutureCffexSpider
from fooltrader.spiders.chinafuture.future_dce_spider import FutureDceSpider
from fooltrader.spiders.chinafuture.future_czce_spider import FutureCzceSpider
from fooltrader.spiders.chinafuture.shfe_trading_calendar_spider import ShfeTradingCalendarSpider
from pandas import datetime
import pandas as pd
import fushare

from fooltrader.contract.files_contract import get_exchange_cache_dir, get_exchange_cache_path
from fooltrader.utils.utils import to_timestamp

def crawl_rollYield_And_Spread():
    cache_dir = get_exchange_cache_dir(security_type='future', exchange='shfe',
                                       data_type="day_kdata")
    today = pd.Timestamp.today()
    calendar = fushare.cons.get_calendar()
    filteredCalendar = list(filter(lambda x:datetime.strptime(x,'%Y%m%d')<=today,calendar))
    for date in filteredCalendar:
        the_dir=get_exchange_cache_path(security_type='future',exchange='shfe',the_date=to_timestamp(date),data_type='misc')
        datet = date
        if not os.path.exists(the_dir):
            # rydf = fushare.get_rollYield_bar(type="var",date=datet)
            # rydf.to_csv(the_dir+'rollYeild'+datet+'.csv')
            try:
                spdf=fushare.get_spotPrice(datet)
                spdf.to_csv(the_dir+'spotPrice'+datet+'.csv')
            except BaseException as e:
                print("not downloaded for "+datet)
            # invdf = fushare.get_rank_sum(start=date,end=datet)
            # invdf.to_csv(the_dir+'rank'+datet+'.csv')
            # rcptdf = fushare.get_reciept(datet)
            # rcptdf.to_csv(the_dir+'recipt'+datet+'.csv')

def crawl_shfe_quote():
    # 先抓历年历史数据
#    process_crawl(FutureShfeSpider, {})
    # 抓今年的交易日历
    # process_crawl(ShfeTradingCalendarSpider, {})
    # 增量抓
    # cache_dir = get_exchange_cache_dir(security_type='future', exchange='shfe', the_year=datetime.today().year,
    #                                    data_type="day_kdata")

    # saved_kdata_dates = [f for f in os.listdir(cache_dir)]
    # trading_dates = get_trading_calendar(security_type='future', exchange='shfe')

    # the_dates = set(trading_dates) - set(saved_kdata_dates)

    process_crawl(FutureShfeSpider, {
        # "trading_dates": the_dates,
        'dataType':"day_kdata"})
    # process_crawl(FutureShfeSpider, {
    #     'dataType':"inventory"})

def crawl_ine_quote():
    # 先抓历年历史数据
    #    process_crawl(FutureShfeSpider, {})
    # 抓今年的交易日历
    # process_crawl(ShfeTradingCalendarSpider, {})
    # 增量抓
    # cache_dir = get_exchange_cache_dir(security_type='future', exchange='shfe', the_year=datetime.today().year,
    #                                    data_type="day_kdata")

    # saved_kdata_dates = [f for f in os.listdir(cache_dir)]
    # trading_dates = get_trading_calendar(security_type='future', exchange='shfe')

    # the_dates = set(trading_dates) - set(saved_kdata_dates)

    process_crawl(FutureIneSpider, {
        # "trading_dates": the_dates,
        'dataType':"day_kdata"})
    # process_crawl(FutureShfeSpider, {
    #     'dataType':"inventory"})

def crawl_cffex_quote():
    process_crawl(FutureCffexSpider, {
        'dataType':"day_kdata"})
    process_crawl(FutureCffexSpider, {
        'dataType':"inventory"})

def crawl_dce_quote():
    process_crawl(FutureDceSpider, {
        'dataType':"day_kdata"})
    process_crawl(FutureDceSpider, {
        'dataType':"inventory"})

def crawl_czce_quote():
    process_crawl(FutureCzceSpider, {
        'dataType':"day_kdata"})
    process_crawl(FutureCzceSpider, {
        'dataType':"inventory"})
