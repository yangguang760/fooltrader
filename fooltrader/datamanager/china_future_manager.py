# -*- coding: utf-8 -*-
import os

from fooltrader import get_exchange_cache_dir
from fooltrader.api.technical import get_trading_calendar
from fooltrader.datamanager import process_crawl
from fooltrader.spiders.chinafuture.future_shfe_spider import FutureShfeSpider
from fooltrader.spiders.chinafuture.future_cffex_spider import FutureCffexSpider
from fooltrader.spiders.chinafuture.future_dce_spider import FutureDceSpider
from fooltrader.spiders.chinafuture.future_czce_spider import FutureCzceSpider
from fooltrader.spiders.chinafuture.shfe_trading_calendar_spider import ShfeTradingCalendarSpider
from pandas import datetime


def crawl_shfe_quote():
    # 先抓历年历史数据
#    process_crawl(FutureShfeSpider, {})
    # 抓今年的交易日历
    process_crawl(ShfeTradingCalendarSpider, {})
    # 增量抓
    cache_dir = get_exchange_cache_dir(security_type='future', exchange='shfe', the_year=datetime.today().year,
                                       data_type="day_kdata")

    saved_kdata_dates = [f for f in os.listdir(cache_dir)]
    trading_dates = get_trading_calendar(security_type='future', exchange='shfe')

    the_dates = set(trading_dates) - set(saved_kdata_dates)

    process_crawl(FutureShfeSpider, {
        "trading_dates": the_dates,
        'dataType':"day_kdata"})

def crawl_cffex_quote():
    process_crawl(FutureCffexSpider, {
        'dataType':"day_kdata"})

def crawl_dce_quote():
    process_crawl(FutureDceSpider, {
        'dataType':"day_kdata"})

def crawl_czce_quote():
    process_crawl(FutureCzceSpider, {
        'dataType':"day_kdata"})
