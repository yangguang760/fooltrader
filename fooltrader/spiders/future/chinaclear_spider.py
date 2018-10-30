# -*- coding: utf-8 -*-

import os
from datetime import datetime
import pandas as pd

import scrapy
from scrapy import Request
from scrapy import signals

from fooltrader.api.quote import parse_shfe_data, parse_shfe_day_data
from fooltrader.contract.files_contract import get_exchange_cache_dir, get_exchange_cache_path
from fooltrader.utils.utils import to_timestamp


class ChinaclearSpider(scrapy.Spider):
    name = "chinaclear_spider"

    custom_settings = {
        # 'DOWNLOAD_DELAY': 2,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 8,

    }

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.trading_dates = None

    def start_requests(self):
        startDate = to_timestamp('2015-05-22')
        today = pd.Timestamp.today()
        for date in pd.date_range(start=startDate,end=today,freq='W'):
            yield Request(url="http://www.chinaclear.cn/cms-search/view.action?action=china&dateStr="+date.strftime('%Y.%m.%d'),meta={
                'the_date':date.strftime('%Y%m%d')
            },
            callback=self.download_chinaclear_data_by_date)

    def download_chinaclear_data_by_date(self, response):
        the_path = os.path.join(get_exchange_cache_dir(security_type='future',exchange='chinaclear'), response.meta['the_date'])
        tableData=response.css('td[width="40%"]').xpath('p/span/text()').extract()

        # 缓存数据
        with open(the_path, "wb") as f:
            f.write(bytes(response.meta['the_date']+"|"+"|".join(tableData)+'\n',encoding="utf8"))
            f.flush()
