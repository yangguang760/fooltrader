from fooltrader.datamanager.china_future_manager import *
from fooltrader.transform.agg_future_dayk import agg_future_dayk
import pandas as pd
import logging
logging.basicConfig(level=logging.DEBUG,#控制台打印的日志级别
                    filename='yg.log',
                    filemode='a',##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    #a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    #日志格式
                    )
today = pd.Timestamp.today().strftime('%Y%m%d')
logging.info("start crawl job for " +today)
logging.info("start cffex")
crawl_cffex_quote()
logging.info("cffex done")
logging.info("start czce")
crawl_czce_quote()
logging.info("czce done")
logging.info("start dce")
crawl_dce_quote()
logging.info("dce done")
logging.info("start shfe")
crawl_shfe_quote()
logging.info("shfe done")
logging.info("start ine")
crawl_ine_quote()
logging.info("ine done")

