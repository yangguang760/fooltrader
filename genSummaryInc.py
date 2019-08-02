from fooltrader.datamanager.china_future_manager import *
from fooltrader.transform.agg_future_dayk import agg_future_dayk
from fooltrader.settings import SUMMARY_PATH
import pandas as pd
import logging
logging.basicConfig(level=logging.DEBUG,#控制台打印的日志级别
                    filename='yg.log',
                    filemode='a',##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    #a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    #日志格式
                    )
today = pd.Timestamp.today().strftime('%Y%m%d')
import os
import subprocess
(status,output) = subprocess.getstatusoutput('tail -n 1 '+SUMMARY_PATH+ "summary.csv")
lastDate = output.split(",")[0]


logging.info("start gen summary for "+today)
agg = agg_future_dayk()
logging.info("start extract all data")
fullData = agg.getCurrentYearAllData()
logging.info("complete extract all data")
logging.info("start get summary")
summaryData = agg.getSummary(fullData,lastDate)
logging.info("done get summary")
logging.info("start gen file")
summaryData.to_csv(SUMMARY_PATH+ "summary.csv",header=False,index=False,mode='a')
logging.info("done for "+today)
