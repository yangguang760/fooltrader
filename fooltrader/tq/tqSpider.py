from fooltrader.api.technical import get_trading_calendar
from fooltrader.transform.agg_future_dayk import agg_future_dayk
from datetime import datetime
from datetime import timedelta
from contextlib import closing
import os
from fooltrader.tq import tqdownloader
from tqsdk.api import TqApi
import logging
logging.basicConfig(level=logging.DEBUG,#控制台打印的日志级别
                    filename='yg.log',
                    filemode='a',##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    #a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    #日志格式
                    )
def scrawl_tick():
    agg = agg_future_dayk()
    logging.info("start filter existed symbols")
    trading_dates = get_trading_calendar(security_type="future",exchange="shfe")
    tdates = {}
    for i in range(len(trading_dates)):
        if i>0:
            tdates[datetime.strptime(trading_dates[i],'%Y%m%d')]=datetime.strptime(trading_dates[i-1],'%Y%m%d')
    path = "/home/yang/D/mdata/tqtick"
    filteredTradingDates = list(filter(lambda y:y>datetime(2018,11,30,0,0), map(lambda x:datetime.strptime(x,'%Y%m%d'),trading_dates)))
    logging.info("complete filter existed symbols")
    exchanges = ["shfe","cffex","dce","czce"]
    logging.info("start getting tick data")
    api = TqApi(account_id="SIM",url="ws://192.168.56.1:7777")
    for ex in exchanges:
        logging.info(ex+": start getting tick")
        currentYearData = agg.getCurrentYearData(ex)
        currentYearData = currentYearData[currentYearData['date'].isin(filteredTradingDates)]
        pathpair=list(map(lambda x:(x[1].strftime('%Y%m%d')+"-"+x[0],x[0],x[1]) ,currentYearData[['symbol','date']].values))
        for i in pathpair:
            if i[1].startswith("sc"):
                continue
            the_dir1 = os.path.join(path,ex.upper(),str(i[2].year))
            if not os.path.exists(the_dir1):
                os.makedirs(the_dir1)
            the_dir = os.path.join(path,ex.upper(),str(i[2].year),i[0]+".csv.gz")
            the_dir2 = os.path.join(path,ex.upper(),str(i[2].year),i[0]+".csv")
            # print(the_dir)
            if not os.path.exists(the_dir):
                td = tqdownloader.DataDownloader(api, symbol_list=[ex.upper()+"."+i[1]], dur_sec=0,
                        start_dt=tdates[i[2]]+timedelta(hours=17), end_dt=i[2]+timedelta(hours=13), csv_file_name=the_dir2)
                while not td.is_finished():
                    api.wait_update()
                    # print("progress:  tick:%.2f%%" %  td.get_progress())
                print("done:" + the_dir)
        logging.info(ex+": complete getting tick")