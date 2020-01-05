from fooltrader.api.technical import get_trading_calendar
#from fooltrader.tq.tqdownloader import DataDownloader
from tqsdk.tools import DataDownloader
from fooltrader.transform.agg_future_dayk import agg_future_dayk
from fooltrader.contract.files_contract import get_exchange_cache_dir, get_exchange_cache_path
from fooltrader.settings import TICK_PATH
from datetime import datetime
from datetime import timedelta
from contextlib import closing
import os
from multiprocessing import Pool
from tqsdk.api import TqApi,TqSim
import logging
import gc
import gzip
logging.basicConfig(level=logging.DEBUG,#控制台打印的日志级别
                    filename='yg.log',
                    filemode='a',##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    #a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    #日志格式
                    )

def scrawl_day_tick(date,ex):
    agg = agg_future_dayk()
    logging.info("start filter existed symbols")
    path = TICK_PATH
    logging.info("start getting tick data")
    api = TqApi(account=TqSim())
    logging.info(ex+": start getting tick")
    currentYearData = agg.getCurrentYearData(ex)
    currentYearData = currentYearData[currentYearData['date']==date]
    pathpair=list(map(lambda x:(x[1].strftime('%Y%m%d')+"-"+x[0],x[0],datetime.utcfromtimestamp(x[1].timestamp())) ,currentYearData[['symbol','date']].values))
    trading_dates = get_trading_calendar(security_type="future",exchange="shfe")
    tdates = {}
    for i in range(len(trading_dates)):
        if i>0:
            tdates[datetime.strptime(trading_dates[i],'%Y%m%d')]=datetime.strptime(trading_dates[i-1],'%Y%m%d')
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
            td =DataDownloader(api, symbol_list=[ex.upper()+"."+i[1]], dur_sec=0,
                                             start_dt=tdates[i[2]]+timedelta(hours=17), end_dt=i[2]+timedelta(hours=15), csv_file_name=the_dir2)
            while not td.is_finished():
                api.wait_update()
                # print("progress:  tick:%.2f%%" %  td.get_progress())
            print("done:" + the_dir)
    logging.info(ex+": complete getting tick")

def scrawl_single_tick(i,path,ex,tdates):
    the_dir1 = os.path.join(path,ex.upper(),str(i[2].year))
    if not os.path.exists(the_dir1):
        os.makedirs(the_dir1)
    the_dir = os.path.join(path,ex.upper(),str(i[2].year),i[0]+".csv.gz")
    the_dir2 = os.path.join(path,ex.upper(),str(i[2].year),i[0]+".csv")
    if not os.path.exists(the_dir):
    #    print(the_dir)
    #    print(i)
    #    print(tdates[i[2]])
    #    print(i[2])
        api = TqApi(account=TqSim())
        # api = TqApi(account=TqSim(),url="ws://192.168.56.1:7777")
        td =DataDownloader(api, symbol_list=[ex.upper()+"."+i[1]], dur_sec=0,
                           start_dt=tdates[i[2]]+timedelta(hours=17), end_dt=i[2]+timedelta(hours=16), csv_file_name=the_dir2)
        while not td.is_finished():
            api.wait_update()
            # print("progress:  tick:%.2f%%" %  td.get_progress())
        print("done:" + the_dir)
        api.close()
        with open(the_dir2, 'rb') as f:
            with gzip.GzipFile(filename=the_dir2 + ".gz", mode='w', compresslevel=9) as gf:
                content = f.read()
                gf.write(content)
        os.remove(the_dir2)
        del td
        del api
        gc.collect()


def scrawl_tick():
    agg = agg_future_dayk()
    logging.info("start filter existed symbols")


    the_path = get_exchange_cache_dir(security_type='future', exchange='shfe',the_year='2020',
                                       data_type='day_kdata')

    trading_dates = sorted(os.listdir(the_path))

    # trading_dates = get_trading_calendar(security_type="future",exchange="shfe")
    tdates = {}
    for i in range(len(trading_dates)):
        if i>0:
            tdates[datetime.strptime(trading_dates[i],'%Y%m%d')]=datetime.strptime(trading_dates[i-1],'%Y%m%d')
    path = TICK_PATH
    filteredTradingDates = sorted(list(filter(lambda y:y>datetime(2018,11,30,0,0), map(lambda x:datetime.strptime(x,'%Y%m%d'),trading_dates))))
    logging.info("complete filter existed symbols")
    exchanges = ["shfe","cffex","dce","czce"]
    logging.info("start getting tick data")
    # api = TqApi(account=TqSim(),url="ws://192.168.56.1:7777")
    for ex in exchanges:
        logging.info(ex+": start getting tick")
        currentYearData = agg.getCurrentYearData(ex)
        currentYearData = currentYearData[currentYearData['date'].isin(filteredTradingDates)]
        pathpair=list(map(lambda x:(x[1].strftime('%Y%m%d')+"-"+x[0],x[0],datetime.utcfromtimestamp(x[1].timestamp())) ,currentYearData[['symbol','date']].values))
        #print(pathpair)
        p = Pool(2)
        for i in pathpair:
            if i[1].startswith("sc") or i[1].startswith("nr"):
                continue
            p.apply_async(scrawl_single_tick,args=(i,path,ex,tdates))

        p.close()
        p.join()
        logging.info(ex+": complete getting tick")
