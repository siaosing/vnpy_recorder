import sys
import os
import redis
import zipfile
import logging
import pandas as pd
from pathlib import Path
from time import sleep
from datetime import datetime, time, timedelta

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.setting import SETTINGS
from vnpy_ctp import CtpGateway
from vnpy.trader.constant import Exchange
from vnpy.trader.object import BarData, ContractData, Product, TickData
from vnpy_datarecorder.engine import RecorderEngine

SETTINGS["log.active"] = True
SETTINGS["log.level"] = logging.INFO
SETTINGS["log.console"] = True
SETTINGS["log.file"] = True
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
file_handler = logging.FileHandler(f'log/{log_timestamp}.log', mode='a', encoding='utf8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# 全局变量
TRADINGDAY = ""  # 交易日
redis_conn = redis.Redis(db=1)       # 创建redis数据库
CTP_SETTING = {
    "用户名": "",
    "密码": "",
    "经纪商代码": "",
    "交易服务器": "",  # 10201 10202; 10130 for 7*24
    "行情服务器": "",   # 180.168.146.187:10131 for 7*24
    "产品名称": "",
    "授权编码": "",
    "产品信息": "",
    "instrument_not_sub": [
        "bb",
        "CY",
        "fb",
        "JR",
        "LR",
        "PM",
        "RI",
        "rr",
        "WH",
        "wr"
    ]
}

DAY_START = time(8, 35)
DAY_END = time(16, 30)
NIGHT_START = time(20, 35)
NIGHT_END = time(2, 45)

trade_calendar = pd.read_csv("trade_calendar.csv", index_col=0)  #每年根据节假日情况更新trade_calendar.csv
trade_calendar = trade_calendar.drop_duplicates()
trade_calendar = trade_calendar[trade_calendar.isOpen == 1]
trade_dates = trade_calendar.calendarDate.to_list()


class WholeMarketRecorder(RecorderEngine):
    def __init__(self, main_engine, event_engine, redis_conn):
        super().__init__(main_engine, event_engine)
        self.red = redis_conn

    def process_contract_event(self, event):
        """"""
        contract: ContractData = event.data
        # 不录制期权 只录制期货
        if contract.product == Product.FUTURES:
            if contract.symbol[:2] not in CTP_SETTING["instrument_not_sub"]:
                self.add_tick_recording(vt_symbol=contract.vt_symbol)
                self.subscribe(contract=contract)

    def record_tick(self, tick: TickData) -> None:
        """"""
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange
        contract: ContractData = self.main_engine.get_contract(tick.vt_symbol)
        if not contract:
            return

        VolumeMultiple = contract.size
        PriceTick = contract.pricetick
        mdlist = (TRADINGDAY,
                  tick.datetime.strftime("%Y%m%d"),
                  tick.datetime.strftime("%H:%M:%S"),
                  tick.datetime.strftime("%T.%f").split('.')[1][:-3],
                  symbol,
                  exchange.value,
                  tick.last_price,
                  tick.pre_settle,
                  tick.pre_close,
                  tick.pre_openinterest,
                  tick.open_price,
                  tick.high_price,
                  tick.low_price,
                  tick.volume,
                  tick.turnover,
                  tick.open_interest,
                  tick.close_price,
                  tick.settle_price,
                  tick.limit_up,
                  tick.limit_down,
                  tick.bid_price_1,
                  tick.bid_volume_1,
                  tick.ask_price_1,
                  tick.ask_volume_1,
                  tick.bid_price_2,
                  tick.bid_volume_2,
                  tick.ask_price_2,
                  tick.ask_volume_2,
                  tick.bid_price_3,
                  tick.bid_volume_3,
                  tick.ask_price_3,
                  tick.ask_volume_3,
                  tick.bid_price_4,
                  tick.bid_volume_4,
                  tick.ask_price_4,
                  tick.ask_volume_4,
                  tick.bid_price_5,
                  tick.bid_volume_5,
                  tick.ask_price_5,
                  tick.ask_volume_5,
                  tick.average_price,
                  VolumeMultiple,
                  PriceTick)

        self.red.lpush(symbol, ','.join((str(x) for x in mdlist)))

def check_trading_period():
    """"""
    trading = False
    current_time = datetime.now().time()
    today = datetime.today().strftime('%Y-%m-%d')

    if today in trade_dates and current_time <= DAY_END:
        trade_date = today
        pre_date = list(trade_calendar[trade_calendar.calendarDate == trade_date]['prevTradeDate'])[0]
        pre_date_next = datetime.strptime(pre_date, '%Y-%m-%d') + timedelta(days=1)
        pre_date_next = pre_date_next.strftime('%Y-%m-%d')

    if today in trade_dates and current_time > DAY_END:
        pre_date = today
        trade_date = list(trade_calendar[trade_calendar.prevTradeDate == today]["calendarDate"])[1]
        pre_date_next = datetime.strptime(pre_date, '%Y-%m-%d') + timedelta(days=1)
        pre_date_next = pre_date_next.strftime('%Y-%m-%d')

    if today not in trade_dates:
        trade_date = list(trade_calendar[trade_calendar.calendarDate > today]["calendarDate"])[0]
        pre_date = list(trade_calendar[trade_calendar.calendarDate == trade_date]['prevTradeDate'])[0]
        pre_date_next = datetime.strptime(pre_date, '%Y-%m-%d') + timedelta(days=1)
        pre_date_next = pre_date_next.strftime('%Y-%m-%d')

    global TRADINGDAY
    TRADINGDAY = trade_date.replace('-','')

    if today == trade_date and DAY_START <= current_time <= DAY_END:  # 交易日白天
        trading = True
        return trading
    if today == pre_date and datetime.strptime(trade_date, '%Y-%m-%d') \
            - datetime.strptime(today, '%Y-%m-%d') > timedelta(days=3):  # 节假日无夜盘
        trading = False
        return trading
    if today == pre_date and NIGHT_START <= current_time:  # 普通交易日夜盘
        trading = True
        return trading
    if today == pre_date_next and current_time <= NIGHT_END:  # 夜盘凌晨 要加return 否则周六凌晨非交易日返回False
        if datetime.strptime(trade_date, '%Y-%m-%d') - datetime.strptime(today, '%Y-%m-%d') > timedelta(days=2):
            trading = False   # 节假日第一天的凌晨
        else:
            trading = True    # 周六凌晨
        return trading

    if today not in trade_dates:  # 非交易日(非凌晨时段)
        trading = False
        return trading

def save_redis(red):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cwd = Path.cwd()
    instruments = red.keys("*")
    save_dir = "tick_data/" + TRADINGDAY
    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except IOError:
            pass

    csvheader = ["TradingDay",
                 "ActionDay",
                 "UpdateTime",
                 "UpdateMillisec",
                 "InstrumentID",
                 "ExchangeID",
                 "LastPrice",
                 "PreSettlementPrice",
                 "PreClosePrice",
                 "PreOpenInterest",
                 "OpenPrice",
                 "HighestPrice",
                 "LowestPrice",
                 "Volume",
                 "Turnover",
                 "OpenInterest",
                 "ClosePrice",
                 "SettlementPrice",
                 "UpperLimitPrice",
                 "LowerLimitPrice",
                 "BidPrice1",
                 "BidVolume1",
                 "AskPrice1",
                 "AskVolume1",
                 "BidPrice2",
                 "BidVolume2",
                 "AskPrice2",
                 "AskVolume2",
                 "BidPrice3",
                 "BidVolume3",
                 "AskPrice3",
                 "AskVolume3",
                 "BidPrice4",
                 "BidVolume4",
                 "AskPrice4",
                 "AskVolume4",
                 "BidPrice5",
                 "BidVolume5",
                 "AskPrice5",
                 "AskVolume5",
                 "AveragePrice",
                 "VolumeMultiple",
                 "PriceTick"]

    # 循环遍历写一下
    for instrument in instruments:
        instrument = instrument.decode()
        csvname = save_dir + os.path.sep + f"{instrument}.csv"
        isExist = os.path.isfile(csvname)

        b = red.lrange(instrument, 0, -1)[::-1]
        with open(csvname, 'a+') as f:
            f.write("\n".join([y.decode() for y in b]))

        if not isExist:
            csvfile = pd.read_csv(csvname, header=None)
            csvfile.to_csv(csvname, header=csvheader, index=False)
        else:
            csvfile = pd.read_csv(csvname)
            csvfile = csvfile.round(4)          # remove redundant 0s to save disk size
            csvfile.to_csv(csvname, index=False)

        red.delete(instrument)

    today = datetime.today().strftime('%Y%m%d')
    if today != TRADINGDAY:
        return
    now = datetime.now().strftime('%H:%M')
    if now < '15:00':
        return
    zip_filename = cwd.joinpath('tick_data/' + today + '.zip')
    az = zipfile.ZipFile(zip_filename, mode='w', compression=0, allowZip64=True, compresslevel=None)
    for name_file in os.listdir("tick_data/" + today):
        az.write(filename="tick_data/" + today + "/" + name_file, arcname=today + "/" + name_file, compress_type=zipfile.ZIP_BZIP2, compresslevel=None)
        os.remove("tick_data/" + today + "/" + name_file)
    az.close()

def main():

    while True:
        trading = check_trading_period()
        if trading:

            event_engine = EventEngine()
            main_engine = MainEngine(event_engine)
            main_engine.add_gateway(CtpGateway)
            logger.info("----------主引擎创建成功--------------")

            whole_market_recorder = WholeMarketRecorder(main_engine, event_engine,redis_conn=redis_conn)
            logger.info('whole_market_recorder 创建成功')

            main_engine.connect(CTP_SETTING, "CTP")
            logger.info(f'连接CTP接口, 用户名: {CTP_SETTING["用户名"]}')
            sleep(10)
            logger.info(f"tick recordings: {whole_market_recorder.tick_recordings.keys()}")
            logger.info("开始录制数据")
 

            while True:
                sleep(10)
                current_time = datetime.now().time().strftime('%H:%M')
                if current_time == '16:00' or current_time == '02:35':
                    logger.info('--------save redis-----------------')
                    save_redis(red=redis_conn)
                    logger.info('--------bye bye-----------------')
                    sys.stdout.flush()
                    sleep(30)
                    os._exit(1)
        else:
            sleep(30)


if __name__ == "__main__":
    main()