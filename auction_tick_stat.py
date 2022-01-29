import os
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts
import tarfile
import sys
import os
from datetime import datetime, timedelta


AMOUNT_BILLION = 1000000000


def un_tar(file_name, dir):
    (shotname, extension) = os.path.splitext(file_name)
    if extension != ".gz":
        return
    tar = tarfile.open(file_name)
    names = tar.getnames()

    for name in names:
        print(name)
        tar.extract(name, dir)
    tar.close()


def read_price(price_file):
    chunksize = 1000000
    auction_result = pd.DataFrame()
    for df in pd.read_csv(price_file, chunksize=chunksize):
        df = df[(df["SourceTime"] <= 92500000) | (df["SourceTime"] == 150003000) | (df["SourceTime"] == 150000000)]
        auction_result= pd.concat([auction_result,df],axis=0)
        # print(auction_result.head())

    auction_result.drop_duplicates(inplace=True)
    return auction_result

def read_position(position_file):
    chunksize = 1000000
    for df in pd.read_csv(position_file, chunksize=chunksize):
        return df

token = 'c3f87035d1bdaf8d946f8eca7e0de867e9ef30a40dc226973f44762f'
ts.set_token(token)
pro = ts.pro_api()

if __name__ == '__main__':
    if (len(sys.argv) != 3):
        print("Usage :python price_stat.py path trade_date")

    file_path = sys.argv[1]
    trade_date = datetime.strptime(sys.argv[2], "%Y%m%d")

    position_file = file_path + "/position.csv"
    position_df = read_position(position_file)
    position_df.set_index(['InstrID'], inplace=True)
    # print(position_df.head())
    result_file = file_path + "/auction_close.csv"
    today = datetime.today()
    for num in range(0, 365):
        trade_date = trade_date + timedelta(days=1)
        if trade_date > today:
            break
        trade_date_str = trade_date.strftime("%Y%m%d")
        daily_df = pro.daily(trade_date=trade_date_str)
        daily_df['InstrID'] = daily_df['ts_code'].str[0:6]
        daily_df['InstrID'] = daily_df['InstrID'].astype(int)
        # daily_df = daily_df['InstrID','open','close']
        # print(daily_df['InstrID'])
        tick_by_tick_file = file_path + "/SZE/tbt_" + trade_date_str + ".csv";
        tar_file = file_path + "/sze_price_" + trade_date_str + ".tar.gz"
        price_file = file_path + "/SZE/price_" + trade_date_str + ".csv";

        if not os.path.exists(tar_file):
            print(tar_file + " not exist")
            continue

        try:
            if not os.path.exists(price_file):
                print("un tar file : " + tar_file)
                un_tar(tar_file, file_path)
        except Exception:
            continue


        day_position = position_df[position_df['TradeDate'].astype(int) == int(trade_date_str)]
        print(position_df[position_df['TradeDate'].astype(int) == int(trade_date_str)])
        price_df = read_price(price_file)
        auction_df = price_df[(price_df['SourceTime'] >=92400000) & (price_df['SourceTime'] < 92457000)]
        min_auction_df = auction_df.groupby("InstrID").agg({"BidPrice1":"min"})
        min_auction_df.rename(columns={'BidPrice1':'AuctionPrice'},inplace=True)

        last_tick_df = price_df[(price_df['SourceTime'] >=92400000) & (price_df['SourceTime'] < 92500000)].drop_duplicates(['InstrID'],keep='last').groupby("InstrID").agg({"BidPrice1":"min"})
        last_tick_df.rename(columns={'BidPrice1':'LastAuctionPrice'},inplace=True)

        # auction_result_df = price_df[(price_df['SourceTime'] >= 92500000) & (price_df['SourceTime'] < 93000000)].groupby("InstrID").agg({"BidPrice1":"max","LastPrice":"max"})
        # auction_result_df.rename(columns={'BidPrice1':'BidOpen','LastPrice':'Open'},inplace=True)

        # close_price_df =  price_df[(price_df['SourceTime'] >=150000000) & (price_df['SourceTime'] <= 150003000)].groupby("InstrID").agg({"BidPrice1":"max","LastPrice":"max"})
        # close_price_df.rename(columns={'BidPrice1':'BidClose','LastPrice':'Close'},inplace=True)
        # result = min_auction_df.merge(auction_result_df,on=["InstrID"])
        # result =result.merge(close_price_df,on=["InstrID"])
        # position_df = position_df[position_df['TradeDate'].astype(int) == int(trade_date_str)]
        # position_df.drop('InstrID',axis=1,inplace=True)
        daily_df.set_index(['InstrID'],inplace=True)
        result = min_auction_df.merge(daily_df,on=["InstrID"])
        result['TradeDate'] = int(trade_date_str)
        result = result.merge(position_df,on=['InstrID','TradeDate'])

        result = result.merge(last_tick_df,on=['InstrID'])
        print(result.head())
        result["InstrID"] = result.index
        columns=['TradeDate','InstrID','AuctionPrice','LastAuctionPrice','open','close','BuyDate','HoldDays','Profit','Rate']
        if not os.path.exists(result_file):
            result.to_csv(result_file, columns=columns, index=False, float_format='%.2f')
        else:
            result.to_csv(result_file, columns=columns, mode='a', header=None, index=False, float_format='%.2f')

        # break

        os.remove(price_file)





