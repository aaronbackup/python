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
        if len(df[df['SourceTime'] < 93000000]) == 0:
            break
        df = df[(df["SourceTime"] < 93000000) & (df["SourceTime"] >= 92400000)]
        auction_result= pd.concat([auction_result,df],axis=0)

    auction_result.drop_duplicates(["InstrID",'SourceTime'],inplace=True)
    return auction_result

def process_result(result_file):
    chunksize = 1000000
    auction_result = pd.DataFrame()
    df = pd.read_csv(result_file)

    columns = ['TradeDate', 'InstrID', 'MinAuctionPrice', 'open', 'UpRate', 'LocalTime_y', 'Seq', 'Price', 'Qty',
               'Money', 'Turnover', 'close', 'pct_chg','day_up','open_rate','prev_close']
    # tbt_df['LocalTime'] = tbt_df.apply(lambda x: x if x > 100 else x + 1000)
    df = df[df['LocalTime_y']>999]
    print(df)
    df['rate'] = df['pct_chg'] / 100
    df['Money'] = df['Money'] / 10000
    df['prev_close'] = df['close'] / (1 + df['rate'])
    df['open_rate'] = (df['open'] - df['prev_close'])/df['prev_close']*100
    df['day_up'] = (df['close'] - df['open'])/df['prev_close']*100
    df.drop_duplicates(['InstrID','TradeDate','Seq'], keep='last', inplace=True)
    df = df.sort_values(by=['Money'], axis=0, ascending=True)
    group_df =df.groupby(by=['TradeDate',"InstrID"]).agg(
        {"MinAuctionPrice": "max", "open": "max", "Turnover": "max", "Money": "sum",
         "close": "max", "pct_chg": "max", "UpRate": "max", "open": "max", "LocalTime_y": "max","Qty":"max","prev_close":"max","open_rate":"max","day_up":"max"})
    # group_df = df.groupby(by=["InstrID","TradeDate"]).agg({"Money": "sum","rate":"max","prev_close":"max","open_rate":"max","day_up":"max","Qty":"max"})
    # df.drop_duplicates()

    group_df.to_csv('G:/md/auction999.csv', columns=columns, index=True, float_format='%.2f')
    return group_df


def read_tbt(tbt_file):
    chunksize = 1000000
    auction_result = pd.DataFrame()
    for df in pd.read_csv(tbt_file, chunksize=chunksize):
        df = df[(df["SourceTime"] <= 92500000)]
        if len(df) == 0:
            break
        auction_result= pd.concat([auction_result,df],axis=0)

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

    result_file = file_path + "/auction_up.csv"
    result_filter_file = file_path + "/auction_filter.csv"

    process_result(result_file)
    exit(0)

    today = datetime.today()
    for num in range(0, 365):
        if trade_date > today:
            break
        trade_date_str = trade_date.strftime("%Y%m%d")
        if(trade_date_str > "20211001"):
            break
        directory = trade_date.strftime("%Y%m")
        trade_date = trade_date + timedelta(days=1)
        tick_by_tick_file = file_path + "/SZE/tbt_" + trade_date_str + ".csv";
        tbt_tar_file = file_path + "/sze_tbt_" + trade_date_str + ".tar.gz"
        tar_file = file_path + "/sze_price_" + trade_date_str + ".tar.gz"
        price_file = file_path + "/SZE/price_" + trade_date_str + ".csv";

        tick_by_tick_file = file_path +"/SZE/tbt_" + trade_date_str + ".csv";
        tbt_tar_file = file_path +"/" + directory + "/sze_tbt_" + trade_date_str + ".tar.gz"
        tar_file = file_path + "/" + directory + "/sze_price_" + trade_date_str + ".tar.gz"
        price_file = file_path +   "/SZE/price_" + trade_date_str + ".csv";

        if not os.path.exists(tar_file) or not os.path.exists(tbt_tar_file):
            print(tar_file + " not exist")
            continue

        try:
            if not os.path.exists(price_file):
                print("un tar file : " + tar_file)
                un_tar(tar_file, file_path)

            if not os.path.exists(tick_by_tick_file):
                print("un tar file : " + tbt_tar_file)
                un_tar(tbt_tar_file, file_path)
        except Exception:
            continue

        try:
            price_df = read_price(price_file)
            auction_df = price_df[(price_df['SourceTime'] >=92400000) & (price_df['SourceTime'] < 92500000)]
            min_auction_df = auction_df.groupby("InstrID").agg({"BidPrice1":"min"})
            min_auction_df.rename(columns={'BidPrice1':'MinAuctionPrice'},inplace=True)
            close_price_df = price_df[price_df['SourceTime'] >= 92500000].drop_duplicates(['InstrID'],keep='last')

            tbt_df = read_tbt(tick_by_tick_file)
            tbt_df['LocalTime'] = (tbt_df['LocalTime'] / 1000000) % 1000
            tbt_df['LocalTime'] = tbt_df['LocalTime'].apply(lambda x:  x if x > 100 else x+1000)
            tbt_df['Money'] =tbt_df['Qty']  * tbt_df['Price']

            # group_tbt_df =tbt_df[tbt_df['LocalTime'] > 995].groupby("InstrID").agg({"Money":"sum","LocalTime":"max","SourceTime":"max"})
            # group_tbt_df =tbt_df[tbt_df['LocalTime']  > 995].groupby("InstrID").agg({"LocalTime":"max"})
            # print(group_tbt_df.head())
            tbt_last_df = tbt_df[(tbt_df['SourceTime'] == 92459990) & (tbt_df['Type'] == 'O') & (tbt_df['Side'] == 'Buy')]

            daily_df = pro.daily(trade_date=trade_date_str)
            daily_df['InstrID'] = daily_df['ts_code'].str[0:6]
            daily_df['InstrID'] = daily_df['InstrID'].astype(int)

            result = close_price_df.merge(min_auction_df,on=['InstrID'])
            result = result.merge(tbt_last_df,on=['InstrID'])
            result = result.merge(daily_df,on=['InstrID'])
            result['TradeDate'] = trade_date_str
            result = result[(result['MinAuctionPrice'] > 0) & (result['Price'] >= result['LastPrice'])]
            result['UpRate'] = (result['LastPrice'] - result['MinAuctionPrice']) / result['MinAuctionPrice'] * 100
            result['Turnover'] = result['Turnover'] / 10000
            # result['LocalTime_y'] = result.apply(lambda x: x if x < 100 else x+1000, axis=1)
            print(result.head())
            columns=['TradeDate','InstrID','MinAuctionPrice','open','UpRate','LocalTime_y','Seq','Price','Qty','Money','Turnover','close','pct_chg']
            if not os.path.exists(result_file):
                result.to_csv(result_file, columns=columns, index=False, float_format='%.2f')
            else:
                result.to_csv(result_file, columns=columns, mode='a', header=None, index=False, float_format='%.2f')


            result = result[(result['UpRate'] > 2) & (result['LocalTime_y'] >= 995) & (result['Turnover'] > 1000) & (result['LastPrice'] > result['MinAuctionPrice'])]
            result = result.sort_values(by=['Money'], axis=0, ascending=True)
            group_df = result.groupby("InstrID").agg(
                {"TradeDate": "max", "MinAuctionPrice": "max", "open": "max", "Turnover": "max", "Money": "sum",
                 "close": "max", "pct_chg": "max", "UpRate": "max", "open": "max", "LocalTime_y": "max"})
            result.drop_duplicates(['InstrID'], keep='last',inplace=True)
            if not os.path.exists(result_filter_file):
                result.to_csv(result_filter_file, columns=columns, index=False, float_format='%.2f')
            else:
                result.to_csv(result_filter_file, columns=columns, mode='a', header=None, index=False, float_format='%.2f')

            group_df['InstrID'] = group_df.index
            auction_up_file = 'G:/md/auction.csv'
            if not os.path.exists(auction_up_file):
                group_df.to_csv(auction_up_file, columns=columns, index=False, float_format='%.2f')
            else:
                group_df.to_csv(auction_up_file, columns=columns, mode='a', header=None, index=False, float_format='%.2f')

        # break
            os.remove(price_file)
            os.remove(tick_by_tick_file)
        except Exception:
            continue

    process_result(result_filter_file)
