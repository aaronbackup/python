# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 21:54:52 2021

@author: aaron
"""
import tushare as ts
import pandas as pd
import sys
import os
import time
from datetime import datetime, timedelta

from numpy import long


def read_auction_result(file):
    result = pd.read_csv(file)
    return result

def auction_result_filter(file):
    result = pd.read_csv(file)
    
    avg_result =result.groupby('InstrID').agg({"Turnover":"mean"})
    avg_result["InstrID"] = avg_result.index
    avg_result["Turnover"] = avg_result["Turnover"] * 10
    return avg_result

def download_k_line():

    result_file = "F:\md\price\k_line.csv"

    if os.path.exists(result_file):
        return pd.read_csv(result_file)

    token = 'c3f87035d1bdaf8d946f8eca7e0de867e9ef30a40dc226973f44762f'
    ts.set_token(token)
    pro = ts.pro_api()

    trade_date = datetime.strptime('20201001', "%Y%m%d")
    result = pd.DataFrame()
    for num in range(0, 180):
        trade_date = trade_date + timedelta(days=1)
        trade_date_str = trade_date.strftime("%Y%m%d")
        df = pro.daily(trade_date=trade_date_str)

        result = pd.concat([result,df])

        time.sleep(1)

    result.to_csv(result_file,index=False,float_format='%.2f')
    return result

def read_market_value(file):
    result = pd.read_csv(file)
    return result;

def get_k_line(df,instr_id,trade_date_str,days):
    trade_date = datetime.strptime(str(trade_date_str.astype(int)), "%Y%m%d")
    for num in range(0,30):
        if days > 0 :
            trade_date =trade_date + timedelta(days=1)
        else:
            trade_date = trade_date - timedelta(days=1)

        trade_date_str = trade_date.strftime("%Y%m%d")
        k_line = df[(df['ts_code'].astype(int) == instr_id.astype(int))   & (df['trade_date'].astype(int) == int(trade_date_str))]
        if len(k_line) > 0:
            if days > 0 :
                days = days - 1
            else :
                days = days + 1

            if days == 0:
                return k_line

    return pd.DataFrame()

if __name__ == "__main__":
    if(len(sys.argv) != 2):
        print("Usage :python price_stat.py path ")
        exit(0)
    
    file_path = sys.argv[1]
    
    if not os.path.exists(file_path):
        print("file not exist : " + file_path)
        exit

    auction_file = file_path + "/auction_volume.csv"
    market_value_df = read_market_value(file_path + "/market_value.csv")
    print(market_value_df.head())
    auction_result = read_auction_result(auction_file)
    avg_result = auction_result_filter(auction_file)
    
    auction_result = auction_result.merge(avg_result,on=['InstrID'])
    auction_result = auction_result[(auction_result["Turnover_x"] > auction_result["Turnover_y"])  & (auction_result["Turnover_x"] > 10000000)]
    # print(avg_result.head())
    # print(auction_result.head())

    k_line_df = download_k_line()
    k_line_df['ts_code'] = k_line_df['ts_code'].str[0:6]
    # result_file = file_path + "/big_auction.csv"
    # auction_result.to_csv(result_file, index=False, float_format='%.2f')
    # for row in auction_result.rows:
    #     print(row["InstrID"])
    #     pass
    for index, row in auction_result.iterrows():
        trade_date_str = row["TradeDate"]
        instr_id = row["InstrID"]

        k_line_td = k_line_df[(k_line_df['ts_code'].astype(int) == instr_id.astype(int)) & (k_line_df['trade_date'].astype(int) == trade_date_str.astype(int))]
        k_line_yd = get_k_line(k_line_df,instr_id,trade_date_str,-1)
        k_line_next_3d = get_k_line(k_line_df,instr_id,trade_date_str,3)

        # print("today line")
        # print(k_line_td.head())
        # row['pct_chg'] = k_line_td['pct_chg']
        # print(k_line_td.loc['pct_chg',0])
        # print(k_line_td['pct_chg'].iloc[0])
        auction_result.loc[(auction_result['InstrID'] == instr_id) & (auction_result['TradeDate'] == trade_date_str), 'pct_chg'] = k_line_td['pct_chg'].iloc[0]
        auction_result.loc[(auction_result['InstrID'] == instr_id) & (auction_result['TradeDate'] == trade_date_str), 'close'] = k_line_td['close'].iloc[0]

        try:
            if (len(k_line_yd) > 0) & len(k_line_yd[k_line_yd['pct_chg'] > 9.8]) > 0:
                auction_result.loc[(auction_result['InstrID'] == instr_id) & (auction_result['TradeDate'] == trade_date_str), 'Del'] = 1
            else:
                auction_result.loc[(auction_result['InstrID'] == instr_id) & (auction_result['TradeDate'] == trade_date_str), 'Del'] = 0

            if len(k_line_next_3d) > 0:
                auction_result.loc[(auction_result['InstrID'] == instr_id) & (auction_result['TradeDate'] == trade_date_str), 'NextPrice'] = k_line_next_3d['close'].iloc[0]
            else:
                auction_result.loc[(auction_result['InstrID'] == instr_id) & (
                        auction_result['TradeDate'] == trade_date_str), 'NextPrice'] = 0
        except Exception:
            print(k_line_td)
            print(k_line_yd)
            print(k_line_next_3d)


    result_file = file_path + "/big_auction_merge.csv"
    auction_result['UpRate'] = auction_result['UpRate']*100

    auction_result = auction_result.merge(market_value_df,on=['InstrID'])
    auction_result.to_csv(result_file, index=False, float_format='%.4f')



