# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 21:49:18 2020

@author: aaron
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 16:58:29 2020

@author: admin
"""

import pandas as pd
import sys
import os
#from matplotlib import pyplot as plt
import tarfile
from datetime import datetime, timedelta

def un_tar(file_name,dir):
    (shotname,extension) = os.path.splitext(file_name)
    if extension != ".gz":
        return 
    tar = tarfile.open(file_name)
    names = tar.getnames()
    
    for name in names:
        print(name)
        tar.extract(name, dir)
    tar.close()
    
columns = ['TradeDate','InstrID','Exchange','UpperLimitPrice','Turnover']
merge_columns = ['InstrID','Exchange']
touch_times = pd.DataFrame()
def read_touch_times(path,trade_date):
    for num in range(1,30) :    
        trade_date = trade_date - timedelta(days=1)
        trade_data_str = trade_date.strftime("%Y%m%d")
        volume_file = path + "/SZE/volume_" + trade_data_str + ".csv" 
        if  os.path.exists(volume_file):
            result = pd.read_csv(volume_file);
            result.drop(['UpperLimitPrice','LastPrice','TradeDate','Exchange','UpperLimitPrice','Turnover'],axis=1, inplace=True)
            result = result[result["TouchTimes"] > 0]
            return result

def read_upper_limit_price(path):
    static_file = path + "/SZE/static.csv" 
    if os.path.exists(static_file):
        result = pd.read_csv(static_file);
        return result[["InstrID","UpperLimitPrice"]]

def read_price(price_file):
    chunksize=1000000
    auction_result=pd.DataFrame()
    for df in pd.read_csv(price_file,chunksize=chunksize):
        df = df[df["SourceTime"]== 92500000]
        df["SellMoney"] = df["AskPrice1"] * df["AskVolume1"]
        auction_result = df [["InstrID","SellMoney"]]
        break
        
    auction_result.drop_duplicates(inplace=True)
    return auction_result

def read_tick_by_tick(tick_by_tick_file,trade_date):
    chunksize=5000000
#    df = pd.read_csv(tick_by_tick_file,chunksize=chunksize, sep=' ')
    auction_result=pd.DataFrame()
    open_result=pd.DataFrame()
    for df in pd.read_csv(tick_by_tick_file,chunksize=chunksize):
        df = df[df["TradeFlag"]=='F']
        df["Turnover"] = df["Price"] * df["Qty"]
        df = df [["InstrID","SourceTime","Price","Turnover"]]
        auction_result = df[df["SourceTime"] == 92500000]
        open_result = df[(df["SourceTime"] <= 93003000) & (df["SourceTime"] > 92500000)].groupby('InstrID').agg({"Price":"max","Turnover":"sum"})
        break
#        touch_result = pd.concat([touch_result,touch_group],axis=0)
#        result = pd.concat([result,group],axis=0)
#        count +=1
        
    auction_result.drop(['Turnover'],axis=1,inplace=True)
    auction_result.drop_duplicates(inplace=True)
    open_result["InstrID"] = open_result.index
    ret = open_result.merge(auction_result, on=["InstrID"])
    return ret
    
        
if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage :python price_stat.py path trade_date")        
    
    file_path = sys.argv[1]
    trade_date = datetime.strptime(sys.argv[2],"%Y%m%d")   
    
    for num in range(0,365) :
        trade_date =trade_date + timedelta(days=1)
        trade_date_str = trade_date.strftime("%Y%m%d")
        tick_by_tick_file = file_path + "/SZE/tbt_" + trade_date_str + ".csv";
        tar_file=file_path + "/" + trade_date_str + ".tar.gz"
        price_file =file_path + "/SZE/price_" + trade_date_str + ".csv";

        if not  os.path.exists(tar_file):
            print(tar_file + " not exist" )
            continue 
        
        try:
            if not os.path.exists(tick_by_tick_file):
                un_tar(tar_file,file_path)
        except Exception:
            continue
        
        if not os.path.exists(tick_by_tick_file):
            print(tick_by_tick_file + " not exist")
            continue
        
        upper_limit_price = read_upper_limit_price(file_path)
        touch_times = read_touch_times(file_path,trade_date)
        open_result = read_tick_by_tick(tick_by_tick_file, trade_date_str)
        auction_price = read_price(price_file)
        ret = open_result.merge(touch_times, on=["InstrID"])
        ret["TradeDate"] = trade_date_str
        ret["MaxPrice"] = ret["Price_x"]
        ret["OpenPrice"] = ret["Price_y"]
        ret["Diff"] = ret["MaxPrice"] - ret["OpenPrice"]

        ret = ret[ret["Diff"] <= 0.02]
        ret.drop(['SourceTime','TouchTimes',"Price_x","Price_y","Diff"],axis=1, inplace=True)
    
        ret = ret.merge(auction_price,on=["InstrID"])
        ret = ret.merge(upper_limit_price, on=["InstrID"])
        ret = ret[ret["MaxPrice"] < ret["UpperLimitPrice"]]
        
        print(ret)
        result_file = file_path + "/open_stat.csv"
        csv_columns = ["TradeDate","InstrID","MaxPrice","OpenPrice","Turnover","SellMoney"]
        if(os.path.exists(result_file)):
            ret.to_csv(result_file,columns=csv_columns, index=False,mode='a',header=None,float_format='%.2f')
        else :
            ret.to_csv(result_file,columns=csv_columns, index=False,float_format='%.2f')
    
        os.remove(tick_by_tick_file)
        os.remove(price_file)
        tick_by_tick_file = file_path + "/SSE/tbt_" + trade_date_str + ".csv";
        os.remove(tick_by_tick_file)
        price_file =file_path + "/SSE/price_" + trade_date_str + ".csv";
        os.remove(price_file)
