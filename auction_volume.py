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
    print("untar file " + file_name)
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

def read_static_file(static_file):
    if os.path.exists(static_file):
        result = pd.read_csv(static_file);
        return result[["InstrID","PrevClosePrice"]]

def read_price(price_file):
    chunksize=1000000
    auction_result=pd.DataFrame()
    for df in pd.read_csv(price_file,chunksize=chunksize):
        df = df[(df["SourceTime"]< 92506000)]
        auction_result = df [["InstrID","LastPrice","Turnover"]]
        break
        
    auction_result.drop_duplicates(subset=['InstrID'],inplace=True,keep='last')
    return auction_result
        
if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage :python price_stat.py path trade_date")        
    
    file_path = sys.argv[1]
    trade_date = datetime.strptime(sys.argv[2],"%Y%m%d")   
    
    for num in range(0,365) :
        trade_date =trade_date + timedelta(days=1)
        trade_date_str = trade_date.strftime("%Y%m%d")
        tar_file=file_path + "/" + trade_date_str + ".tar.gz"
        sze_price_file =file_path + "/SZE/price_" + trade_date_str + ".csv";
        sse_price_file =file_path + "/SSE/price_" + trade_date_str + ".csv";
        sze_tbt_file =file_path + "/SZE/tbt_" + trade_date_str + ".csv";
        sse_tbt_file =file_path + "/SSE/tbt_" + trade_date_str + ".csv";
        sze_static_file =file_path + "/SZE/static.csv";
        sse_static_file =file_path + "/SSE/static.csv";

        if not  os.path.exists(tar_file):
            print(tar_file + " not exist" )
            continue 
        
        try:
            if not os.path.exists(sze_price_file):
                un_tar(tar_file,file_path)
        except Exception:
            print("exception")
            continue
        
        if not os.path.exists(sze_price_file):
            print(sze_price_file + " not exist")
            continue
        
        sse_auction_result=read_price(sse_price_file)
        sze_auction_result=read_price(sze_price_file)
        
        sse_static_result = read_static_file(sse_static_file)
        sze_static_result = read_static_file(sze_static_file)

#        sse_auction_result.append(sze_auction_result)
#        sse_static_result.append(sze_static_result)
        sse_auction_result = pd.concat([sse_auction_result,sze_auction_result])
        sse_static_result = pd.concat([sse_static_result,sze_static_result])
        
        ret = sse_auction_result.merge(sse_static_result, on=["InstrID"])
        ret["TradeDate"] = trade_date_str
        ret["UpRate"] = (ret["LastPrice"] - ret["PrevClosePrice"])/ret["PrevClosePrice"]
        result_file = file_path + "/auction_volume.csv"
        csv_columns = ["TradeDate","InstrID","LastPrice","Turnover","PrevClosePrice","UpRate"]
        if(os.path.exists(result_file)):
            ret.to_csv(result_file,columns=csv_columns, index=False,mode='a',header=None,float_format='%.2f')
        else :
            ret.to_csv(result_file,columns=csv_columns, index=False,float_format='%.2f')
    
        os.remove(sse_tbt_file)
        os.remove(sze_tbt_file)
        os.remove(sze_price_file)
        os.remove(sse_price_file)
