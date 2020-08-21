# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 18:08:45 2020

@author: admin
"""
import pandas as pd
import sys
import os
from matplotlib import pyplot as plt
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
    
    
def read_auction_tick(file):
    chunksize=3000000
    for df in  pd.read_csv(file,chunksize=chunksize):
#        print(df)
        return df[(df["SourceTime"] < 92500000) & (df["SourceTime"] > 92456000)]

def read_auction_tick_by_tick(file):
    chunksize=3000000
    result = pd.DataFrame()
    for df in  pd.read_csv(file,chunksize=chunksize):
#        print(df["ChannelNo"])
        result = pd.concat([result,df[(df["SourceTime"] < 92500000) & (df["SourceTime"] > 92457000)]],axis=0)
        if len(df[df["SourceTime"] >=92500000 ]) > 0 :
            break
    
    return result


def auction_money_stat(file_path,trade_date):
    price_tar_file = file_path + "/price_" + trade_date +".tar.gz"
    tbt_tar_file = file_path + "/tbt_" + trade_date +".tar.gz"
    
    price_file = file_path + "/price_" + trade_date + ".csv"
    tbt_file = file_path + "/tbt_" + trade_date + ".csv"
    if not os.path.exists(price_file):
        un_tar(price_tar_file,file_path)
    if not os.path.exists(tbt_file):    
        un_tar(tbt_tar_file,file_path)
    
    price_data = read_auction_tick(price_file)
    price_data["TurnOver"] = price_data["BidPrice1"]*price_data["BidVolume1"]/10000
    price_data = price_data[price_data["TurnOver"] > 5000]
    tbt_data = read_auction_tick_by_tick(tbt_file)
    
    price_data.drop_duplicates(["InstrID"],keep='last',inplace=True)
    tbt_data["Money"] = tbt_data["Price"]*tbt_data["Qty"]/10000
#    print(tbt_data[["Money","Price","Qty"]].head())
    
#    print(len(tbt_data))
    result = pd.DataFrame()
    for index, row in price_data.iterrows():
#        print(len(tbt_data[(tbt_data["TypeInstrID"] == row["InstrID"])]))
#        print(len(tbt_data[ (tbt_data["TypeInstrID"] == row["InstrID"]) & (tbt_data["Price"] < row["BidPrice1"])]))
#        print(row["InstrID"] + " len1 " + str(len(tbt_data[ (tbt_data["TypeInstrID"] == row["InstrID"]) & (tbt_data["Price"] < row["BidPrice1"])])))
        tbt_data.drop(tbt_data[ (tbt_data["TypeInstrID"] == row["InstrID"]) & (tbt_data["Price"] < row["BidPrice1"])],inplace=True)
        result = pd.concat([result,tbt_data[ (tbt_data["TypeInstrID"] == row["InstrID"]) & (tbt_data["Price"] >= row["BidPrice1"]) & (tbt_data["Side"] == "Buy")]],axis=0)
#        print(len(result[(result["TypeInstrID"] == row["InstrID"])]))
#        break
    
#    print(len(tbt_data))
        
    result["InstrID"] = result["TypeInstrID"]
    group_result = result.groupby('TypeInstrID').agg({"InstrID":"min","Money":"sum"})

    ret = price_data.merge(group_result,on=['InstrID'])
    out_file=file_path + "/" + "big_order.csv"
    ret["TradeDate"] = trade_date
    ret = ret[["TradeDate","InstrID","Money","TurnOver"]]    
    if os.path.exists(out_file):
        ret.to_csv(out_file,mode='a',index=False,header=None,float_format='%.2f')        
    else :
        ret.to_csv(out_file,index=False,float_format='%.2f')
        
    os.remove(price_file)
    os.remove(tbt_file)
        
if __name__ == "__main__":
    file_path = sys.argv[1]
    trade_date = datetime.strptime(sys.argv[2],"%Y%m%d")
    for num in range(0,1000) :
        trade_date =trade_date + timedelta(days=1)
        trade_date_str = trade_date.strftime("%Y%m%d")
        price_tar_file = file_path + "/price_" + trade_date_str +".tar.gz"
        tbt_tar_file = file_path + "/tbt_" + trade_date_str +".tar.gz"
        if not  os.path.exists(price_tar_file):
            continue
        
        if not  os.path.exists(tbt_tar_file):
            continue
        
        auction_money_stat(file_path,trade_date_str)
        