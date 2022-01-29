# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 16:58:29 2020

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
    
columns = ['TradeDate','InstrID','Exchange','UpperLimitPrice','Turnover']
merge_columns = ['InstrID','Exchange']
turn_over = pd.DataFrame()
def read_turn_over_from_file(path,trade_date):
    file_count = 0
    for num in range(1,30) :    
        trade_date =trade_date - timedelta(days=1)
        trade_data_str = trade_date.strftime("%Y%m%d")
        price_file = path + "/volume/volume_" + trade_data_str + ".csv" 
        if  os.path.exists(price_file):
            file_count = file_count + 1
            result = pd.read_csv(price_file);

            result.drop(['UpperLimitPrice','LastPrice','TradeDate'],axis=1, inplace=True)
            result["Turnover"] = result["Turnover"]/1000000 
            result.rename(columns = {"Turnover": 'Turnover_' + trade_data_str},  inplace=True)
     
            if file_count == 1 :
                turn_over = result
            else:
                turn_over = turn_over.merge(result, on=merge_columns)
#                print(file_count)
#                print(turn_over.tail())
#                print(turn_over.columns.values.tolist())
            if(file_count >= 5):
                break
    return turn_over

def read_tick_by_tick(tick_by_tick_file,trade_date):
    chunksize=3000000
#    df = pd.read_csv(tick_by_tick_file,chunksize=chunksize, sep=' ')
    touch_result=pd.DataFrame()
    result=pd.DataFrame()
    count = 0
    for df in pd.read_csv(tick_by_tick_file,chunksize=chunksize, sep=' '):
        df = df[(df["trade"] == "T") & (df["flag"]=='F') ]
        df = df [["instr_id","trade_time","price","volume","ahead_volume"]]
        touch_df = df[df["ahead_volume"] > 0]
        df = df[df["ahead_volume"] <= 0]
#        df.drop(df[df["ahead_volume"] > 0].index, inplace=True)
#        touch_group = touch_df.groupby('instr_id').agg(['min','sum','max'])
        touch_group = touch_df.groupby('instr_id').agg({"trade_time":"min","volume":'sum',"price":'max'})
        group = df.groupby('instr_id').agg({"trade_time":"min"})
        touch_result = pd.concat([touch_result,touch_group],axis=0)
        result = pd.concat([result,group],axis=0)
        count +=1
    touch_result = touch_result.groupby("instr_id").agg({"trade_time":"min","volume":'sum',"price":'max'})
    result = result.groupby("instr_id").agg({"trade_time":"max"})
    result["instr_id"] = result.index
    touch_result["instr_id"] = touch_result.index
 #   print(result.head())
 #   print(touch_result.head())
 #   print(result.index);
    ret = touch_result.merge(result, on=["instr_id"])
    
    ret = ret[ret["trade_time_x"] >= ret["trade_time_y"]]
    ret["TouchTurnOver"] = ret["price"]*ret["volume"] / 1000000
    ret.drop(['price','volume','trade_time_x','trade_time_y'],axis=1,inplace=True)
#    ret.rename(columns = {"instr_id": 'InstrID'},  inplace=True)
    ret['TradeDate'] = trade_date
    ret = ret[['TradeDate','instr_id','TouchTurnOver']]
    return ret
    
        
if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage :python touch_stat.py path trade_date")        
    
    file_path = sys.argv[1]
    trade_date = datetime.strptime(sys.argv[2],"%Y%m%d")   
    
    for num in range(0,30) :
        trade_date =trade_date + timedelta(days=1)
        trade_date_str = trade_date.strftime("%Y%m%d")
        tick_by_tick_file = file_path + "/tick_by_tick/tick_by_tick_" + trade_date_str + ".csv";
        tar_file=file_path + "/tick_by_tick/tick_by_tick_" + trade_date_str + ".tar.gz"
        if not  os.path.exists(tar_file):
            print(tick_by_tick_file + "not exist" )
            continue 
        un_tar(tar_file,file_path+"/tick_by_tick/")
    
        avg_turnover = read_turn_over_from_file(file_path,trade_date)
        td_trade_money = read_tick_by_tick(tick_by_tick_file, trade_date_str)
        df = td_trade_money.merge(avg_turnover, left_on=["instr_id"],right_on=["InstrID"])
        df.drop(['InstrID'],axis=1,inplace=True)
        df.rename(columns = {"instr_id": 'InstrID'},  inplace=True)
    #    print(avg_turnover.head())
    #    print(td_trade_money.head())
        out_file=file_path + "/touch_trade/touch_trade.csv"
        if os.path.exists(out_file):
            df.to_csv(out_file,mode='a',index=False,header=None)        
        else :
            df.to_csv(out_file,index=False)
        
        os.remove(tick_by_tick_file)
    