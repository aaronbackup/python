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
def read_static_file(static_file):
    if os.path.exists(static_file):
        result = pd.read_csv(static_file);
        return result[["InstrID","UpperLimitPrice","PrevClosePrice"]]

def read_price(price_file):
    chunksize=1000000
    auction_result=pd.DataFrame()
    for df in pd.read_csv(price_file,chunksize=chunksize):
        df = df[(df["SourceTime"]< 93600000)]
        auction_result = df [["InstrID","SourceTime","BidPrice1"]]
        break

    auction_result["SourceTime"] = auction_result["SourceTime"] / 100000
    return auction_result

def price_filter(price_df:pd.DataFrame,begin_time,end_time):
    result = price_df[(price_df["SourceTime"] >= begin_time) & (price_df["SourceTime"] < end_time)]
    ret = result.groupby("InstrID").agg({"BidPrice1":"max"})
    # result.drop_duplicates(subset=["InstrID"],inplace=True,keep='first')
    # return result[["InstrID","BidPrice1"]];
    return ret;

def result_stat(filename):
    result = pd.read_csv(filename)
    result = result[ (result["BidPrice_25"] < result["UpperLimitPrice"])
        & (result["BidPrice_35"] == result["UpperLimitPrice"])
    ]

    # result = result[(result["BidPrice_25"] < result["UpperLimitPrice"])
    #                          & (result["BidPrice_35"] < result["UpperLimitPrice"])
    #                          ]

    df_none = result[
            (result["BidPrice_20"] < result["UpperLimitPrice"])
        & (result["BidPrice_21"] < result["UpperLimitPrice"])
        & (result["BidPrice_22"] < result["UpperLimitPrice"])
        & (result["BidPrice_23"] < result["UpperLimitPrice"])
        & (result["BidPrice_24"] < result["UpperLimitPrice"])
    ]

    df_20 = result[(result["BidPrice_20"] == result["UpperLimitPrice"])]
    df_21 = result[(result["BidPrice_21"] == result["UpperLimitPrice"])    ]
    df_22 = result[(result["BidPrice_22"] == result["UpperLimitPrice"])]
    df_23 = result[(result["BidPrice_23"] == result["UpperLimitPrice"])]
    df_24 = result[(result["BidPrice_24"] == result["UpperLimitPrice"])]


    df_any = result[
        (result["BidPrice_20"] == result["UpperLimitPrice"])
        | (result["BidPrice_21"] == result["UpperLimitPrice"])
        | (result["BidPrice_22"] == result["UpperLimitPrice"])
        | (result["BidPrice_23"] == result["UpperLimitPrice"])
        | (result["BidPrice_24"] == result["UpperLimitPrice"])
        ]

    df_20_open = result[(result["BidPrice_20"] == result["UpperLimitPrice"])
        & (result["BidPrice_21"] < result["UpperLimitPrice"])
        & (result["BidPrice_22"] < result["UpperLimitPrice"])
        & (result["BidPrice_23"] < result["UpperLimitPrice"])
        & (result["BidPrice_24"] < result["UpperLimitPrice"])
    ]

    df_21_open = result[(result["BidPrice_21"] == result["UpperLimitPrice"])
        & (result["BidPrice_22"] < result["UpperLimitPrice"])
        & (result["BidPrice_23"] < result["UpperLimitPrice"])
        & (result["BidPrice_24"] < result["UpperLimitPrice"])
    ]

    df_22_open = result[(result["BidPrice_22"] == result["UpperLimitPrice"])
        & (result["BidPrice_23"] < result["UpperLimitPrice"])
        & (result["BidPrice_24"] < result["UpperLimitPrice"])
    ]

    df_23_open = result[(result["BidPrice_23"] == result["UpperLimitPrice"])
                        & (result["BidPrice_24"] < result["UpperLimitPrice"])
                        ]

    df_24_open = result[(result["BidPrice_24"] == result["UpperLimitPrice"])
                        ]

    stat_result=pd.DataFrame();
    stat_result["all"] = [len(result)]
    stat_result["untouched"] = [len(df_none)];
    stat_result["touch_any"] = [len(df_any)];
    stat_result["touch_20"] = [len(df_20)];
    stat_result["touch_21"] = [len(df_21)];
    stat_result["touch_22"] = [len(df_22)];
    stat_result["touch_23"] = [len(df_23)];
    stat_result["touch_24"] = [len(df_24)];

    stat_result["touch_open_20"] = [len(df_20_open)];
    stat_result["touch_open_21"] = [len(df_21_open)];
    stat_result["touch_open_22"] = [len(df_22_open)];
    stat_result["touch_open_23"] = [len(df_23_open)];
    stat_result["touch_open_24"] = [len(df_24_open)];

    print(stat_result)

    # df_21 = result[ (result["BidPrice_30"] == result["UpperLimitPrice"])
    #                  | (result["BidPrice_31"] == result["UpperLimitPrice"])
    #                  | (result["BidPrice_32"] == result["UpperLimitPrice"])
    #                  | (result["BidPrice_33"] == result["UpperLimitPrice"])
    #                  | (result["BidPrice_34"] == result["UpperLimitPrice"])
    #                  | (result["BidPrice_35"] == result["UpperLimitPrice"])
    #                  ]

    # csv_columns = ["TradeDate", "InstrID", "BidPrice_20", "BidPrice_21", "BidPrice_22", "BidPrice_23", "BidPrice_24",
    #                "BidPrice_25",
    #                "BidPrice_30", "BidPrice_31", "BidPrice_32", "BidPrice_33", "BidPrice_34", "BidPrice_35",
    #                "UpperLimitPrice", "PrevClosePrice"]
    stat_result.to_csv("G:\\md\\price\\auction_stat6.csv",  index=False, float_format='%.2f')



if __name__ == '__main__':
    if(len(sys.argv) != 3):
        print("Usage :python price_stat.py path trade_date")        
    
    file_path = sys.argv[1]
    trade_date = datetime.strptime(sys.argv[2],"%Y%m%d")

    result_file = file_path + "/auction_touch.csv"
    result_stat(result_file)
    exit(0);



    for num in range(0,30) :
        trade_date =trade_date + timedelta(days=1)
        trade_date_str = trade_date.strftime("%Y%m%d")
        tar_file=file_path + "\\" + trade_date_str + ".tar.gz"
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
        
        price_df = read_price(sze_price_file)
        static_df = read_static_file(sze_static_file)

        touch_df = price_df[(price_df["SourceTime"] > 925) & (price_df["SourceTime"] < 1501)].groupby("InstrID").agg({"BidPrice1":"max"});
        touch_df = touch_df.merge(static_df,on=["InstrID"])
        touch_df = touch_df[touch_df["BidPrice1"] == touch_df["UpperLimitPrice"]]

        price_df_20 = price_filter(price_df,920,921);
        price_df_21 = price_filter(price_df,921,922);
        price_df_22 = price_filter(price_df,922,923);
        price_df_23 = price_filter(price_df,923,924);
        price_df_24 = price_filter(price_df,924,925);
        price_df_25 = price_filter(price_df,925,926);
        price_df_30 = price_filter(price_df,930,931);
        price_df_31 = price_filter(price_df,931,932);
        price_df_32 = price_filter(price_df,932,933);
        price_df_33 = price_filter(price_df,933,934);
        price_df_34 = price_filter(price_df,934,935);
        price_df_35 = price_filter(price_df,935,936);

        ret = price_df_20.merge(price_df_21,on=["InstrID"])
        ret.rename(columns={"BidPrice1_x":"BidPrice_20","BidPrice1_y":"BidPrice_21"},inplace=True)

        ret = ret.merge(price_df_22,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_22"},inplace=True)

        ret = ret.merge(price_df_23,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_23"},inplace=True)

        ret = ret.merge(price_df_24,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_24"},inplace=True)

        ret = ret.merge(price_df_25,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_25"},inplace=True)

        ret = ret.merge(price_df_30,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_30"},inplace=True)

        ret = ret.merge(price_df_31,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_31"},inplace=True)

        ret = ret.merge(price_df_32,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_32"},inplace=True)

        ret = ret.merge(price_df_33,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_33"},inplace=True)

        ret = ret.merge(price_df_34,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_34"},inplace=True)

        ret = ret.merge(price_df_35,on=["InstrID"])
        ret.rename(columns={"BidPrice1":"BidPrice_35"},inplace=True)

        ret = ret.merge(touch_df,on=["InstrID"])

        ret["TradeDate"] = trade_date_str

        print(ret.head())

        csv_columns = ["TradeDate","InstrID","BidPrice_20","BidPrice_21","BidPrice_22","BidPrice_23","BidPrice_24","BidPrice_25",
                       "BidPrice_30","BidPrice_31","BidPrice_32","BidPrice_33","BidPrice_34","BidPrice_35","UpperLimitPrice","PrevClosePrice"]
        if(os.path.exists(result_file)):
            ret.to_csv(result_file,columns=csv_columns, index=False,mode='a',header=None,float_format='%.2f')
        else :
            ret.to_csv(result_file,columns=csv_columns, index=False,float_format='%.2f')
    
        os.remove(sse_tbt_file)
        os.remove(sze_tbt_file)
        os.remove(sze_price_file)
        os.remove(sse_price_file)