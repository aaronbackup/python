# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 16:16:06 2019

@author: peng.sheng.long
"""

import pandas as pd
import sys
import os
from matplotlib import pyplot as plt
import tarfile

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

###0724 延迟
#l1_sources = ["LtsfUdpEx:1:1","LtsfUdpEx:2:2","LtsfUdpEx:2:3","LtsfUdpEx:2:4"]
#l2_sources = ["LtsfL2Udp:2:1","LtsfL2Udp:2:2","LtsfL2Udp:2:3"]

###0725 国投统计器转发 
#l2_sources = ["LtsfUdpEx:1:1"]
#l1_sources = ["LtsfUdpEx:8:1"]

###0725 华宝延迟
#l1_sources = ["LtsfUdpEx:1:1","Lts"LtsUdp:3:4"fUdpEx:1:2","LtsfUdpEx:1:3","LtsfUdpEx:1:4","LtsCffUdpEx:4:1","LtsCffUdpEx:5:1","LtsCffUdpEx:6:1"]
l1_sources = ["LtsCffUdpEx:4:1"]
#l2_sources = ["L1LtsfFw:10:1","L2LtsfFw:10:1"]
l2_sources = ["HTFUdp:4:1"]
#l2_sources = ["LtsfFw:2:1"]
#l2_sources = ["LtsfUdpFw:4:1","LtsfUdpFw:4:2","LtsfUdpFw:4:3","LtsfUdpFw:4:4"]
#l1_sources = ["LtsfFw:5:1"]

def price_check(price_file,delay_stat_file,miss_match_file):
    chunksize=5000000
#    check_list=['InstrID','SourceTime','OpenPrice','HighPrice','LowPrice','ClosePrice','LastPrice','Volume','Turnover','BidPrice1','BidVolume1','AskPrice1','AskVolume1']
    check_list=['InstrID','SourceTime','LastPrice','Volume','BidPrice1','BidVolume1','AskPrice1','AskVolume1']

#    check_list=['InstrID','Volume','LastPrice','BidPrice1','BidPrice2','BidPrice3','BidPrice4','BidPrice5','BidVolume1','BidVolume2','BidVolume3','BidVolume4','BidVolume5','AskPrice1','AskPrice2','AskPrice3','AskPrice4','AskPrice5','AskVolume1','AskVolume2','AskVolume3','AskVolume4','AskVolume5']  
   # check_list=['InstrID','Volume','LastPrice','BidPrice1','BidPrice2','BidPrice3','BidPrice4','BidPrice5','BidVolume1','BidVolume2','BidVolume3','BidVolume4','BidVolume5','AskPrice1','AskPrice2','AskPrice3','AskPrice4','AskPrice5','AskVolume1','AskVolume2','AskVolume3','AskVolume4','AskVolume5']
    columns=['InstrID','ExchangeID','LocalTime','TradeDate','SourceDate','SourceTime','Source','OpenPrice','HighPrice','LowPrice','ClosePrice','LastPrice','Volume','Turnover','OpenInterest','IOPV','AuctionPrice','BidPrice1','BidPrice2','BidPrice3','BidPrice4','BidPrice5','BidVolume1','BidVolume2','BidVolume3','BidVolume4','BidVolume5','AskPrice1','AskPrice2','AskPrice3','AskPrice4','AskPrice5','AskVolume1','AskVolume2','AskVolume3','AskVolume4','AskVolume5']
    df_all = pd.DataFrame(columns=columns)
    deal_count = 0
    delay_columns=['InstrID',"Source_l1","Source_l2",'SourceTime','Delay',"LocalTime_l1","LocalTime_l2"]
    df_delay = pd.DataFrame(columns=delay_columns)
    for df in pd.read_csv(price_file,chunksize=chunksize):
        df_l2 = df[df["Source"].isin(l2_sources)].drop_duplicates(subset=check_list,keep='first')
        df_l1 = df[df["Source"].isin(l1_sources)].drop_duplicates(subset=check_list,keep='first')
        df_l2 = df_l2.round(6)
        df_l1 = df_l1.round(6)
#        df_l2 = df[(df["Source"].isin(l2_sources)) & (df["InstrID"] == "IH1910") & (df["SourceTime"] > "09:30:00.000")].drop_duplicates(subset=check_list,keep='first')
#        df_l1 = df[(df["Source"].isin(l1_sources)) & (df["InstrID"] == "IH1910") & (df["SourceTime"] > "09:30:00.000")].drop_duplicates(subset=check_list,keep='first')
#         & (df["InstrID"] >= 10001827)  & (df["InstrID"] <= 10001980)
        

        df_all_concat = pd.concat([df_l2,df_l1],axis=0,ignore_index=True)
        df_all = pd.concat([df_all,df_all_concat],axis=0,ignore_index=True)
        df_all.drop_duplicates(subset=check_list,keep=False,inplace=True)
        
        df_merge=df_l1.merge(df_l2,on=check_list,suffixes=('_l1','_l2'))
        df_merge["Delay"]=df_merge["LocalTime_l1"] - df_merge["LocalTime_l2"]
        df_delay = pd.concat([df_delay,df_merge.loc[:,delay_columns]],axis=0,ignore_index=True)
        print("l1 count : "+ str(len(df_l1)) + " l2 count: " + str(len(df_l2)) + " df_delay_count: " + str(len(df_delay)) + " df_merge: " + str(len(df_merge)))
        deal_count +=1
        print("deal count : "+ str(deal_count) + " million wrong price count: " + str(len(df_all)) + " df_delay_count: " + str(len(df_delay)))
    df_delay.to_csv(delay_stat_file,header=True,index=False)
    df_all.to_csv(miss_match_file,header=True,index=False);
    return df_delay

def delay_stat(df_delay,delay_stat_png):
#    bins=[-10000000,-9000000,-7000000,-5000000,-3000000,-2000000,5000,20000,50000,100000,200000,500000,1000000,2000000,3000000,4000000,5000000,6000000,7000000,8000000,9000000,10000000,20000000,30000000]
#    bins=[-10000000,-9000000,-7000000,-5000000,-3000000,-2000000,-120000,-50000,-30000,0,2000,5000,10000,20000,50000,100000,130000,140000,200000,250000,300000,350000,400000,500000,1000000,2000000,3000000]
    bins = [-20000000000]
    bins += [i for i in range(-5000000,5000000,300000)]
#    bins += [i for i in range(0,5000000,300000)]
    bins += [2000000000]
    cats=pd.cut(df_delay['Delay'],bins,precision=0)
    bin_counts=pd.value_counts(cats,sort=False)
    bin_counts.index=[x/1000 for x in bins[0:-1]]

    #yticks=[y for y in range(0,300000,30000)]
    bin_counts.plot(kind='bar',alpha=0.5,rot=0,grid=True,title="delay stat",x='fast(us)',y="count",figsize=(25,16))

    fr = pd.Series(bin_counts.values.cumsum() / bin_counts.values.sum())
    pg=fr.plot(color='r',secondary_y=True,style='-o')
    for x in range(0,len(fr)):
        plt.annotate(format(fr[x], '.2%'), xy=(x,fr[x]), xytext=(x*0.9, fr[x]*0.9),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=.2'))     
    plt.xlabel('delay(us)')
    plt.ylabel('count')
    fig = pg.get_figure()
    fig.savefig(delay_stat_png)
    plt.show()

if __name__ == "__main__":
    
    if(len(sys.argv) != 2):
        print("Usage: python price_check.py  price_file")
        exit(-1)
         
    price_file = sys.argv[1]
    if( not os.path.exists(price_file)):
        print("file not exists :" + price_file)
        exit(-1)
        
    price_path = os.path.dirname(price_file)

    suffix = os.path.splitext(price_file)[1]
    if(suffix == ".gz"):
        date = sys.argv[2]
        un_tar(price_file,price_path)
        price_file = price_path +  "\\market_data\\day\\" + str(date) + "\\CFFEX\\price.csv"

    
    path=os.path.dirname(price_file)
    delay_stat_file=price_path + "\\delay_stat.csv"
    delay_stat_png=price_path + "\\stat.png"
    miss_match_file=price_path + "\\miss_match.csv"
    
    df_delay=price_check(price_file,delay_stat_file,miss_match_file)
    delay_stat(df_delay,delay_stat_png)

