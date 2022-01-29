import os
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts
import talib as ta

AMOUNT_BILLION = 1000000000
class TrendTool():
    def __init__(self,ma_days,min_days):
        self.ma_days = ma_days
        self.ma_name = "ma" + str(self.ma_days)
        self.min_days = min_days

    def is_up_trend(self, df):
        grow_count = 0
        count = 0;
        if len(df) < self.min_days:
            return False

        sampling_days= 20
        last_max_price = float(df.iloc[0:sampling_days][self.ma_name].max())
        for i in range(sampling_days, self.min_days, sampling_days):
            max_price = float(df.iloc[i:i+sampling_days][self.ma_name].max())
            if max_price > last_max_price:
                grow_count += 1
                last_max_price = max_price

            count += 1

        if grow_count < count - 1:
            return False

        return True

    def is_waitting1(self, df):
        # 新高在过去20个交易日内,且股价大幅波动天数比较少
        fall_acc_days = 0
        up_acc_days = 0
        max_price = 0
        min_fall_price = 10000000000
        min_price = 10000000000
        last_price = 0
        max_price_trade_date = 0
        max_price_index = 0
        waitting_days = 0
        ma_fall_days = 0
        fall_days = 0
        aways_fall = True
        last_fall_days = 0
        up_days = 0
        signal_days = 0
        last_fall_acc_days = 0
        lastest_fall_days = 0
        last_up_index = 0
        last_range = 0
        latest_ten_days_max_price = 0
        lastest_fall_acc_days = 0
        continue_fall_acc_days = 0
        last_fall_acc_index = 0
        acc_up_days_after_max_price_date = 0
        max_vol_index = 0
        max_vol_range = 0
        max_vol = 0
        max_price_vol = 0
        max_price_pct_change = 0
        for i in range(-1, -20, -1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            trade_date = sell_row['trade_date']
            pct_chg = sell_row['pct_chg']
            vol = sell_row['vol']
            ma = sell_row[self.ma_name]

            # up_days = 0
            day_range = (close_price - open_price) / pre_close_price * 100

            if last_price == 0:
                last_price = close_price

            if min_price > close_price:
                min_price = close_price

            if day_range> -3 and day_range< 3 and pct_chg < 3 and pct_chg > -3:
                waitting_days += 1

            if pct_chg > 3 or day_range > 3:
                up_acc_days += 1
                acc_up_days_after_max_price_date +=1

            if day_range < -3 or pct_chg < -3:
                fall_acc_days += 1

            if max_price < close_price :
                max_price = close_price
                max_price_trade_date = trade_date
                max_price_index = i
                min_fall_price = 1000000000
                waitting_days = 0
                fall_acc_days = 0
                acc_up_days_after_max_price_date +=1
                fall_days = 0
                up_days = 0
                max_price_vol = vol
                max_price_pct_change = pct_chg

            if min_fall_price > close_price and trade_date > max_price_trade_date:
                min_fall_price = close_price

            if day_range < 0 and i > -5:
                fall_days += 1
                lastest_fall_days += 1

                if day_range< -3:
                    lastest_fall_acc_days +=1

            if day_range > 0 and i > -6:
                up_days += 1

            if min_fall_price > close_price and trade_date > max_price_trade_date:
                min_fall_price = close_price

            if close_price < ma:
                ma_fall_days +=1

        max_fall_range = (min_fall_price - max_price) / max_price * 100
        # return up_acc_days - acc_up_days_after_max_price_date < 3 and ma_fall_days < 10 and fall_acc_days > 0 and max_fall_range > -25 and waitting_days > int(max_price_index * -1 * 0.3)
        # return signal_days == 0 and fall_days > up_days and ma_fall_days < 10 and last_fall_acc_days > 0 and max_fall_range > -20
        # return  fall_days > 2 and ma_fall_days < 10 and lastest_fall_days > 0 and max_fall_range > -20 #no last fall acc days
        # return ((fall_acc_days > 0 and ma_fall_days < 10) or waitting_days > 15) and last_fall_days > 0 and max_fall_range > -20
        # return  i < -15 and ma_fall_days < 10 and (waitting_days > 13) and fall_acc_days > 0 and max_fall_range > -25  #best condition
        # return fall_acc_days < 4 and lastest_fall_days > 1 and max_price_index < -10 and ma_fall_days < 10 and (waitting_days > 10)
        # return (lastest_fall_days > 1 or lastest_fall_acc_days > 0) and max_price_index < -10 and ma_fall_days < 10 and waitting_days > 12
        # return up_acc_days - acc_up_days_after_max_price_date < 3 and fall_acc_days < 3 and (lastest_fall_days > 1) and max_price_index < -5 and ma_fall_days ==0 and int(waitting_days > max_price_index * -1 * 0.3)

        return up_acc_days - acc_up_days_after_max_price_date < 3 and fall_acc_days < 3 and lastest_fall_days > 0 and max_price_index < -8 and ma_fall_days < 10 and waitting_days > int(max_price_index * -1 * 0.3)
        # return   fall_acc_days < 3 and (lastest_fall_days > 0) and max_price_index < -8 and ma_fall_days < 10 and

        # return (lastest_fall_days > 2 or lastest_fall_acc_days > 0) and max_price_index < -8 and ma_fall_days < 10 and waitting_days > 12

        return cur_fall_range < -5 and max_price_index < -8 and ma_fall_days < 10 and waitting_days > 10


    def is_waitting(self,df,max_price_date):
        waitting_days = 0
        begin_date = 0
        up_days_5_plus = 0
        fall_days_5_plus = 0
        up_days_before_max_price = 0
        days_before_max_price = 0

        up_days = 0
        down_days = 0
        max_price = 0
        min_price = 1000000000
        fall_vol = 0
        up_vol = 0

        #新高在过去20个交易日内,且股价大幅波动天数比较少
        for i in range(-1,-20,-1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            trade_date = sell_row['trade_date']
            begin_date = trade_date
            pct_chg = sell_row['pct_chg']
            vol = sell_row['vol']
            if trade_date > max_price_date and min_price > close_price:
                min_price = close_price

            if begin_date == max_price_date:
                max_price = close_price
            day_range = (close_price - open_price) / pre_close_price * 100
            if day_range < -5 or pct_chg < -5 :
                if trade_date > max_price_date:
                    fall_days_5_plus += 1
                    if fall_vol == 0:
                        fall_vol = vol
            elif day_range > 5 or pct_chg > 5:
                if trade_date > max_price_date:
                    up_days_5_plus += 1
                    if up_vol == 0:
                        up_vol = vol
                else :
                    up_days_before_max_price += 1
            elif trade_date > max_price_date:
                waitting_days += 1

            if trade_date < max_price_date:
                days_before_max_price += 1
                if days_before_max_price > 10:
                    break
            else:
                if day_range > 0:
                    up_days += 1
                else:
                    down_days += 1

        #自高点回撤的幅度
        max_fall_range = -1
        if max_price > 0:
            max_fall_range = (min_price - max_price)/ max_price * 100

        #近15个交易日，涨幅大于5或小于-5的天数都小于三天，-5到5之间的天数大于三天，最高价在15个交易日内
        # return (avg_up_vol == 0 or avg_up_vol > avg_fall_vol) and  fall_days_5_plus < 4 and up_days_5_plus < 4 and max_fall_range > -15 and begin_date < max_price_date
        return waitting_days < 4 and (fall_vol == 0 or up_vol > fall_vol) and fall_days_5_plus < 2 and max_fall_range > -12 and begin_date < max_price_date

        # 计算大阴线平均成交量
    def get_max_vol_line(self, df):
        max_vol = 0
        for i in range(-1, -30, -1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            vol = sell_row['vol']
            pct_chg = sell_row['pct_chg']
            trade_date = sell_row['trade_date']
            day_range = (close_price - open_price) / pre_close_price * 100

            if max_vol < vol:
                max_vol = vol

        return [max_vol,pct_chg,day_range,trade_date]

        # 计算大阴线平均成交量
    def is_greater_buy_power(self, df):
        sell_volume = 0
        bid_volume = 0
        max_sell_vol = 0
        max_bid_vol = 0
        for i in range(-1, -15, -1):
            sell_row = df.iloc[i]
            open_price = sell_row['open']
            close_price = sell_row['close']
            pre_close_price = sell_row['pre_close']
            vol = sell_row['vol']
            day_range = (close_price - open_price) / pre_close_price * 100

            if day_range < -2:
                sell_volume += vol
                if max_sell_vol < vol:
                    max_sell_vol = vol
            elif day_range > 2:
                bid_volume += vol
                if max_bid_vol < vol:
                    max_bid_vol = vol

        return max_bid_vol == 0 or (max_bid_vol > max_sell_vol*1.1)


class StrategyBase():
    def __init__(self,strategy_name,ma_days,min_days):
        self.ma_days = ma_days
        self.min_days = min_days
        self.ma_name = "ma" + str(self.ma_days)
        self.strategy_name=strategy_name
        pass

    def run(self, df, stock_id, total_share, backtest_date):
        pass

    def set_ma(self,ma_days):
        self.ma_days = ma_days
        self.ma_name = "ma" + str(self.ma_days)

class BullStrategy(StrategyBase):
    def __init__(self,ma_days,min_days):
        StrategyBase.__init__(self,'BullStrategy',ma_days,min_days)
        self.trend_tool = TrendTool(self.ma_days, self.min_days)

    def run(self, df, stock_id, total_share, backtest_date):
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return []

        if len(df) < self.trend_tool.min_days:
            return []

        head_df = df[df["trade_date"] < trade_date]
        if (len(head_df) < self.trend_tool.min_days):
            return []

        head_df = head_df.tail(self.trend_tool.min_days)
        tail_df = df[df["trade_date"].astype(int) >= trade_date]

        head_df = head_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)

        first_row = head_df.iloc[0]
        cur_row = tail_df.iloc[0]
        mid_row = head_df.iloc[int(self.trend_tool.min_days / 2)]
        close_price = cur_row["close"]
        open_price = cur_row["open"]
        vr = cur_row['volume_ratio']
        vol = cur_row['vol']
        pct_chg = cur_row['pct_chg']
        pre_close_price = cur_row['pre_close']
        #近段时间涨幅，时间可以 是120 150 180 个交易日，自由设置
        up_range = (cur_row['close'] - first_row['close']) / first_row['close'] * 100

        #期初 期中 期末均线价格，均线向上
        cur_ma = cur_row[self.ma_name]
        first_ma = first_row[self.ma_name]
        mid_ma = mid_row[self.ma_name]
        if cur_ma < mid_ma or mid_ma < first_ma or cur_ma < first_ma or up_range < 20:
            return []

        #采样均线，判断均线是否向上
        if not self.trend_tool.is_up_trend(head_df):
            return []

        max_price = float(head_df["close"].max())
        max_price_row = head_df[head_df['close'] > max_price - 0.00001].iloc[0];
        max_price_trade_date = int(max_price_row["trade_date"])
        max_price_range = (max_price_row['close'] - max_price_row['open']) / max_price_row['pre_close'] * 100
        if max_price_range < 0:
            return []
        #自高点下来，回撤幅度大于-15
        # fall_range = round((close_price - max_price) / max_price, 2) * 100
        # if fall_range < -15:
        #     return []

        #当日阳线涨幅
        day_range = round((close_price - open_price) / pre_close_price, 2) * 100
        avg_sell_vol = self.trend_tool.calc_sell_volume(head_df)
        is_waitting = self.trend_tool.is_waitting1(head_df) and self.trend_tool.is_waitting(head_df,max_price_trade_date)
        condition1 = is_waitting and (day_range > 3 or pct_chg > 5) and vr > 1.2 and vol > avg_sell_vol
        if condition1:
            return [stock_id]

        return []

class FallBackStrategy(StrategyBase):
    def __init__(self,ma_days,min_days):
        StrategyBase.__init__(self,'FallbackStrategy',ma_days,min_days)
        self.trend_tool = TrendTool(ma_days,min_days)

    def run(self, df, stock_id, total_share, backtest_date):
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return []

        # df[self.ma_name] = ta.MA(df['close'],60)
        if len(df) < self.min_days:
            return []

        head_df = df[df["trade_date"] < trade_date]
        if (len(head_df) < self.min_days):
            return []

        head_df = head_df.tail(self.min_days)
        tail_df = df[df["trade_date"].astype(int) >= trade_date]

        head_df = head_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)

        first_row = head_df.iloc[0]
        cur_row = tail_df.iloc[0]
        pre_row = head_df.iloc[-1]
        pre_range = (pre_row['close'] - pre_row['open']) / pre_row['pre_close'] * 100;
        mid_row = head_df.iloc[int(self.min_days / 2)]
        close_price = cur_row["close"]
        open_price = cur_row["open"]
        vr = cur_row['volume_ratio']
        tor = cur_row['turnover_rate']
        pct_chg = cur_row['pct_chg']
        pre_close_price = cur_row['pre_close']
        pre_pct_chg = pre_row['pct_chg']
        amount = cur_row[self.ma_name] * cur_row["ma_v_" + str(self.ma_days)]
        vol = cur_row['vol']
        cur_day_range = (close_price - open_price) / pre_close_price * 100
        up_range = (cur_row['close'] - first_row['close']) / first_row['close'] * 100

        #起初 期中 期末价格依次向上
        cur_ma = cur_row[self.ma_name]
        first_ma = first_row[self.ma_name]
        mid_ma = mid_row[self.ma_name]
        if cur_ma < mid_ma or mid_ma < first_ma or cur_ma < first_ma or up_range < 20:
            return []

        max_price = float(head_df["close"].max())
        max_price_date = head_df[head_df['close'] > max_price - 0.000001].iloc[-1]['trade_date']
        max_price_df =head_df[head_df['trade_date']> max_price_date]
        if len(max_price_df) > 30 or len(max_price_df) < 8:
            return []
        min_price = max_price_df['close'].min()
        min_price_date = head_df[(head_df['trade_date']> max_price_date) & (head_df['close'] < min_price + 0.000001)]['close'].min()
        max_price_vol = head_df[(head_df['trade_date']> max_price_date) & (head_df['close'] < min_price + 0.000001)]['vol'].max()

        #自高点回撤幅度大于-30%
        max_fall_range = round((min_price - max_price) / max_price, 2) * 100
        if max_fall_range < -25 :
            return []

        fall_range = round((close_price - max_price) / max_price, 2) * 100
        # if fall_range < -15:
        #     return []

        #周期内呈向上涨的趋势(周期可以是 120 150 180天，根据self.min)
        if not self.trend_tool.is_up_trend(head_df):
            return []

        max_vol_line = self.trend_tool.get_max_vol_line(head_df)
        max_vol = max_vol_line[0]
        max_vol_date = max_vol_line[3]
        if(max_vol > max_price_vol * 1.5 and max_price_date < max_vol_date):
            return []

        if close_price < max_vol_line[1]:
            return []

        #当日阳线涨幅大于3且量比大于1且成交量大于之前阴线成交量平均值
        # condition = self.trend_tool.is_greater_buy_power(head_df) and self.trend_tool.is_waitting1(head_df) and vol > sell_vol and cur_day_range > -3 and tor > 2 and amount * 1000 > 300000000
        ma_v_5 = cur_row['ma_v_5']
        ma = cur_row[self.ma_name]
        ma_range = (close_price - ma) / ma  * 100
        condition = ma_range > -10 and self.trend_tool.is_waitting1(head_df) and cur_day_range < 2 and amount*100 > 300000000
        if condition:
            ma = cur_row[self.ma_name]
            ma_fall_rate = round((close_price - ma) / ma * 100, 2)
            return [backtest_date,stock_id, cur_day_range, fall_range, ma_fall_rate]

        return []

class TushareTool:
    token = 'c3f87035d1bdaf8d946f8eca7e0de867e9ef30a40dc226973f44762f'
    ts.set_token(token)
    pro = ts.pro_api()

    def __init__(self):
        pass

    def get_all_stocks(self):
        return TushareTool.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    def get_trade_cal(self,start_date,end_date):
        result = TushareTool.pro.trade_cal(exchange='SSE',start_date=start_date,end_date=end_date);
        return result

    def get_index_daily(self,ts_code):
        result = TushareTool.pro.index_daily(ts_code)
        return result

    def get_bak_basic(self):
        basic_file= './data/Basic.csv'
        result = pd.DataFrame()
        if os.path.exists(basic_file):
            result = pd.read_csv(basic_file)

        if len(result) > 0:
            return result
        today = datetime.today()
        trade_date = today - timedelta(30);
        if len(result) > 0:
            trade_date =datetime.strptime(str(result['trade_date'].max()),'%Y%m%d')

        # keep 15 days
        if trade_date + timedelta(15) > today:
            return result

        trade_date = today
        while len(result) == 0:
            result = TushareTool.pro.bak_basic(trade_date=trade_date.strftime('%Y%m%d'))
            trade_date = trade_date - timedelta(1)

        result.to_csv(basic_file,  index=False,float_format='%.2f')
        return result

    def download_k_line(self,stock, start_date, end_date):
        result = ts.pro_bar(ts_code=stock, start_date=start_date, end_date=end_date, adj='qfq', ma=[5, 10, 20, 30,60],
                            factors=['tor', 'vr'])
        return result

    def download_moneyflow(self):
        result = pd.DataFrame()
        moneyflow_file = "./data/moneyflow.csv"
        if os.path.exists(moneyflow_file):
            result = pd.read_csv(moneyflow_file)

        today = datetime.today()
        end_date = today
        start_date = (today - timedelta(365 * 3))
        trade_cal = self.get_trade_cal(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"));
        end_date = datetime.strptime(str(trade_cal[trade_cal['is_open'] == 1]['cal_date'].max()),"%Y%m%d")

        if len(result) > 0 :
            start_date = datetime.strptime(str(result['trade_date'].max()),"%Y%m%d") + timedelta(1)

        while start_date <= end_date:
            try:
                df = TushareTool.pro.moneyflow(trade_date=start_date.strftime("%Y%m%d"))
            except:
                print("download money error date="+ start_date.strftime("%Y%m%d"))
                continue
            print("download moneyflow date=" + start_date.strftime("%Y%m%d"))
            start_date = start_date + timedelta(1)
            if os.path.exists(moneyflow_file):
                df.to_csv(moneyflow_file, mode='a', header=None, index=False, float_format='%.2f')
            else:
                df.to_csv(moneyflow_file, index=False, float_format='%.2f')

    def load_moneyflow(self):
        result=pd.DataFrame()
        moneyflow_file = "./data/moneyflow.csv"
        if os.path.exists(moneyflow_file):
            result = pd.read_csv(moneyflow_file)
        return result

    def download_daily_line(self,stocks):
        k_line_file = "./data/k_line.csv"
        result = pd.DataFrame()
        if os.path.exists(k_line_file):
            result = pd.read_csv(k_line_file)

        if not result.empty:
            result.sort_values(by=['trade_date'], axis=0, ascending=True,inplace=True)

        today = datetime.today()
        end_date = today.strftime('%Y%m%d')
        start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
        trade_cal = self.get_trade_cal(start_date,end_date);
        end_date = str(trade_cal[trade_cal['is_open'] == 1]['cal_date'].max())
        count = 0

        for index, row in stocks.iterrows():
            stock = row ['ts_code']
            start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
            if len(result) == 0:
                count = count + 1
                df = self.download_k_line(stock, start_date, end_date)
                if not os.path.exists(k_line_file):
                    df.to_csv(k_line_file, index=False, float_format='%.2f')
            else:
                stock_k_line = result[result['ts_code'] == stock]
                start_date = (today - timedelta(365 * 3)).strftime('%Y%m%d')
                last_end_date = start_date
                if (len(stock_k_line) > 0):
                    last_end_date = int(stock_k_line.iloc[-1]['trade_date'])
                    start_date = str(last_end_date )
                if start_date < end_date:
                    count = count + 1
                    df = self.download_k_line(stock,start_date, end_date)
                    if df is None :
                        continue

                    df = pd.concat([stock_k_line,df],axis=0)
                    df = df[df['trade_date'].astype(int) > int(last_end_date)]
                    df.to_csv(k_line_file, mode='a', header=None, index=False, float_format='%.2f')

            print('download k line stock=' + stock + " " + start_date + " " + end_date + " " + str(count))

            if count % 20 == 0:
                time.sleep(1)

class Context:
    def __init__(self):
        self.balance = 1000000
        self.orders = {}
        self.positions = pd.DataFrame()

class BacktestEngine:
    def __init__(self,is_live:bool,ma_days):
        self.ctx = Context()
        self.tushare_tool = TushareTool()
        self.backtest_result = pd.DataFrame(columns=['stock','buy_date','buy_price','profit_5','profit_10','profit_15','profit_20','fall_days','day_range','fall_rate','amount','market_value','moneyflow','strategy_id','ma_name'])
        self.stocks_pool = pd.DataFrame(columns=['stock','begin_date','end_date','begin_price','end_price','diff','rate'])
        self.strategy_signals = pd.DataFrame(columns=['trade_date','stock','day_range','fall_rate','ma_fall_range'])
        self.stocks_pool_file = './data/stocks.csv'
        self.backtest_result_file = './data/backtest.csv'
        self.signals_file = './data/signals.csv'
        self.target_stocks = {}
        self.is_live = is_live
        self.strategies = {}
        self.fall_days = {}
        self.ma_name = "ma" + str(ma_days)

    def add_strategy(self,id, strategy:StrategyBase):
        self.strategies[id] = strategy

    def load_k_line_from_file(self,start_date,end_date):
        result_file = "./data/k_line.csv"

        result = pd.DataFrame()
        if os.path.exists(result_file):
            result = pd.read_csv(result_file)

        result['range'] = (result['close'] - result['open']) / result['pre_close'] * 100
        result = result[(result['trade_date'] >= int(start_date)) & (result['trade_date'] <= int(end_date))]
        return result

    def run(self,begin_date,end_date):
        stock_basic = self.tushare_tool.get_bak_basic()
        k_line = self.load_k_line_from_file(begin_date, end_date)
        k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)

        backtest_date = int(end_date)
        if len(k_line[k_line['trade_date'] == backtest_date]) == 0:
            return

        for index, row in stock_basic.iterrows():
            stock_id = row['ts_code']
            total_share = row['total_share']
            df = k_line[k_line["ts_code"] == stock_id].copy()
            if(df.empty):
                continue
            self.calc_ma(df)

            for id in self.strategies:
                result = self.strategies[id].run(df, stock_id, total_share, backtest_date)
                if len(result) > 0:
                    self.strategy_signals.loc[len(self.strategy_signals)] = result
                    print(result)

        if os.path.exists(self.signals_file):
            self.strategy_signals.to_csv(self.signals_file,header=None,index=False,mode='a',float_format='%.2f')
        else:
            self.strategy_signals.to_csv(self.signals_file,index=False,float_format='%.2f')

    def run_backtest(self,begin_date,end_date):
        stock_basic = self.tushare_tool.get_bak_basic()
        k_line = self.load_k_line_from_file(begin_date, end_date)
        moneyflow = self.tushare_tool.load_moneyflow()
        k_line = k_line.sort_values(by=['trade_date'], axis=0, ascending=True)

        backtest_end_date = datetime.strptime(end_date,'%Y%m%d')
        count = 0
        for index, row in stock_basic.iterrows():
            stock_id = row['ts_code']
            total_share = row['total_share']
            float_share = row['float_share']
            df = k_line[k_line["ts_code"] == stock_id].copy()

            if(df.empty):
                continue


            close_price = df.iloc[-1]['close']
            market_value = total_share * close_price * 100000000
            if market_value < 5000000000 or market_value > 80000000000:
                continue

            if row['pe']  <= 0 or row['pe'] > 200:
                continue

            # if row['profit_yoy'] < 10:
            #     continue
            self.calc_ma(df)

            count = count + 1
            # if count < 1000 :
            #     continue
            trade_date = datetime.strptime(begin_date, '%Y%m%d') + timedelta(260)
            trade_date = datetime.strptime('20210101', '%Y%m%d')
            # trade_date = '20210101'#backtest_end_date - timedelta(30)
            print("start backtest stock_id=" + str(stock_id) + " begin_date = " + begin_date + " start_date=" + trade_date.strftime('%Y%m%d') + " end_date=" + end_date + " count=" + str(count))
            while trade_date <= backtest_end_date:
                backtest_date = trade_date.strftime('%Y%m%d')
                for id in self.strategies:
                    result = self.strategies[id].run(df,stock_id, total_share,backtest_date)
                    if len(result) > 0:
                        net_moneyflow = 0
                        stock_moneyflow = moneyflow[(moneyflow['trade_date'] == int(backtest_date)) & (moneyflow['ts_code'] == stock_id)]
                        if len(stock_moneyflow) > 0:
                            row =  stock_moneyflow.iloc[0]
                            head_df = moneyflow[moneyflow['ts_code'] == stock_id]
                            head_df_10 = head_df.tail(10)
                            sum_df = head_df_10.agg({'buy_lg_amount':'sum','buy_elg_amount':'sum','sell_lg_amount':'sum','sell_elg_amount':'sum','net_mf_amount':'sum'})
                            big_buy_money = sum_df['buy_lg_amount'] * 10000 + sum_df['buy_elg_amount'] * 10000
                            big_sell_money = sum_df['sell_lg_amount'] * 10000 + sum_df['sell_elg_amount'] * 10000
                            net_moneyflow = big_buy_money - big_sell_money
                            net_moneyflow = sum_df['net_mf_amount'] * 10000
                        self.calc_profit_record(stock_id,df,backtest_date,self.strategies[id].strategy_name,self.ma_name,total_share,net_moneyflow)
                trade_date = trade_date + timedelta(1)

    def calc_profit(self,df,trade_date,hold_days):
        profit = 0
        df_len = len(df)
        if df_len < 2:
            return profit
        cur_row = df[df['trade_date'] == trade_date].iloc[0]
        buy_price = cur_row['close']
        fall_days = 0
        max_price = 0
        for i in range(1,hold_days, 1):
            if df_len <= i:
                break
            sell_row = df.iloc[i]
            sell_price = sell_row["close"]
            pct_chg = sell_row['pct_chg']
            profit_rate = (sell_price - buy_price) / buy_price * 100
            profit = round(profit_rate * 100000 / 100, 2)
            if max_price < sell_price:
                max_price = sell_price

            fall_rate = (sell_price - max_price)/max_price * 100
            if (profit_rate < -20 and pct_chg > -9.8 ) or sell_price < sell_row[self.ma_name]:
                break;

        return profit

    def calc_fall_days(self,df,trade_date):
        fall_days = 0
        df = df[df['trade_date'] < int(trade_date)]
        for i in range(-1, -120, -1):
            row = df.iloc[i]
            if row[self.ma_name] > row['close']:
                fall_days += 1
            else:
                break
        return fall_days

    def calc_profit_record(self, stock_id, df, backtest_date,strategy_name,ma_name, total_share,money_flow):
        # order_date = datetime.strptime(backtest_date,'%Y%m%d')
        trade_date = int(backtest_date)
        if len(df[df["trade_date"] == trade_date]) == 0:
            return

        tail_df = df[df["trade_date"].astype(int) >= trade_date]
        if (len(tail_df) == 0):
            return

        tail_df = tail_df.sort_values(by=['trade_date'], axis=0, ascending=True)
        cur_row = tail_df.iloc[0]

        head_df = df[df["trade_date"].astype(int) < trade_date]
        # if stock_id in self.ctx.orders:
        #     last_trade_date = str(self.ctx.orders[stock_id])
        #     if datetime.strptime(last_trade_date, '%Y%m%d') + timedelta(15) >= order_date:
        #         return

        # self.ctx.orders[stock_id] = cur_row['trade_date']

        if self.is_live:
            return

        close_price = cur_row['close']
        market_value = close_price * total_share * 100000000
        open_price = cur_row['open']
        pre_close_price = cur_row['pre_close']
        insert_date = cur_row['trade_date']
        amount = cur_row['amount'] * 1000
        ma = cur_row[self.ma_name]
        day_range = (close_price - open_price) / pre_close_price * 100
        fall_rate = round((close_price - ma) / ma * 100,2)

        max_price = float(head_df["close"].max())
        max_price_date = head_df[head_df['close'] > max_price - 0.000001].iloc[-1]['trade_date']
        max_price_df = head_df[head_df['trade_date'] > max_price_date]
        if len(max_price_df) > 30:
            return []
        min_price = max_price_df['close'].min()
        min_price_date = head_df[(head_df['trade_date'] > max_price_date) & (head_df['close'] < min_price + 0.000001)][
            'close'].min()

        fall_days = (datetime.strptime(backtest_date, '%Y%m%d') -  datetime.strptime(str(max_price_date),'%Y%m%d')).days
        #hold 10 15 20 days
        profit_5 = self.calc_profit(tail_df,trade_date,5)
        profit_10 = self.calc_profit(tail_df,trade_date,10)
        profit_15 = self.calc_profit(tail_df,trade_date,15)
        profit_20 = self.calc_profit(tail_df,trade_date,100)
        # fall_days = self.calc_fall_days(df,trade_date)

        self.backtest_result.loc[len(self.backtest_result)] = [stock_id, insert_date, close_price,profit_5,profit_10,profit_15,profit_20,fall_days,day_range,fall_rate,amount,market_value,money_flow,strategy_name,ma_name]
        if os.path.exists(self.backtest_result_file):
            self.backtest_result.to_csv(self.backtest_result_file, mode='a', header=None, index=False,
                                        float_format='%.2f')
        else:
            self.backtest_result.to_csv(self.backtest_result_file, index=False, float_format='%.2f')

        self.backtest_result.drop(self.backtest_result.index, inplace=True)
        self.stocks_pool.drop(self.stocks_pool.index, inplace=True)

    def calc_ma(self,df:pd.DataFrame):
        df['ma60'] = ta.MA(df['close'], 60)
        df['ma30'] = ta.MA(df['close'], 30)
        df['ma20'] = ta.MA(df['close'], 20)
        df['ma10'] = ta.MA(df['close'], 10)
        df['ma5'] = ta.MA(df['close'], 5)

        df['ma_v_60'] = ta.MA(df['vol'], 60)
        df['ma_v_30'] = ta.MA(df['vol'], 30)
        df['ma_v_20'] = ta.MA(df['vol'], 20)
        df['ma_v_10'] = ta.MA(df['vol'], 10)
        df['ma_v_5'] = ta.MA(df['vol'], 5)


class BacktestThread(threading.Thread):
    def __init__(self, ma_days):
        self.ma_days = ma_days
        super().__init__(name="backtest")

    def run(self) -> None:
        engine = BacktestEngine(False,self.ma_days)
        today = datetime.today()
        end_date = today.strftime('%Y%m%d')
        start_day = (datetime.strptime(end_date, '%Y%m%d') - timedelta(1000)).strftime('%Y%m%d')

        trend_days = 90
        strategy = BullStrategy(self.ma_days,trend_days)
        # engine.add_strategy('BullStrategy', strategy)
        engine.add_strategy('FallBackStrategy',FallBackStrategy(self.ma_days, trend_days))

        engine.backtest_result_file = './data/' + str(self.ma_days)+ '.csv'
        engine.run_backtest(start_day, end_date)
        # engine.run(start_day,end_date)
        engine.ctx.orders.clear()


if __name__ == "__main__":
    #create data dir
    if not os.path.exists('./data/'):
        os.mkdir('./data/')
    engine = BacktestEngine(False,30)
    # stocks = engine.tushare_tool.get_all_stocks()
    stocks = engine.tushare_tool.get_bak_basic()
    today = datetime.today()
    end_date = today.strftime('%Y%m%d')
    start_day = ( datetime.strptime(end_date,'%Y%m%d') - timedelta(1000)).strftime('%Y%m%d')
    engine.tushare_tool.download_daily_line(stocks)
    engine.tushare_tool.download_moneyflow()

    ma_array = [60]
    threads = {}
    for ma_days in ma_array:
        thread = BacktestThread(ma_days)
        threads[ma_days] = thread
        thread.start()

    for thread_id in threads:
        threads[thread_id].join()

    #engine.tushare_tool.download_daily_line(stocks)
    # engine.run(start_day,end_date)
    # engine.run_backtest(start_day,end_date)