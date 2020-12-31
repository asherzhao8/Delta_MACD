import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from custom_function.trade_days_query import trade_days_query
from datetime import timedelta
import os


#---预设参数
start_date = '2019-10-21'
end_date = '2020-05-22'
MONEY = 1e8   #初始资金
N1 = 12       #短均线窗口
N2 = 26       #长均线窗口
N3 = 9        #DEA线窗口
path_index = 'D:/index/'
path_IC = 'D:/IC/'


#---获取数据
filenames = os.listdir(path_index)
files = [file for file in filenames if '000905' in file]
del files[-3]    # 暂取2017年-2020年数据
data = pd.DataFrame()
for file in files:
    data = pd.concat([data, pd.read_csv(path_index + file,index_col=0)])
data.sort_values(['dataDate', 'barTime'],inplace=True)
data.index = pd.to_datetime(data.dataDate + ' ' + data.barTime)


#--聚合指数日频、小时、5分钟数据
data_1min = data['closePrice']
data_5min = data[['closePrice']].resample('5min', label='right').ohlc().dropna()['closePrice']['close']    # 索引09:35表示09:24的收盘价
data_1d = data[['closePrice']].resample('1d', label='left').ohlc().dropna()['closePrice']['close']
idx = list(map(lambda x: x if x.strftime('%H:%M') in ['10:30', '11:30', '14:00', '15:00'] else None, data_5min.index))
data_1h = data_5min[list(filter(None, idx))]    # 将5min数据转换为小时数据


#--IC2006分钟最新价
dates = trade_days_query(start_date, end_date).get_trade_days()
indexdata = pd.Series()
for today in dates:
    index_data = pd.read_csv(path_IC + 'IC2006_' + today.replace('-','') + '.csv', index_col=0)
    index_data.index = pd.to_datetime(index_data.dataDate + ' ' + index_data.dataTime)
    index_data = index_data['lastPrice'].resample('1min', label='right').ohlc().dropna()['close']
    indexdata = pd.concat([indexdata, index_data])


def cal_EMA(price, n):
    '''
    计算指数平均数
    输入价格序列，输出ema序列
    '''
    ema_list = [price[0]]
    for i in range(len(price)-1):
        ema = ema_list[-1] * (1 - 2 / (n + 1)) + price[i+1] * (2 / (n + 1))
        ema_list.append(ema)
    return np.array(ema_list)


def cal_MACD(price_series, N1, N2, N3):
    '''
    计算MACD
    '''
    EMA_short = cal_EMA(price_series, N1)
    EMA_long = cal_EMA(price_series, N2)
    DIF = EMA_short - EMA_long
    DEA = cal_EMA(DIF, N3)
    return 2 * (DIF-DEA)


def refresh_MACD(data, close):
    '''
    输入直至T-1期的价格序列和当前价格
    输出T-1期的MACD和此时的动态MACD
    '''
    EMA_short = cal_EMA(data, N1)                      # 计算直至T-1期的日频ema价格
    EMA_long = cal_EMA(data, N2)
    short = np.append(EMA_short, (1 - 2 / (N1 + 1)) * EMA_short[-1] + (2 / (N1 + 1)) * close)    # 加上当前时刻的ema价格
    long = np.append(EMA_long, (1 - 2 / (N2 + 1)) * EMA_long[-1] + (2 / (N2 + 1)) * close)
    DIF = short - long
    DEA = cal_EMA(DIF, 9)
    return (2 * (DIF-DEA))[-1]


def trade_records(today):
    '''
    交易明细
    输入交易日日期，输出当日收益率
    '''
    data_today = data[data.dataDate == today]['closePrice']                          # T期分钟数据
    entry_time = [pd.to_datetime(today + ' ' + '09:00:00')]                          # 记录入场时间
    money_remain = [MONEY]                                                           # 剩余现金
    shares = [0]                                                                     # 仓位，正数表多仓，负数表空仓
    money = [MONEY]                                                                  # 投资组合价值
    for i in range(len(data_today)):                                                 # T期每分钟计算一次MACD
        time = data_today.index[i]
        close = data_today[i]
        flag = 1                                                                     # 标记入场日
        MACD_1d_now = refresh_MACD(data_1d[:time][-200:-1], close)                   # T期实时日频MACD (只取长度为200的序列计算小时ema(小数点后5位数一致))
        MACD_1h_now = refresh_MACD(data_1h[:time][-200:], close)                   # T期实时小时MACD

        #--判断是否入场
        if (MACD_1d_now > 0) and (MACD_1h_now > 0):
            MACD_5min_now = refresh_MACD(data_5min[:time][-200:], close)
            if i:
                MACD_5min_yes = refresh_MACD(data_5min[:(time - timedelta(minutes=1))][-200:], data_today[i-1])   # 前一分钟计算的5minMACD
            else:
                MACD_5min_yes = cal_MACD(data_5min[:time][-200:], N1, N2, N3)[-1]             # T期计算第一个的MACD需和T-1期的MACD作比较
            if (MACD_5min_yes < 0) and (MACD_5min_now > 0):                          # 5分钟金叉
                flag = 0
                index_price = indexdata[time]
                if shares[-1] < 0:   # 在金叉出现的前五分钟内出现过死叉，先平多仓，再建空仓
                    money_today = money_remain[-1] + shares[-1] * index_price * 200
                    shares.append(int(money_today / index_price / 200))
                    entry_time.append(time)
                    money_remain.append(money_today - shares[-1] * index_price * 200)
                    money.append(money_remain[-1] + shares[-1] * index_price * 200)
                else:
                    money_today = money[-1]
                    shares.append(int(money_today / index_price / 200))
                    entry_time.append(time)
                    money_remain.append(money_today - shares[-1] * index_price * 200)
                    money.append(money_remain[-1] + shares[-1] * index_price * 200)

        elif (MACD_1d_now < 0) and (MACD_1h_now < 0):                                                        
            MACD_5min_now = refresh_MACD(data_5min[:time][-200:], close)
            if i:
                MACD_5min_yes = refresh_MACD(data_5min[:(time - timedelta(minutes=1))][-200:], data_today[i-1])   
            else:
                MACD_5min_yes = cal_MACD(data_5min[:time][-200:], N1, N2, N3)[-1]             
            if (MACD_5min_yes > 0) and (MACD_5min_now < 0):                          # 5分钟死叉
                flag = 0
                index_price = indexdata[time]
                if shares[-1] > 0:   # 在死叉出现的前五分钟内出现过金叉，先平空仓，再建多仓
                    money_today = money_remain[-1] + shares[-1] * index_price * 200
                    shares.append(int(money_today / index_price / 200))
                    entry_time.append(time)
                    money_remain.append(money_today - shares[-1] * index_price * 200)
                    money.append(money_remain[-1] + shares[-1] * index_price * 200)
                else:
                    money_today = money[-1]
                    shares.append(- int(money_today / index_price / 200))
                    entry_time.append(time)
                    money_remain.append(money_today - shares[-1] * index_price * 200)
                    money.append(money_remain[-1] + shares[-1] * index_price * 200)
        else:
            pass

        #--判断是否出场            
        time_after_5min = entry_time[-1] + timedelta(minutes=5)
        if time_after_5min > pd.to_datetime(today + ' ' + '11:30:00'):                    # 11:30后的时间从13:00计起
            time_after_5min = time_after_5min - pd.to_datetime(today + ' ' + '11:30:00') + pd.to_datetime(today + ' ' + '13:00:00')
        else:
            pass
        
        if time_after_5min == time:                                                       # 入场5分钟后出场
            money.append(shares[-1] * indexdata[time] * 200 + money_remain[-1])
            shares.append(0)
            money_remain.append(money[-1])
        elif (time == pd.to_datetime(today + ' ' + '11:30:00')) and (shares[-1] != 0):    # 上午收盘时清仓
            money.append(shares[-1] * indexdata[time] * 200 + money_remain[-1])
            shares.append(0)
            money_remain.append(money[-1])
        elif (time == pd.to_datetime(today + ' ' + '15:00:00')) and (shares[-1] != 0):    # 下午收盘时清仓
            money.append(shares[-1] * indexdata[time] * 200 + money_remain[-1])
            shares.append(0)
            money_remain.append(money[-1])
        elif flag:                                                                        # 未达入出场条件
            shares.append(shares[-1])
            money_remain.append(money_remain[-1])   
            money.append(shares[-1] * indexdata[time] * 200 + money_remain[-1])
        else:
            pass
    
    return (money[-1] - money[0]) / money[0]

if __name__ == '__main__':
    rets = []
    for today in dates:
        rets.append(trade_records(today))
        print(today)
    
    plt.plot(np.array(rets).cumsum())
    plt.show()