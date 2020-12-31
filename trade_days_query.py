import pandas as pd

class trade_days_query():
    def __init__(self, start_time, end_time, shift=0):
        self.start_time = start_time
        self.end_time = end_time
        self.shift = shift
        self.trade_days = pd.read_csv('D:/download_data/TRADEDAYS/trade_days.csv').time    #Series
    
    def get_trade_days(self):
        '''
        获得起止日期之间的交易日日期
        '''
        return self.trade_days[(self.trade_days >= self.start_time) & (self.trade_days <= self.end_time)].values
        
    def count_trade_days(self):  
        '''
        计算起止日期间的交易日数量，包括首尾日期
        '''
        return self.trade_days[(self.trade_days >= self.start_time) & (self.trade_days <= self.end_time)].count()
        
    def trade_days_shift(self):
        '''
        日期偏移，负数表示向前推，正数向后推
        '''
        try:    
            idx = self.trade_days[self.trade_days == self.start_time].index[0]
            day_shift = self.trade_days[idx + self.shift]
        except:    #输入日期非交易日
            idx = self.trade_days[self.trade_days >= self.start_time].index[0]
            if self.shift > 0:
                day_shift = self.trade_days[idx + self.shift - 1]
            elif self.shift < 0:
                day_shift = self.trade_days[idx + self.shift]
            else:
                raise ValueError('%s is not a trade day, so shift can not be 0' %self.start_time)
        return day_shift
