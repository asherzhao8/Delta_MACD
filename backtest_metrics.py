import pandas as pd
import numpy as np

class backtest_metrics():
    def __init__(self, rets, transfer='close', window=None, benchmark='000300.SH'):
        self.rets_series = rets         #策略(日)收益率Series
        self.rets = rets.values         #策略(日)收益率array
        self.transfer = transfer        #调仓时间：可选close(收盘前) or open(开盘后)，默认收盘前
        self.start = rets.index[0]      #策略开始时间，计算IR时需输入
        self.end = rets.index[-1]       #策略结束时间，计算Sharpe、IR时需输入
        self.benchmark = benchmark      #比较基准，计算IR时需输入，默认沪深300
        self.dates_len = len(rets)
        if window is None:              #调仓频率list，调仓日值为1，其他为0，默认每日调仓，即[1,1,...,1]
            self.freq = [1] * self.dates_len
        else:
            self.freq = [0] * len(rets) 
            for i in list(range(0, len(rets), window))[1:]:
                self.freq[i] = 1
     
    def assets(self):
        '''
        净值
        '''
        assets = (self.rets + 1).cumprod()
        return assets
    
    def ret_ann(self):
        '''
        年化收益率
        '''
        assets = self.assets()
        ret_ann = assets[-1] ** (252 / self.dates_len) - 1
        ret_ann = round(ret_ann * 100, 2)
        return ret_ann
    
    def maxDrawdown(self):
        '''
        最大回撤
        '''
        assets = (self.rets_series + 1).cumprod()
        expanding_max = assets.expanding().max()#asset.expanding().max()为滚动最高值
        remains = assets/expanding_max
        maxdrawdown = (1-remains.min())*100
        end_date = remains[remains.values==remains.min()].index[0]
        max_max = expanding_max[expanding_max.index==end_date]  #最大回撤区域对应的最高值
        start_date = expanding_max[expanding_max.values==max_max.values].index[0]
        return round(maxdrawdown, 2), start_date, end_date
    
    def win_prob(self):
        '''
        胜率
        '''
        if self.transfer == 'close':
            #-收盘前调仓
            idxes = np.append(0, np.where(np.array(self.freq) == 1)[0] + 1)
            rets_cumsum = np.array([self.rets[idxes[i]: idxes[i+1]].sum() for i in range(len(idxes)-1)])
        elif self.transfer == 'open':
            #-开盘后调仓
            idxes = np.append(0, np.where(np.array(self.freq) == 1)[0])
            rets_cumsum = np.array([self.rets[idxes[i]: idxes[i+1]].sum() for i in range(len(idxes)-1)])
        else:
            print('需要确定调仓时间于收盘前或是开盘后')
        win_prob = len(rets_cumsum[rets_cumsum > 0]) / len(rets_cumsum) * 100
        win_prob = round(win_prob, 2)
        return win_prob
    
    # def p_l_ratio(self):
    #     '''
    #     盈亏比
    #     '''
    #     assets = self.assets()
    #     profit_delta = np.append(1, assets)[: -1] * self.rets    #每日绝对盈亏
    #     p_l_ratio = profit_delta[profit_delta > 0].sum() / profit_delta[profit_delta < 0].sum() * (-1)
    #     p_l_ratio = round(p_l_ratio, 2)
    #     return p_l_ratio
    
    def p_l_ratio(self):
        '''
        盈亏比
        '''
        assets = self.assets()
        profit_delta = np.append(1, assets)[: -1] * self.rets    #每日绝对盈亏
        if self.transfer == 'close':
            #-收盘前调仓
            idxes = np.where(np.array(self.freq) == 1)[0]
            profit = [profit_delta[i] for i in idxes]
        elif self.transfer == 'open':
            #-开盘后调仓
            idxes = np.where(np.array(self.freq) == 1)[0] - 1
            profit = [profit_delta[i] for i in idxes]
        else:
            print('需要确定调仓时间于收盘前或是开盘后')
        profit = np.array(profit)
        p_l_ratio = profit[profit > 0].sum() / profit[profit < 0].sum() * (-1)
        p_l_ratio = round(p_l_ratio, 2)
        return p_l_ratio
    
    def sharpe(self):
        '''
        sharpe比率
        '''
        ret_ann = self.ret_ann()
        rf_his = pd.read_csv('D:/download_data/R-007/R-007.csv', index_col=0)
        rf = rf_his[rf_his.index == self.end].close.values[0]
        sigma = self.rets.std() * 252 ** 0.5    #年化标准差
        sharpe = (ret_ann / 100 - rf) / sigma
        sharpe = round(sharpe, 2)
        return sharpe
    
    def IR(self):
        '''
        信息比率
        '''
        if self.benchmark == '000016.SH':
            index_ret = pd.read_csv('D:/download_data/INDEXDATA/INDEXHISTORY000016.csv')
        elif self.benchmark == '000300.SH':
            index_ret = pd.read_csv('D:/download_data/INDEXDATA/INDEXHISTORY000300.csv')
        elif self.benchmark == '000905.SH':
            index_ret = pd.read_csv('D:/download_data/INDEXDATA/INDEXHISTORY000905.csv')
        else:
            print('请输入正确的基准指数代码：000016.SH 或 000300.SH 或 000905.SH')        
        index_ret_IR = index_ret[(index_ret.time >= self.start) & (index_ret.time <= self.end)].changeRatio
        try:
            rt_minus = self.rets - index_ret_IR.values
        except:
            print('请检查起止日期是否输入或格式有误')
        rt_minus_year = (self.rets + 1).cumprod()[-1] - (index_ret_IR + 1).cumprod().values[-1]    #年化相对收益率
        sigma_minus = rt_minus.std() * len(rt_minus) ** 0.5    #年化标准差
        IR = rt_minus_year / sigma_minus
        IR = round(IR, 2)
        return IR
    
    def metrics(self):
        '''
        所有评估指标
        '''
        maxD = self.maxDrawdown()
        metrics = {
            'ret_ann%': self.ret_ann(),
            'maxDrawdown%': maxD[0],
            'maxDrawdown_start': maxD[1],
            'maxDrawdown_end': maxD[2],
            'win_prob%': self.win_prob(),
            'p_l_ratio': self.p_l_ratio(),
            'sharpe_ratio': self.sharpe(),
            'IR': self.IR()
        }
        return metrics