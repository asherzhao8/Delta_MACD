# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 17:19:12 2020

@author: 赵聪
"""

from iFinDPy import *
import pandas as pd
import time
import os
import pymysql

import warnings
warnings.filterwarnings('ignore')
    
#---判断ifind是否已经登录成功
thsLogin = THS_iFinDLogin('cinda1023', '645981')
if(thsLogin == 0 or thsLogin == -201):
    print('ifind登录成功')
else:
    print("ifind登录失败")
    
#---预设参数
path_base = 'D:/download_data/BASEDATA/'   #行情数据存储路径
path_fin = 'D:/download_data/FINDATA/'    #财务数据存储路径
path_index = 'D:/download_data/INDEXDATA/'   #指数数据存储路径
market_cols = ['preClose', 'open', 'high', 'low', 'close', 'changeRatio', 'volume', 'amount', \
               'turnoverRatio', 'totalCapital', 'floatCapitalOfAShares', 'pe_ttm']
date_cols = ['ths_pb_latest_stock', 'ths_pcf_operating_cash_flow_ttm_stock', 'ths_trading_status_stock', \
             'ths_up_and_down_status_stock', 'ths_the_citic_industry_stock', 'ths_ipo_date_stock']
#today = '2020-08-26'    #today = datetime.now().strftime('%Y-%m-%d')   #今日日期
index_codes = ['000016.SH', '000300.SH', '000905.SH']   #需获取的指数成分  暂不取中证1000：'000852.SH'

def download_market(today):
    '''
    下载行情数据
    '''
    start = time.time()
    #---取全A股代码和名称
    all_sec = THS_DataPool('block', today + ';001005010', 'thscode:Y,security_name:Y', True)
    all_sec = THS_Trans2DataFrame(all_sec)
    all_codes = list(all_sec.THSCODE.values)
    
    #---获取行情数据
    # 包括：前收、开高低收价、涨跌幅、成交量、成交额、换手率、总市值、流通市值、市净率、市盈率、市现率、交易状态、涨跌停状态、
    # 所属中信一级行业名称、上市时间、后前复权因子、特殊交易状态
    basedata1 = THS_HistoryQuotes(all_codes, ';'.join(market_cols), 'Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Blank', today, today, True)
    basedata1 = THS_Trans2DataFrame(basedata1)
    basedata2 = THS_DateSerial(all_codes, ';'.join(date_cols), '100;100;;;100;', 'Days:Tradedays,Fill:Blank,Interval:D', today, today, True)
    basedata2 = THS_Trans2DataFrame(basedata2)
    basedata2.columns = ['time', 'thscode', 'pb_lf', 'pcf_ttm', 'trade_status', 'maxupordown', 'citic_industry', 'ipo_date']
    basedata = pd.merge(basedata1, basedata2, how='outer')   #basedata1会去掉某些未交易的股票，但这部分信息也需保留，basedata2中的股票是全的
    basedata.sort_values('thscode', inplace=True)
    basedata.reset_index(drop=True, inplace=True)
    
    b_close = THS_HistoryQuotes(all_codes, 'close', 'Interval:D,CPS:5,baseDate:1900-01-01,Currency:YSHB,fill:Blank', today, today, True)
    b_close = THS_Trans2DataFrame(b_close)
    b_close.rename(columns={'close': 'b_close'}, inplace=True)
    b_close = pd.merge(basedata[['time','thscode']], b_close, how='outer')
    b_close['b_close'] = b_close['b_close'] / basedata['close'].values   #后复权因子
    f_close = THS_HistoryQuotes(all_codes, 'close', 'Interval:D,CPS:4,baseDate:1900-01-01,Currency:YSHB,fill:Blank', today, today, True)
    f_close = THS_Trans2DataFrame(f_close)
    f_close.rename(columns={'close': 'f_close'}, inplace=True)
    f_close = pd.merge(basedata[['time','thscode']], f_close, how='outer')
    f_close['f_close'] = f_close['f_close'] / basedata['close'].values   #前复权因子
    basedata = pd.concat([basedata, b_close['b_close'], f_close['f_close']], axis=1)
    
    basedata.insert(2, 'code_name', all_sec.SECURITY_NAME)
    basedata[['changeRatio', 'turnoverRatio']] = basedata[['changeRatio', 'turnoverRatio']] / 100   #百分比指标
    basedata['special_trade_type'] = basedata['code_name'].map(lambda x: 1 if 'ST' in x else 0)    #ST股贴标签
    basedata.set_index('thscode', inplace=True)
    
    #--存储数据
    basedata.to_csv(path_base + 'BASEDATA' + today + '.csv', encoding = 'utf-8-sig')
    basedata.to_hdf(path_base + 'BASEDATA.h5', today, mode = 'a')   #以日期为索引将所有数据存为一个文件，追加模式（文件不存在则新建）  读取：pd.read_hdf(path_base + 'BASEDATA.h5', today)
    if os.path.exists(path_base + 'BASEDATA.csv'):    #只在第一次保留列名
        basedata.to_csv(path_base + 'BASEDATA.csv', encoding = 'utf-8-sig', mode = 'a', header = 0)
    else:
        basedata.to_csv(path_base + 'BASEDATA.csv', encoding = 'utf-8-sig', mode = 'a')
    #--行情数据下载至数据库
    con = pymysql.connect(host='localhost', user='root', password='583191126q', database='market_data', charset='utf8')
    
    data = basedata.reset_index()
    data = data.astype(str)
    data[data == 'nan'] = None  #将NaN值插入MySQL前要重置为None
    
    cursor =con.cursor()  #获取一个光标
    sql = 'INSERT INTO market VALUES(' + ','.join(['%s'] * data.shape[1]) + ');'
    for i in range(data.shape[0]):
        cursor.execute(sql,list(data.iloc[i,:].values))
    con.commit()
    cursor.close()
    con.close()
    
    print('%s 数据下载完成，共耗时 %s s' %(today, str(time.time() - start)))
    
def download_index(today):
    '''
    下载指数成分数据
    '''
    for index in index_codes:
        indexdata = THS_DataPool('index', today + ';' + index, 'thscode:Y,weight:Y',True)
        indexdata = THS_Trans2DataFrame(indexdata)
        indexdata.columns = ['code', 'weights']
        indexdata['weights'] = indexdata['weights'] / 100
        indexdata.set_index('code',inplace = True)
        indexdata.to_csv(path_index + 'INDEXCOMPONENTSWEIGHT' + index[:6] + '-' + today + '.csv', encoding = 'utf-8-sig')
        print('%s %s 已下载' %(today, index))

#---主程序
dates = THS_DateQuery('SSE','dateType:0,period:D,dateFormat:0','2013-01-01','2014-08-08')
dates = dates['tables']['time'][::-1]
for today in dates:
    download_market(today)
    #download_index(today)