import time
from datetime import date
from datetime import datetime, timedelta
import pandas as pd
import math
import talib as tl
from jqdata import gta

def getDivid(stocks): #(context,stocks):
    #year = context.current_dt.year-1
    now = datetime.now()  
    year = now.year-1
    
    #将当前股票池转换为国泰安的6位股票池
    stocks_symbol=[]
    for s in stocks:
        stocks_symbol.append(s[0:6])

    # 累加3年分红信息
    # 如果知道前一年的分红，那么得到前一年的分红数据
    df1 = gta.run_query(query(
            gta.STK_DIVIDEND.SYMBOL,#股票代码
            gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
            gta.STK_DIVIDEND.TOTALDIVIDENDDISTRI,#派息数(实)
            gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
        ).filter(
            gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
            gta.STK_DIVIDEND.DIVDENDYEAR == year,
           #且分红信息在上一年度
            gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol)
        )).dropna(axis=0)
    
    stocks_symbol_this_year=list(df1['SYMBOL'])
    
    #知道前两年的分红数据
    df2 = gta.run_query(query(
            gta.STK_DIVIDEND.SYMBOL,#股票代码
            gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
            gta.STK_DIVIDEND.TOTALDIVIDENDDISTRI,#派息数(实)
            gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
    ).filter(
            gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
            gta.STK_DIVIDEND.DIVDENDYEAR == year-1,
           #且分红信息在上一年度
            gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol)
    )).dropna(axis=0)
    
    #知道前3年的分红数据
    df3 = gta.run_query(query(
            gta.STK_DIVIDEND.SYMBOL,#股票代码
            gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
            gta.STK_DIVIDEND.TOTALDIVIDENDDISTRI,#派息数(实)
            gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
    ).filter(
            gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
            gta.STK_DIVIDEND.DIVDENDYEAR == year-2,
           #且分红信息在上一年度
            gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol)
    )).dropna(axis=0)
    
    # 如果前一年的分红不知道，那么知道前4年的分红数据
    df4 = gta.run_query(query(
            gta.STK_DIVIDEND.SYMBOL,#股票代码
            gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
            gta.STK_DIVIDEND.TOTALDIVIDENDDISTRI,#派息数(实)
            gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
    ).filter(
            gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
            gta.STK_DIVIDEND.DIVDENDYEAR == year-3,
           #且分红信息在上一年度
            gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol),
            gta.STK_DIVIDEND.SYMBOL.notin_(stocks_symbol_this_year)  #不知道今年信息的
    )).dropna(axis=0)
    
    df= pd.concat((df4,df3,df2,df1))
    #print df[(df.SYMBOL == '002495')]
    
    # 下面四行代码用于选择在当前时间内能已知去年股息信息的股票
    df['pubtime'] = map(lambda x: int(x.split('-')[0]+x.split('-')[1]+x.split('-')[2]),df['DECLAREDATE'])
    #print df['pubtime']
    #currenttime  = int(str(context.current_dt)[0:4]+str(context.current_dt)[5:7]+str(context.current_dt)[8:10])
    currenttime  = int(str(now.year)+'{:0>2}'.format(str(now.month))+'{:0>2}'.format(str(now.day)))
    print currenttime
    # 筛选出pubtime小于当前时期的股票，然后剔除'DECLAREDATE','pubtime','SYMBOL'三列
    # 并且将DIVIDENTBT 列转换为float
    df = df[(df.pubtime < currenttime)]
    df['SYMBOL']=map(normalize_code,list(df['SYMBOL']))
    df.index=list(df['SYMBOL'])
        
    df=df.drop(['SYMBOL','pubtime','DECLAREDATE'],axis=1)

    df['DIVIDENTBT'] = map(float, df['DIVIDENTBT'])
    df['TOTALDIVIDENDDISTRI'] = map(float, df['TOTALDIVIDENDDISTRI'])
    
    q_now = query(valuation.code, valuation.capitalization)
    df_now = get_fundamentals(q_now)
    df_now.index=list(df_now['code'])
    #print df_now
    
    #接下来这一步是考虑多次分红的股票，因此需要累加股票的多次分红
    #按照股票代码分堆
    df = df.groupby(df.index).sum()
    df['cap'] = df_now['capitalization']
    #得到当前股价
    #Price=history(1, unit='1d', field='close', security_list=list(df.index), df=True, skip_paused=False, fq='pre')
    
    Price=get_price(list(df.index), count = 1, end_date=now , frequency='daily', fields='close')
    #print Price['close']
    Price=Price['close'].T
    df['pre_close']=Price

    
    #计算股息率 = 股息/股票价格
    df['divpercent']=df['DIVIDENTBT']/df['pre_close']
    df['divpercent2']=df['TOTALDIVIDENDDISTRI']/df['cap']/df['pre_close']/1000
    
    df['code'] = np.array(df.index)
    df = df.sort(['divpercent'], ascending=[False])
    df_name = get_all_securities(['stock'])
    df['name'] = df_name['display_name']
    #print df_name
    return df

stocks = get_index_stocks('000300.XSHG')
stocks = get_all_securities(['stock']).index
getDivid(stocks)