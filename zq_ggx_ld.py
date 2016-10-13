import pandas as pd
import numpy as np
import math
import talib as tl
from jqdata import gta
'''
================================================================================
总体回测前
================================================================================
'''
# 初始化函数，设定要操作的股票、基准等等
#总体回测前要做的事情
def initialize(context):
    set_params()                                # 设置策略常量
    #set_variables()                            # 设置中间变量
    set_backtest()                              # 设置回测条件
    
def set_params():
    #----------------Settings--------------------------------------------------/
    
    g.periodBullBear = 20                       # 某个时间，计算牛熊的
    g.periodBeta = 64
    g.daysDelta = timedelta(days=g.periodBeta)  # 64天内上市的新股不考虑
    g.poolSize = 50                             # 统计前 50 的高息股，求平均股息率
    g.holdSize = 5
    g.dqlx = 2                                  # 大于一年定期存款利率1.85%,買股
    
    # holds buffer
    g.stockBuyList = []
    
    
    g.f = 1.0/g.poolSize*2.0 
    g.apr_min_filter = 0.01
    g.apr_max_filter = 1.00
    #----------------Settings--------------------------------------------------/
    
    #run_monthly(checkMonthly, 1, 'before_open')
    run_monthly(checkMonthly, 1, 'open')
    #run_monthly(swapWeekly, 1, 'open')
    
#3
#设置回测条件
def set_backtest():
    set_option('use_real_price',True)        # 用真实价格交易
    log.set_level('order','debug')           # 设置报错等级
    

# 判断股息
def checkMonthly(context) :
    
    log.debug('checkMonthly')
    allValue = context.portfolio.portfolio_value
    g.holdSize = int(allValue / 20000)
    
    # 取得目标交易标的
    df = get_all_securities(['stock'])
    #获得基金列表 : 'etf' 'fja' 'fjb'
    #df = df[df['type']=='etf']
    df = df[df['start_date'] < (context.current_dt - g.daysDelta).date()]
    # 去除ST股票
    df = df[~df['display_name'].str.contains('ST|退|\*')]
    # 去除不必要的列
    df = df.drop(['start_date', 'end_date', 'type', 'name'], 1)
    
    stockList = list(df.index)
    
    # 去除停牌的股票
    current_data = get_current_data()
    df['paused'] = map(lambda x : current_data[x].paused, stockList)
    df = df[df['paused'] == False]
    stockList = list(df.index)
    
    '''
    1) 总市值全市场从大到小前80%
    2）市盈率全市场从小到大前40%（剔除市盈率为负的股票）
    3）市收率小于2.5
    4）同时满足上述3条的股票，按照股息率从大到小排序，选出股息率最高的50只股票
       等权构建组合
    '''
    
    # 1) 总市值全市场从大到小前80%
    fCap = get_fundamentals(
        query(valuation.code)
        .filter(valuation.code.in_(stockList))
        .order_by(valuation.market_cap.desc())
        .limit(int(len(stockList) * 0.8))
    )
    sListCap = list(fCap['code'])
    log.debug(len(sListCap))
    
    # 2）市盈率全市场从小到大前40%（剔除市盈率为负的股票）
    fPE = get_fundamentals(
        query(valuation.code)
        .filter(valuation.code.in_(stockList), valuation.pe_ratio > 0)
        .order_by(valuation.pe_ratio.asc())
        .limit(int(len(stockList) * 0.4))
    )
    sListPE = list(fPE['code'])
    log.debug(len(sListPE))
    
    # 3）市收率小于2.5
    fPS = get_fundamentals(
        query(valuation.code)
        .filter(valuation.code.in_(stockList)
        , valuation.ps_ratio < 2.5)
    )
    sListPS = list(fPS['code'])
    log.debug(len(sListPS))
    
    stockList = list(set(sListCap) & set(sListPE) & set(sListPS))
    log.debug(len(stockList))
    
    # 4）同时满足上述3条的股票，按照股息率从大到小排序，选出股息率最高的50只股票
    fDivid = get_fundamentals(
        query(cash_flow.code, cash_flow.dividend_interest_payment)
        .filter(cash_flow.code.in_(stockList))
    )
    # 股息率
    fMerge = getDivid(context, stockList)
    
    # 按照股息率从大到小排序
    fMerge = fMerge.sort(['divpercent'], ascending=[False])
    
    # ***************************
    #fDivid['GuxiLv'] = map(lambda x : cal_guxilv(x, context.current_dt), fDivid['code'])
    #fDivid = fDivid.sort(['GuxiLv'], ascending=[False])
    #fDivid = fDivid.dropna()
    #fDivid = fDivid.head(g.poolSize)
    ##fMerge = fDivid
    #log.debug([fMerge, fDivid])
    # ***************************
    
    fMerge = fMerge.head(g.poolSize)
    #log.debug(fMerge)
    #log.debug(fMerge.mean())
    log.debug(fMerge.mean()['divpercent'])
    
    if fMerge.mean()['divpercent'] < g.dqlx:
    # 存錢
        sell_all_stock(context)
    else:
    # 平均利息大于定期利息
        fMerge = fMerge[fMerge.divpercent > 6]
        list_canbuy = list(fMerge['code'])[0:g.holdSize]
        list_sell = set(context.portfolio.positions.keys()) - set(list_canbuy)
        for stock in list_sell:
            order_target_value(stock, 0)
        list_tobuy = set(list_canbuy) - set(context.portfolio.positions.keys())
        if len(list_tobuy) == 0:
            return
        capital_unit = context.portfolio.available_cash/len(list_tobuy)
        for stock in list_tobuy:
            order_target_value(stock, capital_unit)
        


def sell_all_stock(context):
    for stock in context.portfolio.positions.keys():
        order_target_value(stock, 0)

def before_trading_start(context):
    
    set_universe(g.stockBuyList)
    pass

	
def getDivid(context, stocks, year_watch = 3):
    year = context.current_dt.year-1
    #now = datetime.now()  
    #year = now.year-1
    
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
    
    if year_watch == 3:
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
    elif year_watch == 1:
        # 如果前一年的分红不知道，那么知道前4年的分红数据
        df2 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,#股票代码
                gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
                gta.STK_DIVIDEND.TOTALDIVIDENDDISTRI,#派息数(实)
                gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
        ).filter(
                gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
                gta.STK_DIVIDEND.DIVDENDYEAR == year-1,
               #且分红信息在上一年度
                gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol),
                gta.STK_DIVIDEND.SYMBOL.notin_(stocks_symbol_this_year)  #不知道今年信息的
        )).dropna(axis=0)
        df=pd.concat((df2,df1))
    else:
        log.info('不支持1年和3年之外的参数！！！')
        return

    #print df[(df.SYMBOL == '601006')]
    
    # 下面四行代码用于选择在当前时间内能已知去年股息信息的股票
    df['pubtime'] = map(lambda x: int(x.split('-')[0]+x.split('-')[1]+x.split('-')[2]),df['DECLAREDATE'])
    #print df['pubtime']
    currenttime  = int(str(context.current_dt)[0:4]+str(context.current_dt)[5:7]+str(context.current_dt)[8:10])
    #currenttime  = int(str(now.year)+'{:0>2}'.format(str(now.month))+'{:0>2}'.format(str(now.day)))
    #print currenttime
    # 筛选出pubtime小于当前时期的股票，然后剔除'DECLAREDATE','pubtime','SYMBOL'三列
    # 并且将DIVIDENTBT 列转换为float
    df = df[(df.pubtime < currenttime)]
    df['SYMBOL']=map(normalize_code,list(df['SYMBOL']))
    df.index=list(df['SYMBOL'])
        
    df=df.drop(['SYMBOL','pubtime','DECLAREDATE'],axis=1)

    df['DIVIDENTBT'] = map(float, df['DIVIDENTBT'])
    df['TOTALDIVIDENDDISTRI'] = map(float, df['TOTALDIVIDENDDISTRI'])
    
    q_now = query(valuation.code, valuation.market_cap)
    df_now = get_fundamentals(q_now)
    df_now.index=list(df_now['code'])
    #print df_now
    
    #接下来这一步是考虑多次分红的股票，因此需要累加股票的多次分红
    #按照股票代码分堆
    df = df.groupby(df.index).sum()
    df['market_cap'] = df_now['market_cap']
    #得到当前股价
    #Price=history(1, unit='1d', field='close', security_list=list(df.index), df=True, skip_paused=False, fq='pre')
    
    #Price=get_price(list(df.index), count = 1, end_date=now , frequency='daily', fields='close')
    #print Price['close']
    #Price=Price['close'].T
    #print Price
    #Price=Price.T
    #df['pre_close']=Price

    
    #计算股息率 = 股息/股票价格
    #df['divpercent']=df['DIVIDENTBT']/df['pre_close']
    df['divpercent']=df['TOTALDIVIDENDDISTRI']/df['market_cap']/1000000/year_watch
    #print df
    df['code'] = np.array(df.index)
    #print df[(df.code == '601006.XSHG')]
    df = df.sort(['divpercent'], ascending=[False])
    df_name = get_all_securities(['stock'])
    df['name'] = df_name['display_name']
    
    return df

    
    