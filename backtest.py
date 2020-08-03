#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 12 17:34:38 2020

@author: zoakes
"""

import datetime as dt
import pandas as pd

import pandas_datareader as pdr
import alpaca_trade_api as tradeapi
from collections import defaultdict

from APM import PortfolioManager


LE = []
SE = []
SHORTS = 50
mu_beta_dict = defaultdict(float)
sum_beta_dict = defaultdict(float)
beta_dict = defaultdict(list)

ep = 'https://paper-api.alpaca.markets'
api_key = 'PK4LB1H4BH7YUF4XOODN'
secret_key = '7EuTmI/C/7FwtNkEzqzFD7yiVMqaI16XfuerzHgR'

class Backtest:
    
    def __init__(self,path=None,pct_chg_path=None):
        self.api = tradeapi.REST(base_url = ep, key_id = api_key, secret_key=secret_key)
        
        self.long_OK = self.get_tickers(min_price = 10, max_price = 450,etb=False)
        self.short_OK = self.get_tickers(min_price = 10, max_price = 450,etb=True)
        
        if path is not None:
            self.path = path
            self.df = pd.read_csv(path,header=None)
            self.df.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
        #self.LE, self.SE = self.parse(path)
        
        if pct_chg_path is None:
            self.pct_chg = pd.read_csv(pct_chg_path,header=None)
            self.pct_chg.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
    

    def get_tickers(self,min_price=10,max_price=500,etb=True):
        '''
        Returns JSON element (sort_lst) or (uses ticker.ticker for t in res lc)
        SORTED BY VOLUME
        '''
        print('Getting current ticker data...')
        tickers = self.api.polygon.all_tickers() #ALL SYMBOLS 
        print('All symbols loaded')
        assets = self.api.list_assets()
        if etb: #FILTER TRADABLE SYMBOLS; easy to borrow if true
            symbols = [asset.symbol for asset in assets if asset.tradable and asset.easy_to_borrow]
        else:
            symbols = [asset.symbol for asset in assets if asset.tradable] 
        
        #JUST return sort_lst if you want full JSON object -- else return LC
        sort_lst = sorted([ticker for ticker in tickers if ( 
            ticker.ticker in symbols and
            ticker.lastTrade['p'] >= min_price and
            ticker.lastTrade['p'] <= max_price and
            ticker.prevDay['v'] * ticker.lastTrade['p'] > 500000 #and
            #ticker.todaysChangePerc <= 3.5
        )], key = lambda t: t.prevDay['v'], reverse=True)
        
        return [ticker.ticker for ticker in sort_lst]
    

    def parse(self,path=None):
        #Vectorized operations
        
        global LE, SE
        
        #old_long = LE
        #old_short = SE
        
        #DF = pd.DataFrame({'RA_T':[-1,3,5],'SYMBOL':['AAPL','INTC','AMD'],'P_Change':[1.5,2.5,3.5]})  #TEMPORARY !!!
        if path is None:
            DF = pd.read_csv(self.path,header=None)
            DF.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
            #DF = pd.read_csv("C:\\Users\\Administrator\\Downloads\\rankanalysis_05-05-2020.csv",header=None)
        else:
            DF = pd.read_csv(path,header=None)
            DF.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
        
        #LE's
        strong = DF[DF['RA_T'] >= 5]
        mu = DF['P_Change'].mean()
        sig = DF['P_Change'].std()
        no_out = strong[strong['P_Change'] <= (mu+sig)]
        
        #SE's 
        weak = DF[DF['RA_T'] <= -5]
        #short_new = weak[weak['P_Change'] <= (mu+sig)]
        
        LE = no_out.SYMBOL.to_list()
        SE = weak.SYMBOL.to_list()
        
        '''To Go back to original, COMMENT NEXT 4 LINES '''
        
        shorts_by_volume = [ticker for ticker in self.short_OK if ticker in SE]     #Sorted SIGNALS by Volume!
        longs_by_volume = [ticker for ticker in self.long_OK if ticker in LE]
        
        print('Base LongsByVol (w pct filt): ',len(longs_by_volume))
        if len(longs_by_volume) < 100:
            print('Not using pct Filter today...')
            LE = strong.SYMBOL.to_list()
            longs_by_volume = [ticker for ticker in self.long_OK if ticker in LE]
        
        LE = longs_by_volume[:100]
        SE = shorts_by_volume[:SHORTS]
        
        return LE, SE
    
    def calc_return(self,path,LE,SE,slip_usd=3,fill_pct=.8,usd=False):

        #df = self.pct_chg                                                     #CHANGED TO PCT_CHG DF!! 
        '''Alternate -- Need to make this a default, check it, then define this otherwise'''
        df = pd.read_csv(path,header=None)
        df.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
        
        positions = len(LE) + len(SE)
        pos_size = 100000 / positions
        total_l, total_s = 0,0
        for i in df.itertuples():
            if i.SYMBOL in LE:
                total_l += pos_size * (i.P_Change/100)
            elif i.SYMBOL in SE:
                total_s += -pos_size * (i.P_Change/100)
        
        total_usd = total_l + total_s
        
        real_usd = total_usd * fill_pct - (slip_usd * positions) 
        print('Real USD -- ', real_usd)
        if usd:
            return real_usd 
        return (real_usd / 100000) * 100 
    
    

    def sort_by_PIT_volume(self,symbols,begin_str):
        assert isinstance(symbols,list), 'Please Use List as Input'
        df = pdr.DataReader(symbols,'yahoo',begin_str) #Confirm end_str not break?
        vol = df.Volume
        
        dates = [row.strftime('%m-%d-%Y') for row, index in df.iterrows()]
        assert isinstance(dates[0],str), 'Strftime() not working -- please check dates LC'
        
        sorted_symbols = defaultdict(list)
        for d in dates:
            tmp = vol.sort_values(by=d,axis=1,ascending=False)
            sorted_symbols[d].append(tmp.columns.to_list())
         
        assert len(sorted_symbols) == len(dates), 'Index and Dict length not matching -- check both vars'
        return sorted_symbols
    
    
    def estimate_beta(self,le=None,se=None,agg='M'):
        '''Most accurate is with MEAN and NO * 1/200'''
        if le or se is None:
            global LE, SE
            le = LE
            se = SE
        if 'SPY' not in le:
            le.append('SPY')
        if 'SPY' not in se:
            se.append('SPY')
            
        bm = pdr.DataReader('SPY','yahoo',2020).drop(columns=['Close','High','Low','Open','Volume'])
        bm = bm.pct_change()
        bmv = bm.var()
            
        print('Beginning Long Beta Calcs...')
        LECV = pdr.DataReader(le,'yahoo',2020)
        LCV = LECV.pct_change().cov()
        
        
        
        print('Beginning Short Beta Calcs...')
        SECV = pdr.DataReader(se,'yahoo',2020)
        SCV = SECV.pct_change().cov()
        
        #Remove multi-index bullshit -- DOES NOT WORK  HERE? 
        #lc = LCV.loc['Adj Close',:]
        #sc = SCV.loc['Adj Close',:]
        #print(sc)
        #print(lc.loc['SPY'])
        
        if agg == 'S':
            print('Dont use sum...')
            #long_total = lc.loc['SPY'].sum()
            #short_total = sc.loc['SPY'].sum()
        else:
            long_total = LCV.loc['Adj Close','SPY'].mean()
            #short_total = sc.loc['SPY'].mean()
            short_total = SCV.loc['Adj Close','SPY'].mean()  #OLD DUMB WAY W MIDX
        short_total *= -1
        
        long_total = (long_total / bmv) #* 1/200
        short_total = (short_total / bmv) #* 1/200
        
        total = long_total[0] + short_total[0]
        print('LE B:',long_total[0])
        print('Total B:',total)
        
        return total
        


if __name__ == '__main__':

    
    path = '/Users/zoakes/Desktop/rank/rankanalysis_06-15-2020.csv'
    today_path = '/Users/zoakes/Desktop/rank/rankanalysis_06-16-2020.csv'
    
    y = dt.datetime.now() - dt.timedelta(days=1)
    #assert (path[-14:-4] == y.strftime('%m-%d-%Y')) , 'Check that CSV is current date' 

    
    bt = Backtest(path,today_path)
    #print('Long OK',bt.long_OK)
    #print('Short OK',bt.short_OK)
    
    '''Beta Calc --- '''
    SHORTS = 0 #25
    LE, SE = bt.parse(today_path)                                                        #Don't Parse !! (If so, need YEST csv for TODAYs signals)
    print(len(LE),len(SE))

    '''need to add var of spy in this calc...'''

    #beta = bt.estimate_beta(agg='m')
    #print('Beta:',beta)
    

    #W/ 50 (sum) -- -.019239
    #w 50 se (mean) -- -.0003265
    
    #W/ 25 SE (sum) -- -.0033075
    #W/ 25 SE (mu) --  -0.000331
    
    #w/ 20 SE (sum) - -0.0003158
    #w/ 20 SE (15th) -- -0.0003146
    #POSITIVE !!
    
    #W/ 15 SE (sum) -- 0.0994 
    #W/ 15 SE (sum 15th) -- -0.000318

    #W/ 10 SE (sum) -- 0.11429
    #W/ 10 (15th) -- -0.000286
    
    #/0 (15th) -- L.O.: 0.000254
    
    #MU is actually correct in this context
    

    
    
    
    
    
    
    ''' 
    Can also just calculate w copy paste -- SAFER, as not 2 paths needed ! 
    '''
    LE =  ['AMD', 'TQQQ', 'MSFT', 'AAPL', 'LOW', 'QQQ', 'KGC', 'INTC', 'NVDA', 'MRVL', 'TLT', 'SE', 'TSLA', 'TTWO', 'GFI', 'ATVI', 'NFLX', 'EBAY', 'PAAS', 'TAL', 'NEM', 'HD', 'LLY', 'VCIT', 'VCSH', 'ADBE', 'MNST', 'NUAN', 'CDNS', 'GIS', 'IMMU', 'DHI', 'MAS', 'SWKS', 'SHY', 'NTES', 'OSUR', 'VRTX', 'ACMR', 'DT', 'HOLX', 'IIVI', 'GLUU', 'AU', 'MNTA', 'LSCC', 'LLNW', 'LULU', 'ASML', 'BLDP', 'VGSH', 'TREX', 'NBIX', 'ADSK', 'DGX', 'CTXS', 'VCLT', 'LMNX', 'ROK', 'TEAM', 'VMBS', 'ACAD', 'ILMN', 'TBIO', 'LOGI', 'FTNT', 'ALNY', 'ADPT', 'GNMK', 'CLX', 'VGIT', 'SGEN', 'HALO', 'KLAC', 'AEM', 'SNPS', 'DXCM', 'WERN', 'CHTR', 'WST', 'NDAQ', 'MELI', 'MASI', 'AVEO', 'RMBS', 'CGNX', 'ANSS', 'REGN', 'RDY', 'ENTG', 'BF.B', 'WGO', 'TECH', 'DVA', 'DPZ', 'TYL', 'CRUS', 'HUM', 'NDSN', 'PZZA'] 
    SE =  ['WFC', 'T', 'OXY', 'LUV', 'C', 'JBLU', 'KEY', 'MGM', 'MRO', 'RF', 'USB', 'SLB', 'HBAN', 'HAL', 'SPG', 'IVZ', 'FHN', 'RDS.A', 'KIM', 'NLY', 'FITB', 'LVS', 'KSS', 'GM', 'HPQ', 'EPD', 'CNP', 'HST', 'GPS', 'SPR', 'RDS.B', 'PPL', 'EOG', 'STWD', 'JCI', 'UA', 'SU', 'ING', 'BP', 'CVX', 'OKE', 'SHO', 'EMR', 'AA', 'FTI', 'AGNC', 'UNM', 'GLW', 'UPS', 'COF', 'PBF', 'ROIC', 'PBCT', 'DD', 'FLR', 'DVN', 'IPG', 'AES', 'MTG', 'FNB', 'SIX', 'AIG', 'BK', 'TD', 'WY', 'IRM', 'CS', 'DFS', 'COP', 'SLG', 'EQR', 'IR', 'FDX', 'MAC', 'DOW', 'MUR', 'BWA', 'O', 'VNO', 'PRU', 'WU', 'ORI', 'MMM', 'CIM', 'MET', 'WDC', 'WYNN', 'OMC', 'CF', 'MFC', 'AEO', 'PXD', 'TOT', 'EPR', 'TRV', 'IBM', 'JEF', 'AFL', 'CTSH', 'KAR']
    
    
    '''
    manager = PortfolioManager()
    LE, SE = manager.parse(path)
    print(len(LE),len(SE))
    '''
    
    LE = LE[:50] #Sweet SPOT ?! @ 5 - 50?
    SE = [] #SE[:10]
    #bt.calc_return(path,LE,SE,slip_usd=5,fill_pct=.65)
    
    #bt.estimate_beta(agg='M')
    
    bt.calc_return(today_path,LE,SE)
    
    
    #print(bt.calc_return(today_path,LE,slip_usd=1.5,fill_pct=1/1)) #Negative Rates
    
    
    #print(bt.sort_by_PIT_volume(['SPY','QQQ','USO'],2019))

    