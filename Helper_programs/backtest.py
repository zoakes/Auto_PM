#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 12 17:34:38 2020

@author: zoakes
"""

import datetime as dt
import pandas as pd
import alpaca_trade_api as tradeapi

LE = []
SE = []

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
        
        LE = longs_by_volume[:100]
        SE = shorts_by_volume[:100]
        
        return LE, SE
    
    def calc_return(self,path,LE,SE,slip_usd=3,fill_pct=.8,usd=False):

        #df = self.pct_chg                                                     #CHANGED TO PCT_CHG DF!! 
        '''Alternate -- Need to make this a default, check it, then define this otherwise'''
        df = pd.read_csv(path,header=None)
        df.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
        
        positions = len(LE) + len(SE)
        total_l, total_s = 0,0
        for i in df.itertuples():
            if i.SYMBOL in LE:
                total_l += 500 * (i.P_Change/100)
            elif i.SYMBOL in SE:
                total_s += -500 * (i.P_Change/100)
        
        total_usd = total_l + total_s
        
        real_usd = total_usd * fill_pct - (slip_usd * positions) 
        if usd:
            return real_usd 
        return (real_usd / 100000) * 100 


if __name__ == '__main__':

    
    path = '/Users/zoakes/Desktop/rank/rankanalysis_05-14-2020.csv'
    today_path = '/Users/zoakes/Desktop/rank/rankanalysis_05-15-2020.csv'
    
    y = dt.datetime.now() - dt.timedelta(days=1)
    #assert (path[-14:-4] == y.strftime('%m-%d-%Y')) , 'Check that CSV is current date' 

    bt = Backtest(path,today_path)
    #print('Long OK',bt.long_OK)
    #print('Short OK',bt.short_OK)
    #LE, SE = bt.parse()                                                        #Don't Parse !! (If so, need YEST csv for TODAYs signals)
    print(len(LE),len(SE))
    #print(LE)
    #print(SE)
    ''' 
    Can also just calculate w copy paste -- SAFER, as not 2 paths needed ! 
    '''
    LE =  ['QQQ', 'MSFT', 'AMD', 'KGC', 'AAPL', 'NVDA', 'NFLX', 'GFI', 'EBAY', 'SE', 'TLT', 'NEM', 'GIS', 'TAL', 'ATVI', 'HD', 'KLAC', 'IMMU', 'AU', 'LLY', 'GLUU', 'SHY', 'QDEL', 'HOLX', 'CGEN', 'GNMK', 'OSUR', 'AMGN', 'VRTX', 'FTNT', 'HALO', 'CDNS', 'DT', 'CPB', 'FLWS', 'PRTS', 'VGSH', 'MNTA', 'SGEN', 'SNPS', 'AEM', 'LOGI', 'PETS', 'SMG', 'DG', 'DXCM', 'CTXS', 'TREX', 'TEAM', 'CLX', 'ASML', 'ACMR', 'SLGN', 'MASI', 'NTES', 'PZZA', 'MKC', 'RGLD', 'FORM', 'JKHY', 'RGEN', 'LMNX', 'WGO', 'ALNY', 'NBR', 'VMBS', 'ENTG', 'CYTK', 'HUM', 'MPWR', 'DPZ', 'AHPI', 'WERN', 'TBIO', 'ATRC', 'CRUS', 'CCC', 'BKI', 'CNST', 'SAIA', 'NDSN', 'NVMI', 'TECD', 'VGIT', 'MRTN', 'WST', 'TYL', 'MOH', 'EBS', 'POWI', 'AVEO', 'MSCI', 'VGLT', 'STMP', 'AUDC', 'AXE', 'TECH', 'CALX', 'SMED', 'CRL'] 
    SE =  ['WFC', 'T', 'OXY', 'LUV', 'C', 'JBLU', 'KEY', 'MGM', 'MRO', 'RF', 'USB', 'SLB', 'HBAN', 'HAL', 'SPG', 'IVZ', 'FHN', 'RDS.A', 'KIM', 'NLY', 'FITB', 'LVS', 'KSS', 'GM', 'HPQ', 'EPD', 'CNP', 'HST', 'GPS', 'SPR', 'RDS.B', 'PPL', 'EOG', 'STWD', 'JCI', 'UA', 'SU', 'ING', 'BP', 'CVX', 'OKE', 'SHO', 'EMR', 'AA', 'FTI', 'AGNC', 'UNM', 'GLW', 'UPS', 'COF', 'PBF', 'ROIC', 'PBCT', 'DD', 'FLR', 'DVN', 'IPG', 'AES', 'MTG', 'FNB', 'SIX', 'AIG', 'BK', 'TD', 'WY', 'IRM', 'CS', 'DFS', 'COP', 'SLG', 'EQR', 'IR', 'FDX', 'MAC', 'DOW', 'MUR', 'BWA', 'O', 'VNO', 'PRU', 'WU', 'ORI', 'MMM', 'CIM', 'MET', 'WDC', 'WYNN', 'OMC', 'CF', 'MFC', 'AEO', 'PXD', 'TOT', 'EPR', 'TRV', 'IBM', 'JEF', 'AFL', 'CTSH', 'KAR']
    #calc_return(path,LE,SE,slip_usd=5,fill_pct=.65)
    
    print(bt.calc_return(today_path,LE,SE,slip_usd=1.5,fill_pct=200/200)) #Negative Rates
    