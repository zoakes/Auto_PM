#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 28 22:46:35 2020

@author: zoakes
"""

import pandas as pd
import pandas_datareader as pdr
import datetime as dt
import numpy as np

from APM import PortfolioManager
import APM as apm


def get_past_date(months=3):
    from dateutil.relativedelta import relativedelta
    td = dt.date.today()
    mo3 = relativedelta(months=months)
    _ago = td - mo3

    ago_str = _ago.strftime('%Y-%m-%d')
    return ago_str


def sort_beta(le=None,se=None,lb=2020):
    ''' 
    Maintains Long as first 100, Shorts as Second 100 
    CURRENTLY: simply using 2020 data for Beta calc -- could be specificied.
    LB -- Lookback: Int
        Int(4) > 2000 -- Begin Year -- TAKES YEAR IN INT FORM
        Int(2) < 100 -- Months Lookback -- Takes MONTHS in Int Form
    '''
    
    assert (2010 <= lb <= 2025) or (1 <= lb <= 24), \
        'Check LB Argument -- can either be START date, or Month lookback'
    #if le or se is None:
    #    global LE, SE
    #    le = LE
    #    se = SE
    spy_in_se = False
    if 'SPY' not in le:
        le.append('SPY')
    if 'SPY' not in se:
        se.append('SPY')
        spy_in_se = True
    
    #Defaults to beginning on calendar year -- 2020
    if len(str(lb)) == 4 and isinstance(lb,int) and lb > 2000:
        st = lb
    #Otherwise can be used as A MONTHS lookback -- i.e. 3 = 3months from today
    elif lb < 100:
        st = get_past_date(lb)
    #Default of 2020 start used
    else:
        st = 2020
        
    print(f'Lookback date -- {st}')
    

    bm = pdr.DataReader('SPY','yahoo',st).drop(columns=['Close','High','Low','Open','Volume'])
    bm = bm.pct_change()
    bmv = bm.var()
        
    
    LECV = pdr.DataReader(le,'yahoo',st).drop(columns=['High','Low','Open','Close','Volume'])
    #LPCT = LECV.pct_change()
    #LECV.dropna(inplace=True)                                                  #DROPNA !! in case NaNs cause problem!!
    LCV = LECV.pct_change().cov()
    print('Done with Longs')
    
    SECV = pdr.DataReader(se,'yahoo',st).drop(columns=['High','Low','Open','Close','Volume'])
    #spct = SECV.pct_change()
    #SECV.dropna(inplace=True)                                                  #DROPNA !!
    SCV = SECV.pct_change().cov()
    
    print('Done with Shorts')

    long_total = LCV.loc['Adj Close','SPY'] #.sum()
    short_total = SCV.loc['Adj Close','SPY'] #.sum()

    short_total *= -1
    
    #NEW LINE TO COMBINE THEM!! 
    total = long_total.append(short_total)
    
    
    
    
    #Remove multi-index bullshit
    total = total.loc['Adj Close',:]
    
    
    
    if spy_in_se:
        #total.drop(total.tail(1).index,inplace=True) # drop last row if SPY apendded to shorts
        total = total[:-1]
       
    #Apply Dividing by VARIANCE                                                 #TO Calculate True -- UNCOMMENT (No diff)
    #total = total.apply(lambda x: x / bmv)  
    
    return long_total, short_total, total



def get_num_shorts(beta_df):
    '''
    TAKES a sort_betas() 3rd Arg DF!!!
    BREAK when reaches balanced number of shorts
    Returns # Shorts, and final DF
    '''
    #Remove short 'SPY' append
    #if beta_df.to_list()[-1] == 'SPY':
    #    beta_df = beta_df.iloc[:-1]                                             #NEW ADDITION HERE!!! MAKE SURE WORKS
    
    b = beta_df.reset_index()
    b.columns = ['Symbol','B']
    
    b.B.fillna(0,inplace=True) #;                                                          #Added to REMOVE NaNs 
    #Remove long 'SPY' append
    if b.Symbol.iloc[100] == 'SPY':
        b = b.drop(100)
        
    L = b.iloc[:100]
    S = b.iloc[100:]
    #S.sort_values(by='B',ascending=False,inplace=True)
    #b = L.append(S)
    
    #print('S',S)
    

    
    #Initial values -- LOOP THROUGH ALL, maybe all longs can have neg
    pfb = 0 #L.B.sum()
    ct = 0 #100
    idx = 0 #100
    n_shorts = 0
    for row in b.itertuples():
        
        #Increment Beta sum and Count (and Mu)
        pfb += row.B
        ct += 1
        
        #mu = csum / ct #For AVERAGE -- not PF beta (which is SUMMED)
        #print(csum, ct, mu)
        #Break if crosses 0, take the index slice of original
        if pfb < 0:
            print('PFB at halt -- ',pfb)
            n_shorts = row.Index
            idx = n_shorts - 1 # -- Take off most recent, to keep it JUST positive           
            break
        
    print(f'Beta : {pfb - row.B}')
    final = b.iloc[:idx]
    
    return idx - 100, final



def save_to_csv(df,name='beta.csv'):
    if name[-4:] != '.csv':
        name += '.csv'
        
    df.to_csv(name,header=False)
 
###############################################################################

'''Beta Optimized Backtesting Functions'''
    
def get_returns(symbol,lookback=1):
    '''Helper function to calculate returns based on csv'''
    #start = dt.datetime(2020,6,1)                                         #Previously was just '2020' int
    try:
        df = pdr.DataReader(symbol,'yahoo',2020) #.drop(columns=['Volume','Open','High','Low'])
        pct = df.Close.pct_change().tail(lookback).values[0]
    except:
        print('Error calling pdr with ',symbol)
        pct = 0
    finally:                                                                    #ADDED NEW FINALLY BLOCK! IF Error...
        if np.isnan(pct):
            pct = 0
        #print(pct)
        return pct

def get_returns_by_date(symbol,date_str):
    '''Date String should be in format %Y-%m-%d -- 2020-01-03'''
    df = pdr.DataReader(symbol,'yahoo',2020).drop(columns=['Volume','Open','High','Low'])
    pct = df.Close.pct_change()
    try:
        ret = pct.loc[date_str]
    except:
        print('No return for date -- likely a weekend')
        return 0
    return ret
    
    
def parse_beta_entries(path_to_beta='beta.csv'):
    '''Parse beta.csv into longs and shorts -- for backtest'''
    df = pd.read_csv(path_to_beta,header=None)
    df.columns = ['idx','security','b']
    df.set_index('idx',inplace=True)
    secs = df.security.to_list()
    longs = secs[:100]
    shorts = secs[100:]
    return longs, shorts


def calc_beta_return(longs,shorts,slip=1.7,pf_size=100000):
    '''
    Calculate returns, add to total -- calc slippage and real return 
    based on account equity
    '''
    n_pos = len(longs) + len(shorts)
    pos_size = pf_size / n_pos
    
    total = 0
    for symbol in longs:
        ret = get_returns(symbol)
        total += pos_size * (ret)
    long_ret = total
    print('Longs -- USD gain: ',long_ret)
        
    for short in shorts:
        ret = get_returns(short)
        total += pos_size * (ret) * -1
    short_ret = total - long_ret
    print('Shorts -- USD gain: ',short_ret)
    
    real = total - (slip * n_pos)
    print('Total Realized Gain: ', real)
    
    rr = real / pf_size 
    print(f'Total Pct Change -- {rr * 100}')
    return rr


    
    
    
###############################################################################        
if __name__ == '__main__':
    
    '''If running in AM -- NEEED to use fixed list'''
    #Manual entry of symbols -- good for testing IN AM!!!!
    #LE =  ['AMD', 'QQQ', 'MSFT', 'NVDA', 'AAPL', 'INTC', 'NEM', 'ATVI', 'MRVL', 'TSLA', 'SE', 'GFI', 'EBAY', 'LOW', 'DHI', 'REGN', 'FAST', 'PAAS', 'HD', 'TAL', 'AU', 'MAS', 'TSCO', 'TEAM', 'BJ', 'DG', 'ADBE', 'NUAN', 'LEN', 'GIS', 'FTNT', 'GLUU', 'VRTX', 'DXCM', 'HOLX', 'GNMK', 'IMMU', 'BLDP', 'KNX', 'LRCX', 'DT', 'ADSK', 'LULU', 'SWKS', 'SGEN', 'AEM', 'NYT', 'MNST', 'CLX', 'A', 'CERS', 'CDNS', 'PZZA', 'HALO', 'EFX', 'CCI', 'DGX', 'CMI', 'KLAC', 'CTXS', 'AMKR', 'CGEN', 'CHTR', 'IIVI', 'QDEL', 'ROK', 'FLWS', 'OSUR', 'DPZ', 'LLNW', 'JBHT', 'NBIX', 'ITW', 'ILMN', 'TBIO', 'CHK', 'SNPS', 'BLK', 'MNTA', 'PKI', 'BKI', 'GNRC', 'WGO', 'MANH', 'CGNX', 'AVEO', 'MASI', 'LSCC', 'MSCI', 'JKHY', 'WST', 'TREX', 'ALNY', 'NTES', 'NDAQ', 'HUM', 'WMS', 'RGEN', 'ASML', 'SLGN'] 
    #SE =  ['WFC', 'LUV', 'T', 'MGM', 'MRO', 'OXY', 'GPS', 'HST', 'KSS', 'GM', 'KEY', 'HBAN', 'USB', 'BP', 'DVN', 'SPG', 'KIM', 'MAC', 'CNP', 'ING', 'DFS', 'AIG', 'WY', 'CVX', 'ERJ', 'IVZ', 'AES', 'MET', 'CIM', 'WYNN', 'SPR', 'EXPE', 'LVS', 'BCS', 'FTI', 'VTR', 'JCI', 'CNK', 'COF', 'RWT', 'FHN', 'COP', 'MOS', 'EPD', 'CAKE', 'ALK', 'TD', 'PPL', 'OKE', 'DRH', 'RUTH', 'BUD', 'PBF', 'CIT', 'MUR', 'PEB', 'STWD', 'SU', 'EQR', 'EOG', 'OMC', 'RL', 'BEN', 'PRU', 'CUK', 'MFC', 'EAT', 'IR', 'PBCT', 'CLR', 'HFC', 'BG', 'VNO', 'TOT', 'EIX', 'CMA', 'AER', 'JEF', 'XRX', 'BXP', 'HP', 'OHI', 'PVH', 'URBN', 'FNB', 'ATI', 'ARNC', 'SLG', 'PUK', 'ROIC', 'HRB', 'CNQ', 'GIII', 'SHO', 'FLS', 'UMPQ', 'GES', 'ST', 'OI', 'SNV']

    #SE = ['NYT', 'BA', 'WFC', 'DAL', 'MRO', 'MGM', 'RCL', 'XOM', 'LUV', 'GM', 'HST', 'BDN', 'LVS', 'GPS', 'CNP', 'KSS', 'DVN', 'HBAN', 'NOV', 'BP', 'PPL', 'CVX', 'SPG', 'KEY', 'IR', 'FHN', 'SU', 'EPD', 'OKE', 'USB', 'COP', 'MET', 'KIM', 'AIG', 'SJI', 'MTG', 'SPR', 'HRB', 'VLO', 'CNQ', 'STWD', 'CNK', 'PBF', 'EIX', 'IPG', 'COF', 'DRH', 'PRU', 'DFS', 'BCS']
    #LE = ['AAPL', 'PLUG', 'MSFT', 'PENN', 'EBAY', 'GFI', 'ATVI', 'AMAT', 'TSM', 'MRVL', 'SE', 'CRM', 'LLY', 'ABB', 'DHI', 'FAST', 'MAS', 'CDNS', 'TBIO', 'GIS', 'IMMU', 'HD', 'STM', 'NUAN', 'LAKE', 'DT', 'HOLX', 'DG', 'BLDP', 'ADBE', 'OSTK', 'AU', 'BMRN', 'KNX', 'BJ', 'ADI', 'ADSK', 'PAAS', 'AHPI', 'CLX', 'LRCX', 'DLR', 'MKC', 'TEAM', 'INSM', 'A', 'SWKS', 'CLGX', 'CCI', 'CTXS', 'CMC', 'LULU', 'CMI', 'LSCC', 'WGO', 'VRTX', 'PRTS', 'KLAC', 'FTNT', 'TREX', 'ENTG', 'TAL', 'MNTA', 'DXCM', 'HALO', 'NBIX', 'IIVI', 'AVEO', 'EBS', 'SGEN', 'AMKR', 'THO', 'TSCO', 'RMD', 'REGN', 'ALNY', 'QDEL', 'CHTR', 'OTEX', 'WERN', 'BKI', 'ADPT', 'BLK', 'SNPS', 'WSM', 'FORM', 'AMRC', 'ILMN', 'MCO', 'CGEN', 'MOH', 'NTES', 'ROK', 'PEGA', 'SPY', 'NVDA', 'NEM', 'LOW', 'GLUU', 'SAP']
    
    ###############################################################
    #Block to GET CURRENT LE / SE w APM -- (rather than copy paste)
    
    manager = PortfolioManager()

    path = '/Users/zoakes/Desktop/rank/rankanalysis_07-31-2020.csv'
    
    t = dt.datetime.now() #Changed to dt.now() instead of t.today()
    y = t - dt.timedelta(days=1)
    if t.hour < 10:
        assert (path[-14:-4] == y.strftime('%m-%d-%Y')) , 'Check that CSV is YEST date for TODAYS signals'
       
    #Backtest -- OR saving CSV for tomorrow.
    if t.hour >= 10 and t.weekday() != 0:
        assert (path[-14:-4] == t.strftime('%m-%d-%Y')) or \
            (path[-14:-4] == y.strftime('%m-%d-%Y')) , \
            'Check that CSV path is current date for TOMORROW signals -- \
            OR -- Todays date for BACKTEST'
            
    if (path[-14:-4] == t.strftime('%m-%d-%Y')):
        print('Ensure running TODAYS CSV for TOMORROWs signals')
    else:
        print('Ensure running YEST CSV for TODAYs BACKTEST, \n OR \n Todays CSV for TMs Signals')
    
    #apm.SHORTS = 100
    LE, SE = manager.parse(path)
    
    

    assert len(LE) == 100, 'Wait for data to be ready -- NEED 100 long instruments for get_num_shorts'
    #assert len(SE) > 0 , 'NEED SE to balance ... MAKE SURE YOU RUN AT PROPER TIMES!! During/Post market -- NOT 7AM'
    print(len(LE), len(SE))
    
    #END Block of APM
    ###############################################################
    print('Running Beta Optimization...')

    
    
    l, s, b = sort_beta(LE,SE,lb=3) #Default lb (lookback) = 2020 -- can use N for Monthsback
    print(b)
 
    
    #Discrete Optimization
    _, final = get_num_shorts(b)
    print('shape',final.shape)
    
    #print(_)
    #final.fillna(0,inplace=True)
    #write to csv -- to be used by parse_beta in apm4.py
    save_to_csv(final)
    
    
    ##############################################################
    ## Backtesting Beta Op[timized] pf
    print('Calculating Daily Returns...')
    l, s = parse_beta_entries(path_to_beta='beta.csv')  #Added KWARG specification here, can remove.
    
    real = calc_beta_return(l,s,slip=1.0)
    
    
    