#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 20:37:46 2020

@author: zoakes
"""

import pandas as pd
import pandas_datareader as pdr
import datetime as dt
import numpy as np

from APM import PortfolioManager
import APM as apm

import beta_discrete_optimize as bdo

from os import listdir

BT = {}
RTL = []
DL = []

##############################################################


def get_cum_ret(df,rm_pct=False,add_to_df=False):
    '''
    Args: 
        df -- Date + Return columns or Return column
        optional:
            rm_pct -- Boolean, default to off (to go from PCT to decimal)
            add_to_df -- Bool, def Off -- to Add column to df (Slower)

    Return: either DF with new cumulative return column, or 
    NP.Series of cumulative returns
    
    '''
    if len(df.columns) == 1:
        df.columns = ['ret']
        
    elif len(df.columns) == 2:
        df.columns = ['date','ret']
        df.sort_values(by='date').set_index('date') 
        
    if rm_pct:
        df.ret = df.ret.apply(lambda row: row / 100) #If need to go back from pct...

    cum_ret = np.cumprod(1 + df['ret'].values) - 1
    if add_to_df:
        df['cumulative'] = cum_ret
        return df
    return cum_ret



def split_beta_BT(df):
    '''Parse beta.csv into longs and shorts -- for backtest'''
    #df = pd.read_csv(path_to_beta,header=None)
    if len(df.columns) == 3:
        df.columns = ['idx','security','b']
        df.set_index('idx',inplace=True)
    else:
        df.columns = ['security','b']

    
    secs = df.security.to_list()
    longs = secs[:100]
    shorts = secs[100:]
    return longs, shorts




##############################################################

def get_active_dates(begin=2020):#
    '''
    Maybe should be looping through DIRECTORY with a LS command?
    '''
    
    spy = pdr.DataReader('SPY','yahoo',begin)
    spy.reset_index(inplace=True)
    dates = spy.Date.to_list()
    
    dstr = [str(d)[:10] for d in dates]
    
    return dstr

def get_next_mkt_day(day,fwd = 1):
    '''Gets next OPEN, MKT day -- fwd is how many days to jump'''
    if day[0:4] != '2020':
        date_str = reformat_date(day,jump=False)
    else:
        date_str = day
    df = pdr.DataReader('SPY','yahoo',date_str).drop(columns=['Volume','Open','High','Low'])
    dates = df.reset_index().Date.to_list()
    #Slice time off date (first 10 digits), and return 2nd item (idx 1) in list -- NEXT date
    dstr = [str(d)[:10] for d in dates][fwd] 
    return dstr


def find_csv_filenames( path_to_dir, suffix=".csv" ):
    '''BETTER version...'''
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

def reformat_date(day,jump=False):
    '''Reformat from 06-10-2020 BACK to 2020-06-10 -- and GO FORWARD 1 DAY!!
    Need to skip forward one day -- KEEP AT FALSE -- using GET_RETS_BY_DATE jump instead
    maybe better to jump one row in DF? (BC of weekends)
    '''
    from datetime import datetime
    date_object = datetime.strptime(day, "%m-%d-%Y")
    #Switch to CORRECT date (for RETURN calc)
    if jump:
        if date_object.weekday() == 4: #If Friday
            date_object = date_object + dt.timedelta(days=2) #Add ANOTHER 2 days (for weekend)
        date_object = date_object + dt.timedelta(days=1) #Add 1 day -- normal

    date = date_object.strftime('%Y-%m-%d')
    
    return date #date_object #WAS RETURNING DATE -- NOW DATETIME DATE OBJECT!

def get_returns_by_date(symbol,date_str,jump=True):
    '''Date String should be in format %Y-%m-%d -- 2020-01-03
    CAN ALSO SKIP FORWARD 1 to get correct index (and business day)
    '''
    try:
        start = dt.datetime(2020,6,1)  
        df = pdr.DataReader(symbol,'yahoo',start).drop(columns=['Volume','Open','High','Low'])
        pct = df.Close.pct_change()
    except:
        print('Cannot get returns for ',symbol)
        return 0
    
    try:
        if jump:
            ret = pct.loc[date_str:][1] #GETS 1 day FORWARD!!
        else:
            ret = pct.loc[date_str]
    except:
        print('No return for date -- using 0')
        return 0
    return ret


##############################################################


def calc_beta_bt(day,longs,shorts,slip=1.7,pf_size=100000):
    '''
    Args: 
        day -- in PATH/csv_date format
        longs -- list of symbols
        shorts -- lst of symbols
    optional:
        slip -- (Slippage) in $ per RT
        pf_size -- in USD, for % calculation
        
    Calculate returns, add to total -- calc slippage and real return 
    based on account equity
    ONLY needs to jump 1x,(in reformat, or get_rets) 
        better to do in DF as timedelta has flaws in wkd, holiday
    USES DATE, as well as symbols...
    RETURNS:
        total daily return in decimal -- .01 == 1%, .1 == 10%
        
    '''
    n_pos = len(longs) + len(shorts)
    pos_size = pf_size / n_pos
    #date =  MAY NEED TO FORMAT BACK TO 2020-01-01 from 06-10-2020
    date = reformat_date(day,jump=False)
    
    total = 0
    for symbol in longs:
        ret = get_returns_by_date(symbol,date,jump=True)                      #TO SKIP FORWARD 1 BUS DAY -- takes return of NEXT business day after csv date
        total += pos_size * (ret)
    long_ret = total
    print('Longs -- USD gain: ',long_ret)
        
    for short in shorts:
        ret = get_returns_by_date(short,date,jump=True)                       #TO SKIP FORWARD 1 BUS DAY 
        total += pos_size * (ret) * -1
    short_ret = total - long_ret
    print('Shorts -- USD gain: ',short_ret)
    
    real = total - (slip * n_pos)
    print('Total Realized Gain: ', real)
    
    rr = real / pf_size 
    print(f'Total Pct Change: {day} -- {rr * 100}')
    return rr


###############################################################################    
    
if __name__ == '__main__':
    
    
    manager = PortfolioManager()
    
    '''BRAND NEW -- never tested'''
    beg = '/Users/zoakes/Desktop/rank/'
    files = find_csv_filenames('/Users/zoakes/Desktop/rank/')
    
    
    paths = [beg + file for file in files]
    
    '''TEMPORARY TEST -- smaller dataset'''
    paths = ['/Users/zoakes/Desktop/rank/rankanalysis_07-22-2020.csv']

    print('Beginning Backtest (super) loop...',dt.datetime.now().strftime('%H:%M:%S'))
    for path in paths:
        #path = '/Users/zoakes/Desktop/rank/rankanalysis_06-12-2020.csv'
        day = path[-14:-4] 
        CSV_Date = day #For checks 
        print(f'BackTesting for {day} (csv date).')
    
        
        apm.SHORTS = 100
        LE, SE = manager.parse(path)
    
        assert len(LE) == 100, 'Wait for data to be ready -- NEED 100 long instruments for get_num_shorts'
        assert len(SE) > 0, 'SE must be full, to balance beta...'
        #END Block of APM
        ###############################################################
    
        l, s, b = bdo.sort_beta(LE,SE,lb=3) #Default lb (lookback) = 2020 -- can use N for Monthsback
        #print(b)
 
    
        #Discrete Optimization
        _, final = bdo.get_num_shorts(b)
    
        #NO NEED TO WRITE TO CSV -- SIMPLY READ THE FINAL DF 
    
        #write to csv -- to be used by parse_beta in apm4.py
        #save_to_csv(final)
    
        ##############################################################
        ## Backtesting Beta Optimized pf
    
        #NEEDed TO REWRITE THIS A BIT -- NO LONGER READING CSV
        #l, s = parse_beta_entries(path_to_beta='beta.csv')
        #print(final.head())
    
        l, s = split_beta_BT(final)
    
        day_ret = calc_beta_bt( day, l, s )                                     #CHANGED THIS SLIGHTLY !!! CHECK IT!
        
        #format_date = reformat_date(day,jump=False)                           #Replaced this !!
        bt_date = get_next_mkt_day(day)
        BT[bt_date] = day_ret * 100  #CONFIRM NOT MULTIPLYING BY 100 TWICE!!
        
        #Add to lists -- (as a backup...)
        RTL.append(day_ret * 100)
        DL.append(bt_date)
        print(f'Done with {day} i.e. BT for {bt_date}.')
        
    ##############################################################
    #Formatting aggregated results -- Dict -> DF -> CSV
    
    #BE Super Careful bc DONT want to ruin results after super loop...

    print('Done with Backtest Loop!')
    try:
        dates = list(BT.keys())
        rets = list(BT.values())
        BTDF = pd.DataFrame({'Date': dates, 'Return': rets})
        BDF = BTDF.sort_values(by='Date').set_index('Date')
        print('DF Created...')
    except:
        BTDF = pd.DataFrame({'Date': DL, 'Return': RTL})
        BDF = BTDF.sort_values(by='Date').set_index('Date')
        print('Backup DF Created.')
        
    try:
        BDF.to_csv('backtest.csv',header=False)
        print('Success (to_csv).')
        
    except:
        print('to_csv Not working... trying save_to...')

        
    finally:
        
        print('REMEMBER -- CSV (prev) day shows BT (next bus day) SIGNALS...')
        print('DF uses next_mkt_day func so proper dates.')
     

    
    
    ###########################################################
    
    print('Taking true cumulative return...')
    bdf = BDF

    
    cr_df = get_cum_ret(bdf,rm_pct=True,add_to_df=True)
    cr_df.to_csv('cumu.csv',header=False)
    
    cr = cr_df.cumulative.to_list()
    #cr = get_cum_ret(bdf,rm_pct=True)
    print(f'True Cumulative Return -- {cr[-1]}')

    
    
    
    
    

    
    