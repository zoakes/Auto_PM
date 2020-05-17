#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 13:10:17 2020

@author: zoakes
"""

import pandas_datareader as pdr
import pandas as pd


def get_beta_dicts(longs,shorts):
    if 'SPY' not in longs: longs.append('SPY')
    if 'SPY' not in shorts: shorts.append('SPY')
    
    #Longs
    DFL = pdr.DataReader(longs,'yahoo',2020)
    long_cov = DFL['Adj Close'].pct_change().cov()
    beta_long = long_cov['SPY'].to_dict()
    del beta_long['SPY']
    beta_long = {k: v for k, v in sorted(beta_long.items(), key=lambda item: item[1])}
    print('Beta Long Dict: ',beta_long)
    
    #Shorts -- Need to multiply by -1 at some point.
    DFS = pdr.DataReader(shorts,'yahoo',2020)
    short_cov = DFS['Adj Close'].pct_change().cov()
    
    short_series = short_cov['SPY'] * -1 #Short Entries -- thus *= -1 
    
    beta_short = short_series.to_dict()
    del beta_short['SPY']
    beta_short = {k:v for k,v in sorted(beta_short.items(), key= lambda x: x[1], reverse=True)} #ALSO reverse!
    print('Beta Short Dict: ',beta_short)
    
    return beta_long, beta_short


def DPW(bl,bs):
    '''
    BL is LONG beta dictionary, BS is SHORT beta dictionary
    -- MUST BE SAME LENGTH !
    '''
    pairs = []        
    for k,v in zip(bl.items(),bs.items()):
        #print(k[0],v[0])
        pairs.append([(k[0], v[0]), k[1]+v[1]])
    
    return pairs

def _DPW(bl,bs):
    '''
    For Lists of different Lengths -- use THIS one
    '''
    i = 0
    j = 0
    ns = len(bs.items())
    wprs = []
    while i < len(bl.items()) or j < len(bs.items()):
        n = len(bl.items()) - 1
        ns = len(bs.items()) - 1
    
        longs = list(bl.items())
        shorts = list(bs.items())
    
        l_ele = longs[i][0]
        s_ele = shorts[j][0]
        print(l_ele,s_ele)
        
        #Add first of longs, and FIRST (reversed shorts) of shorts -- for LAST, use [ns-i], and pop(s_ele)
        wprs.append([l_ele,s_ele])
        
        #To loop backwards through shorts...
        #last_ele = shorts[ns-i][0]
        #wprs.append([l_ele,last_ele])
        
        i += 1
        j += 1
        #Gotta be a better way todo this ?
        if i == len(bl.items()) or j == len(bs.items()):
            print('Last Paired Elements: ',longs[i-1][0],shorts[j-1][0])
            break
            
    return wprs

def run_DPW(longs,shorts):
    bl, bs = get_beta_dicts(longs,shorts)  
    if len(longs) == len(shorts):
        pairs = DPW(bl,bs)
    else:
        pairs = _DPW(bl,bs)
    
    return pairs


def create_SR_df(secs,lb=20):
    df = pdr.DataReader(secs,'yahoo',2020)
    #COLS_IN_MIDX = spy_qqq.columns.unique(level=1) #In case we don't have list...
    
    df['Adj Close'] = df['Adj Close'].pct_change()
    
    #Can only do 1x col by loop...
    for i in secs:
        df['MU',i] = df['Adj Close',i].rolling(window=lb).mean()
        
    for i in secs:
        df['SD',i] = df['Adj Close',i].rolling(window=lb).std()
        
    for i in secs:
        df['SR',i] = df['MU',i] / df['SD',i]
        
    
    return df
    
    
    
if __name__ == '__main__':
    LE = ['QQQ', 'MSFT', 'AMD', 'AAPL', 'NVDA', 'NFLX', 'GIS', 'EBAY', 'SE', 'ATVI', 'TLT', 'SHY', 'NEM', 'IMMU', 'HD', 'TAL', 'LLY', 'HOLX', 'QDEL', 'LOGI', 'DT', 'KLAC', 'HALO', 'GNMK', 'AU', 'OSUR', 'FTNT', 'CDNS', 'SGEN', 'MNTA', 'DG', 'AMGN', 'FLWS', 'CYTK', 'DXCM', 'TREX', 'NTES', 'PETS', 'CTXS', 'SLGN', 'CGEN', 'VRTX', 'SNPS', 'TEAM', 'CPB', 'CLX', 'LMNX', 'ATRC', 'ENTG', 'VGSH', 'MASI', 'RGLD', 'PZZA', 'AEM', 'ASML', 'FORM', 'DPZ', 'HUM', 'TBIO', 'MPWR', 'NBR', 'MKC', 'BKI', 'JKHY', 'CRUS', 'CCC', 'WGO', 'VMBS', 'ALNY', 'RGEN', 'ACMR', 'NDSN', 'WERN', 'EBS', 'SMG', 'CNST', 'MSCI', 'AUDC', 'VGIT', 'STMP', 'TECD', 'WST', 'CRL', 'MOH', 'TYL', 'TECH', 'CALX', 'MRTN', 'POWI', 'AHPI', 'SAIA', 'AXE', 'DSPG', 'NVMI', 'AMSWA', 'CYBE', 'VGLT', 'VICR', 'WMK', 'DSGX'] 

    SE = ['T', 'WFC', 'OXY', 'LUV', 'MGM', 'C', 'SLB', 'USB', 'SPG', 'FITB', 'HAL', 'HST', 'EPD', 'GM', 'OKE', 'KSS', 'RDS.A', 'TD', 'PPL', 'RDS.B', 'LVS', 'AGNC', 'HPQ', 'JCI', 'CVX', 'STWD', 'EOG', 'SPR', 'SU', 'BP', 'IR', 'DVN', 'AES', 'GLW', 'PBCT', 'WY', 'MUR', 'CNP', 'SIX', 'MET', 'SLG', 'UPS', 'EPR', 'EMR', 'COP', 'PXD', 'PRU', 'EQR', 'WYNN', 'ORI', 'BK', 'JEF', 'UNM', 'IRM', 'WDC', 'AIG', 'SNV', 'IBM', 'DOW', 'GIL', 'FDX', 'CTSH', 'BEN', 'DFS', 'OHI', 'FNF', 'O', 'CAKE', 'NUE', 'IPG', 'MOS', 'FL', 'VTR', 'COF', 'STL', 'MFC', 'OMC', 'DKS', 'SBGI', 'GEO', 'L', 'MMM', 'STT', 'TOT', 'TOL', 'WU', 'LNC', 'MIDD', 'VNO', 'EXPE', 'DD', 'AFL', 'CNQ', 'HOG', 'LEG', 'KAR', 'SWK', 'EAT', 'FLS', 'CNK']
    #PAIRS = run_DPW(LE,SE)    
    #print(PAIRS)
    print(create_SR_df(['AAPL','AMZN']))