#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 19:19:14 2020

@author: zoakes
"""

'''Individual Symbol PNL + Trailstop !! '''
#aapl_position = api.get_position('AAPL')
from collections import defaultdict
import alpaca_trade_api as tradeapi
import datetime as dt
import time
import threading

# Get a list of all of our positions.

DEBUG = True

FREQ_MINS = 5

open_pnl = 0
hi_dict = {} #defaultdict(float)
trail_on = {} #defaultdict(bool) 
trigger_dict = {}     #defaultdict(float)
TRIGGER_USD = 45      #40(/450) == 10%
TRAIL_PCT = .2        #20%

TOTAL_USD = 750

#If +, WILL NOT BE A TRAILSTOP -- MUST SET negative Values
TOTAL_CATSTOP =  0 #-1000
CATSTOP = 0 #-100   

ep = 'https://paper-api.alpaca.markets'
api_key = 'PK4LB1H4BH7YUF4XOODN'
secret_key = '7EuTmI/C/7FwtNkEzqzFD7yiVMqaI16XfuerzHgR'


def every(delay,task):
    '''Takes SECONDS'''
    next_time = time.time() + delay
    while True:
        time.sleep(max(0,next_time - time.time()))
        try:
            print('Running Task')
            task()
        except Exception:
            print('Threading Error')
        next_time += (time.time() - next_time) // delay *  delay + delay



class TrailStop:
    
    def __init__(self):
        self.api = tradeapi.REST(base_url = ep, key_id = api_key, secret_key=secret_key)
        

    def check_trail_cat(self):
        global open_pnl 
        
        exit_syms = []
        
        portfolio = self.api.list_positions()
        
        #Reset TOTAL each time, otherwise MULTIPLYING
        open_pnl = 0
    
        for position in portfolio:
            #print("{} shares of {}".format(position.qty, position.symbol))
            #print(f'PNL {position.unrealized_pl} -- PNL % {position.unrealized_plpc}')
            
            #Total Vars
            open_pnl += float(position.unrealized_pl)
            
            #Single Vars
            pos_pnl = float(position.unrealized_pl)
            sym = str(position.symbol)
            
            if not hi_dict.get(position.symbol):
                hi_dict[position.symbol] = float(position.unrealized_pl)
                
            if float(position.unrealized_pl) > hi_dict.get(position.symbol):
                hi_dict[position.symbol] = float(position.unrealized_pl)
                
            if pos_pnl >= TRIGGER_USD and not trail_on.get(sym):
                trail_on[sym] = True
                trigger_dict[sym] = hi_dict[sym] * (1-TRAIL_PCT) # * .8 == 20% below high
                
            if CATSTOP < 0:
                if pos_pnl <= CATSTOP:
                    print(f'Cat Stop Exit {sym}')
                
            if trail_on.get(sym):
                trigger_dict[sym] = hi_dict[sym] * (1-TRAIL_PCT)
                
                if pos_pnl <= trigger_dict[sym]:
                    print(f'EXITTING {sym}')
                    #Think this resolves any need for Defaultdict()
                    if not DEBUG: self.liquidate_symbols([sym])                           #Must be in list, so can support multiple!
                    
                    exit_syms.append(sym)                                    #To exit x symbols once at end...
                    
                    trail_on[sym] = False
                    hi_dict[sym] = 0
                    trigger_dict[sym] = 0
                    
                  
        #self.liquidate_symbols(exit_syms)                                     #To exit x symbols ONCE (at end)
        #exit_syms.remove(sym)
        
        
        #AGGREGATE TGT TRAIL PCT    
    
        if TOTAL_CATSTOP < 0:
            if open_pnl <= TOTAL_CATSTOP:
                print(f'AGG CatStop ')
                self.liquidate_all()
                
        if not hi_dict.get('total'):
            hi_dict['total'] = 0
        if open_pnl > hi_dict['total']:
            hi_dict['total'] = open_pnl
            
        if open_pnl > TOTAL_USD:
            trail_on['total'] = True
            trigger_dict['total'] = hi_dict['total'] * (1-TRAIL_PCT)
            
        if trail_on.get('total'):
            trigger_dict['total'] = hi_dict['total'] * (1-TRAIL_PCT)
            if open_pnl <= trigger_dict['total']:
                print(f'EXITTING (AGG) FOR DAY!')
                #self.liquidate_all()                                          #Cleaner!
                if not DEBUG: self.liquidate_symbols([p.symbol for p in portfolio])
                open_pnl = 0
                trail_on['total'] = False
                hi_dict['total'] = 0
                trigger_dict['total'] = 0
                
        
            
            
        print(f'Total Open PNL: {open_pnl}')
    
    
    def liquidate_symbols(self,symbols):
        '''
        Used to exit One or Many Active Positions 
        (input: list of tickers)
        Returns # Rejected (ideally 0)
        '''
        positions = self.api.list_positions()
        
    
        #Match EXIT symbols with ACTIVE positions, reduce large loop to only relevant
        exit_positions = [p for p in positions if p.symbol in symbols]
        
        rejected = defaultdict(str)
        closed = []
        for p in exit_positions:
            side = 'sell'
            #If short -- CLOSE W BUY
            if int(p.qty) < 0:
                p.qty = abs(int(p.qty))
                side = 'buy'
                
            closed.append(self.api.submit_order(p.symbol, qty=p.qty,side=side, type='market',time_in_force='day'))
            
        if closed:
            self.log(f'Submitted Orders: {closed}')
            
        for o in closed:
            status = self.api.get_order(o.id)
            if status.status == 'rejected':
                self.log('Order Failed -- Order was Rejected')
                rejected[o.oid] = status.status
        self.log(f'{len(rejected)} Orders Rejected -- {rejected}')
                
        return len(rejected)
    
    def run_stops(self,interval=None):
        '''
        Takes interval in MINUTES
        if NO arg provided, uses GLOBAL default (5 mins)
        '''
        if interval is None:
            interval = FREQ_MINS
            
        now = dt.datetime.now()
        tm = time.time()
        nxt = tm + (interval * 60)
        eodt = 15
        bodt = 9
        sess_open = now.hour > bodt and now.hour < eodt

        while sess_open:
            
            now = dt.datetime.now()
            bt = time.time()

            
            if bt >= nxt:
                print(f'Running Check -- {time.time()}')
                self.check_trail_cat()
                
                nxt = time.time() + (interval*60) #Make sure this doesnt further incremement (go from 5 min to 10 min)
            
            if now.hour >= 15:
                break
            
        print('Market Ended -- TrailStop Package Ended')
        
        
                
                
                
            
            
        
        


if __name__ == '__main__':
    ts = TrailStop()
    
    #Threading Version 
    #threading.Thread(target=every, args=(60*FREQ_MINS,ts.check_trail_cat)).start()
    
    
    ts.run_stops()
    #ts.check_trail_cat()
    