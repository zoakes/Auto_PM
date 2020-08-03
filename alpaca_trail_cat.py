#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 19:19:14 2020

@author: zoakes

Created by Zach Oakes

Version 1.0.2:
    5.15.20 -- Initial build
    5.29.20 -- Added RE_CONN logic,(to manage disconnects) and threading
    6.17.20 -- Added INTRADAY_PL to logic -- should RESET each day!!
"""

'''Individual Symbol PNL + Trailstop !! '''
#aapl_position = api.get_position('AAPL')
from collections import defaultdict
import alpaca_trade_api as tradeapi
import datetime as dt
import time
import threading
import sys

# Get a list of all of our positions.

DEBUG = False
DEBUG_IND = True
RE_CONN = True
thread_halt = False #Used to STOP thread

FREQ_MINS = 5

open_pnl = 0
hi_dict = {} #defaultdict(float)
trail_on = {} #defaultdict(bool) 
trigger_dict = {}     #defaultdict(float)
TRIGGER_USD = 45      #40(/450) == 10%
TRAIL_PCT = .1        #20% = .2

#CATSTOP MUST BE NEGATIVE to be ON; TGT MUST BE + to be ON
CATSTOP = 0 #-100   
TGT = 0

#Aggregate
TOTAL_TRIG_USD = 550 #300 #625

#If +, WILL NOT BE A TRAILSTOP -- MUST SET negative Values
TOTAL_CATSTOP = 0 #-750 #-1000
TOTAL_TGT = 0


ep = 'https://paper-api.alpaca.markets'
api_key = 'KEY'
secret_key = 'SKEY'


def every(delay,task):
    '''Takes SECONDS'''
    global thread_halt
    next_time = time.time() + delay
    while True:
        now = dt.datetime.now()
        if now.hour >= 15 or thread_halt:
            print('Thread Ending')
            break
            raise('Thread_Ending -- forced')
        time.sleep(max(0,next_time - time.time()))
        try:
            print('Running Task -- ', dt.datetime.now().strftime('%H:%M:%S'))
            task()
        except Exception:
            print('Threading Error')
        next_time += (time.time() - next_time) // delay *  delay + delay



class TrailStop:
    
    def __init__(self):
        self.api = tradeapi.REST(base_url = ep, key_id = api_key, secret_key=secret_key)
        

    def check_trail_cat(self):
        
        #COULD possibly put self.portfolio in the INIT ?   ********************
        if RE_CONN:
            try:
                portfolio = self.api.list_positions()
            except:
                print('Reconnecting...')
                self.api = tradeapi.REST(base_url = ep, key_id = api_key, secret_key=secret_key)
                portfolio = self.api.list_positions()

        else:
            portfolio = self.api.list_positions()                                  #THREW INSIDE TRY-EXCEPT BLOCK
            
                

        global open_pnl 
        
        exit_syms = []
        
        
        
        #Reset TOTAL each time, otherwise MULTIPLYING
        open_pnl = 0
    
        for position in portfolio:
            #print("{} shares of {}".format(position.qty, position.symbol))
            #print(f'PNL {position.unrealized_pl} -- PNL % {position.unrealized_plpc}')
            
            #Total Vars
            open_pnl += float(position.unrealized_intraday_pl)
            
            #Single Vars
            pos_pnl = float(position.unrealized_intraday_pl)
            sym = str(position.symbol)
            
            if not hi_dict.get(position.symbol):
                hi_dict[position.symbol] = float(position.unrealized_intraday_pl)
                
            if float(position.unrealized_intraday_pl) > hi_dict.get(position.symbol):
                hi_dict[position.symbol] = float(position.unrealized_intraday_pl)
                
            if pos_pnl >= TRIGGER_USD and not trail_on.get(sym):
                print(f'TrailStop Triggered {sym}')
                trail_on[sym] = True
                trigger_dict[sym] = hi_dict[sym] * (1-TRAIL_PCT) # * .8 == 20% below high
                
            if CATSTOP < 0:
                if pos_pnl <= CATSTOP:
                    print(f'Cat Stop Exit {sym}')
                    if not DEBUG_IND: self.liquidate_symbols([sym])
                    
            if TGT > 0:
                if pos_pnl >= TGT:
                    print(f'TGT Hit {sym}')
                    if not DEBUG_IND: self.liquidate_symbols([sym])
                
            if trail_on.get(sym):
                trigger_dict[sym] = hi_dict[sym] * (1-TRAIL_PCT)
                
                if pos_pnl <= trigger_dict[sym]:
                    print(f'EXITTING {sym}')
                    #Think this resolves any need for Defaultdict()
                    if not DEBUG_IND: self.liquidate_symbols([sym])                           #Must be in list, so can support multiple!
                    
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
                if not DEBUG: 
                    self.liquidate_all()
                    return -1
                
        if TOTAL_TGT > 0:
            if open_pnl >= TOTAL_TGT:
                print(f'AGG TGT Hit {sym}')
                if not DEBUG: 
                    self.liquidate_all()
                    return 1
                    
                
        if not hi_dict.get('total'):
            hi_dict['total'] = 0
        if open_pnl > hi_dict['total']:
            hi_dict['total'] = open_pnl
            
            
        if open_pnl > TOTAL_TRIG_USD and not trail_on['total']:
            print(f'TrailStop Triggered: TOTAL')
            trail_on['total'] = True
            trigger_dict['total'] = hi_dict['total'] * (1-TRAIL_PCT)
            
        if trail_on.get('total'):
            trigger_dict['total'] = hi_dict['total'] * (1-TRAIL_PCT)
            if open_pnl <= trigger_dict['total']:
                print(f'EXITTING (AGG) FOR DAY!')
                #                                         #Cleaner!
                if not DEBUG: 
                    self.liquidate_all() 
                    
                    #self.liquidate_symbols([p.symbol for p in portfolio])
                trail_on['total'] = False
                hi_dict['total'] = 0
                trigger_dict['total'] = 0
                return 0
                
        
            
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
            print(f'Submitted Orders: {len(closed)}')
            
        for o in closed:
            status = self.api.get_order(o.id)
            if status.status == 'rejected':
                self.log('Order Failed -- Order was Rejected')
                rejected[o.oid] = status.status
        self.log(f'{len(rejected)} Orders Rejected -- {rejected}')
                
        return len(rejected)
    
    
    def liquidate_all(self):
        '''Liquidate ALL positions and orders'''
        orders = self.api.list_orders(status='open')
        positions = self.api.list_positions()
        
        rejected = {}
        if orders or positions:
            if positions:
                print('positions:',len(positions))

            if orders:
                print("Canceling open orders:")
                print([o.id for o in orders])
                result = [self.api.cancel_order(o.id) for o in orders]
                print(result)

            closed = []
            for p in positions:
                side = 'sell'
                #If Short, CLOSE W Buy (else, still closing w sell)
                if int(p.qty) < 0:
                    p.qty = abs(int(p.qty))
                    side = 'buy'
                    
                closed.append(
                    self.api.submit_order(p.symbol, qty=p.qty, side=side, type="market", time_in_force="day")
                    )

            if closed:
                print("Submitted Orders:", len(closed))
            
            
            for o in closed:
                status = self.api.get_order(o.id)
                if status.status == 'rejected':
                    print("ORDER FAILED: Your Order was Rejected!!!")
                    rejected[o.oid] = status.status
                    
        return f'{len(rejected)} Orders Rejected.'
    
    
    def run_thread(self,interval=None):
        global thread_halt, ts
        if interval is None:
            interval = FREQ_MINS
            
        now = dt.datetime.now()

        eodt = 15
        #bodt = 9
        sess_open =  now.hour < eodt
        open = (now.minute >= 35 and now.hour >= 8) or now.hour >= 9
        sess_open = open and sess_open
        #Pre-Market Loop
        if not sess_open: print(f'Waiting for open (8:45) ... ')
        '''
        while True:
            now = dt.datetime.now()
            open = (now.minute >= 35 and now.hour >= 8) or now.hour >= 9
            if open:
                break
            time.sleep(60) #5seconds * 60 seconds = 5mins
        '''
        #RTH Loop
        print(f'Market now Open {now.hour}:{now.minute}')
        try:
            #t = threading.Thread(target=every, args=(60*interval, self.check_trail_cat()))
            t = threading.Thread(target=every, args=(60*interval,ts.check_trail_cat))
            #t.setDaemon(True)
            
            t.start()
        except:
            print('Threading Version Not working -- going to original')
            self.run_stops(interval=interval)
            
        while True:
            now = dt.datetime.now()
            
            #if now.minute >= 16:                                                #TEMPORARY TESTING
            #    print('Breaking')
            #    thread_halt = True
            #    break 
            
            if now.hour > eodt:
                break
        
       
    
        print(f'Market Closed for day.  Trailstop thread closed')
        t.join()
        thread_halt = True

            
    
    
    def run_stops(self,interval=None):
        '''
        Takes interval in MINUTES
        if NO arg provided, uses GLOBAL default (5 mins)
        '''
        if interval is None:
            interval = FREQ_MINS
            
        now = dt.datetime.now()

        eodt = 15
        #bodt = 9
        sess_open =  now.hour < eodt
        open = (now.minute >= 35 and now.hour >= 8) or now.hour >= 9
        sess_open = open and sess_open
        #Pre-Market Loop
        if not sess_open: print(f'Waiting for open (8:45) ... ')
        while True:
            now = dt.datetime.now()
            open = (now.minute >= 35 and now.hour >= 8) or now.hour >= 9
            if open:
                break
            time.sleep(60) #5seconds * 60 seconds = 5mins

        #RTH Loop
        print(f'Market now Open {now.hour}:{now.minute}')
        tm = time.time()
        nxt = tm + (interval * 60)
        while True:
            
            now = dt.datetime.now()
            bt = time.time()

            
            if bt >= nxt:
                print(f'Running Check -- {now.hour}:{now.minute}:{now.second}')
                self.check_trail_cat()
                
                nxt = time.time() + (interval*60) #Make sure this doesnt further incremement (go from 5 min to 10 min)
            
            if now.hour >= 15:
                sess_open = False
                break
            
        print('Market Ended -- TrailStop Package Ended')
        
        
                
                
                
            
            
        
        


if __name__ == '__main__':
    ts = TrailStop()
    
    #Threading Version :
    FREQ_MINS= .5
    
    #t = threading.Thread(target=every, args=(60*FREQ_MINS,ts.check_trail_cat))
    #t.start()
    #if dt.datetime.now().hour >= 15:
    #    t.join()
    #    thread_halt = True

    #NON-Threading Version:
       
    #Test relevant function (once)
    #ts.check_trail_cat()

    #TO RESET CONNECTION EACH LOOP -- TO HANDLE PAPER SERVER ERRORS !
    RE_CONN = True                                                            #SET TO TRUE !
    #SEE LINE 71 -- MAY BE A SIMPLER IMPLEMENTATION! (self.portfolio in INIT -- only need ACTIVE positions)
    
    
    #ts.run_stops(interval=.5) #Default of 5 (via global)
    ts.run_thread(interval=.5)
    
    #t.join()
    
    #ts.liquidate_all()
    
    
