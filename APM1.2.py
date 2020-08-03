#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  7 20:56:55 2020

@author: zoakes

Created By: Zach Oakes

Version 1.2.3:
    5.07.20 -- Added Short support, and OO run functions to simplify execution
    5.07.20 -- Resolved LISTS from Parse function (Not NDArray)
    5.08.20 -- Added Universe Filtering (10 - 1k, sorted by volume), 200 total entries
    5.08.20 -- Added order / execution type Parameter (block, send, timeout (Default))
    5.11.20 -- Added Logging features
    5.15.20 -- Added SHORTS global, and block to rm pct filter on longs

    
"""

import alpaca_trade_api as tradeapi
import threading
import time
import os
import datetime as dt
import pandas as pd
import logging
import sys


LE = []
SE = []

EOD_EXIT = False
TIMEOUT = 120 #Seconds
_MIN = 5
_MAX = 850 #Use Allocation as Max -- 100k/200 == 500

SHORTS = 0 #CHANGE MAX WHEN THIS CHANGES!

ep = 'https://paper-api.alpaca.markets'
api_key = 'API_KEY'
secret_key = 'SECRET_KEY'


class PortfolioManager():
    
    def __init__(self):
        self.api = tradeapi.REST(base_url = ep, key_id = api_key, secret_key=secret_key)
        self.logger = logging.getLogger(__name__)
        
        self.long_OK = self.get_tickers(min_price = _MIN, max_price = _MAX,etb=False)
        self.short_OK = self.get_tickers(min_price = _MIN, max_price = _MAX,etb=True)
        
        #self.ETB = self.short_OK
        self.r_positions = {}
        self.fill_history = {}
        n = dt.datetime.now()
        ns = n.strftime('%m-%d-%Y')
        self.logger.setLevel(logging.INFO)
        self.handler = logging.FileHandler(f'APM1.2.py_{ns}_.log')
        self.handler.setLevel(logging.INFO)
        self.formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)
        self.logger.info('Starting log at {}'.format(dt.datetime.now()))

        
    def log(self, msg=""):
        """Add log to output file"""
        self.logger.info(msg)
        #if not USING_NOTEBOOK:                                                 #CHGD
        print(msg)
        


    def format_percent(self, num):
        if(str(num)[-1] == "%"):
            return float(num[:-1]) / 100
        else:
            return float(num)

    def clear_orders(self):
        try:
            self.api.cancel_all_orders()
            self.log("All open orders cancelled.")
        except Exception as e:
            self.log(f"Error: {str(e)}")

    def add_items(self, data):
        ''' Expects a list of lists containing two items: symbol and position qty/pct
        '''
        for row in data:
            self.r_positions[row[0]] = [row[1], 0]

    def percent_rebalance(self, order_style, timeout=TIMEOUT):
        self.log(f"Desired positions: ")
        for sym in self.r_positions:
            self.log(f"{sym} - {self.r_positions.get(sym)[0]} of portfolio.")
        self.log()

        positions = self.api.list_positions()
        account = self.api.get_account()
        portfolio_val = float(account.portfolio_value)
        for sym in self.r_positions:
            try:
                price = self.api.get_barset(sym, "minute", 1)[sym][0].c
                self.r_positions[sym][0] = int(
                    self.format_percent(
                        self.r_positions.get(sym)[0]) * portfolio_val / price)
            except:
                self.log(f'Skipped {sym}')
                continue

        self.log(f"Current positions: ")
        for position in positions:
            self.log(
                f"{position.symbol} - {round(float(position.market_value) / portfolio_val * 100, 2)}% of portfolio.")
        self.log()

        self.clear_orders()

        self.log("Clearing extraneous positions.")
        for position in positions:
            if(self.r_positions.get(position.symbol)):
                self.r_positions.get(position.symbol)[1] = int(position.qty)
            else:
                self.send_basic_order(
                    position.symbol, position.qty, ("buy", "sell")[
                        position.side == "long"])
        self.log()

        if(order_style == "send"):
            for sym in self.r_positions:
                qty = self.r_positions.get(
                    sym)[0] - self.r_positions.get(sym)[1]
                self.send_basic_order(sym, qty, ("buy", "sell")[qty < 0])
        elif(order_style == "timeout"):
            threads = []
            for i, sym in enumerate(self.r_positions):
                qty = self.r_positions.get(
                    sym)[0] - self.r_positions.get(sym)[1]
                
                print('Testing self.r_positions',self.r_positions.get(sym)[0], self.r_positions.get(sym)[1])
                print('Testing looped -- qty:',qty)
                threads.append(
                    threading.Thread(
                        target=self.timeout_execution, args=(
                            sym, qty, ("buy", "sell")[
                                qty < 0], self.r_positions.get(sym)[0], timeout)))
                threads[i].start()

            for i in range(len(threads)):
                threads[i].join()
        elif(order_style == "block"):
            threads = []
            for i, sym in enumerate(self.r_positions):
                qty = self.r_positions.get(
                    sym)[0] - self.r_positions.get(sym)[1]
                threads.append(
                    threading.Thread(
                        target=self.confirm_full_execution, args=(
                            sym, qty, ("buy", "sell")[
                                qty < 0], self.r_positions.get(sym)[0])))
                threads[i].start()

            for i in range(len(threads)):
                threads[i].join()

    def rebalance(self, order_style, timeout=60):
        self.log(f"Desired positions: ")
        for sym in self.r_positions:
            self.log(f"{sym} - {self.r_positions.get(sym)[0]} shares.")
        self.log("\n")

        positions = self.api.list_positions()

        self.log(f"Current positions: ")
        for position in positions:
            self.log(f"{position.symbol} - {position.qty} shares owned.")
        self.log()

        self.clear_orders()

        self.log("Clearing extraneous positions.")
        for position in positions:
            if(self.r_positions.get(position.symbol)):
                self.r_positions[position.symbol][1] = int(position.qty)
            else:
                self.send_basic_order(
                    position.symbol, position.qty, ("buy", "sell")[
                        position.side == "long"])
        self.log()

        if(order_style == "send"):
            for sym in self.r_positions:
                qty = int(self.r_positions.get(sym)[
                          0]) - self.r_positions.get(sym)[1]
                self.send_basic_order(sym, qty, ("buy", "sell")[qty < 0])
        elif(order_style == "timeout"):
            threads = []
            for i, sym in enumerate(self.r_positions):
                qty = int(self.r_positions.get(sym)[
                          0]) - self.r_positions.get(sym)[1]
                threads.append(
                    threading.Thread(
                        target=self.timeout_execution, args=(
                            sym, qty, ("buy", "sell")[
                                qty < 0], self.r_positions.get(sym)[0], timeout)))
                threads[i].start()

            for i in range(len(threads)):
                threads[i].join()
        elif(order_style == "block"):
            threads = []
            for i, sym in enumerate(self.r_positions):
                qty = int(self.r_positions.get(sym)[
                          0]) - self.r_positions.get(sym)[1]
                threads.append(
                    threading.Thread(
                        target=self.confirm_full_execution, args=(
                            sym, qty, ("buy", "sell")[
                                qty < 0], self.r_positions.get(sym)[0])))
                threads[i].start()

            for i in range(len(threads)):
                threads[i].join()

    def send_basic_order(self, sym, qty, side):
        qty = int(qty)
        if(qty == 0):
            return
        q2 = 0
        try:
            position = self.api.get_position(sym)
            curr_pos = int(position.qty)
            if((curr_pos + qty > 0) != (curr_pos > 0)):
                q2 = curr_pos
                qty = curr_pos + qty
        except BaseException:
            pass
        try:
            if q2 != 0:
                self.api.submit_order(sym, abs(q2), side, "market", "gtc")
                try:
                    self.api.submit_order(sym, abs(qty), side, "market", "gtc")
                except Exception as e:
                    self.log(
                        f"Error: {str(e)}. Order of | {abs(qty) + abs(q2)} {sym} {side} | partially sent ({abs(q2)} shares sent).")
                    return False
            else:
                self.api.submit_order(sym, abs(qty), side, "market", "gtc")
            self.log(f"Order of | {abs(qty) + abs(q2)} {sym} {side} | submitted.")
            return True
        except Exception as e:
            self.log(
                f"Error: {str(e)}. Order of | {abs(qty) + abs(q2)} {sym} {side} | not sent.")
            return False

    def confirm_full_execution(self, sym, qty, side, expected_qty):
        sent = self.send_basic_order(sym, qty, side)
        if(not sent):
            return

        executed = False
        while(not executed):
            try:
                position = self.api.get_position(sym)
                if int(position.qty) == int(expected_qty):
                    executed = True
                else:
                    self.log(f"Waiting on execution for {sym}...")
                    time.sleep(20)
            except BaseException:
                self.log(f"Waiting on execution for {sym}...")
                time.sleep(20)
        self.log(
            f"Order of | {abs(qty)} {sym} {side} | completed.  Position is now {expected_qty} {sym}.")

    def timeout_execution(self, sym, qty, side, expected_qty, timeout):
        sent = self.send_basic_order(sym, qty, side)
        if(not sent):
            return
        output = []
        executed = False
        timer = threading.Thread(
            target=self.set_timeout, args=(
                timeout, output))
        timer.start()
        while(not executed):
            if(len(output) == 0):
                try:
                    position = self.api.get_position(sym)
                    if int(position.qty) == int(expected_qty):
                        executed = True
                    else:
                        self.log(f"Waiting on execution for {sym}...")
                        time.sleep(20)
                except BaseException:
                    self.log(f"Waiting on execution for {sym}...")
                    time.sleep(20)
            else:
                timer.join()
                try:
                    position = self.api.get_position(sym)
                    curr_qty = position.qty
                except BaseException:
                    curr_qty = 0
                self.log(
                    f"Process timeout at {timeout} seconds: order of | {abs(qty)} {sym} {side} | not completed. Position is currently {curr_qty} {sym}.")
                return
        self.log(
            f"Order of | {abs(qty)} {sym} {side} | completed.  Position is now {expected_qty} {sym}.")

    def set_timeout(self, timeout, output):
        time.sleep(timeout)
        output.append(True)
        
        
    '''Zach Additions ------------ 1.0'''
    def format_holdings(self,longs=[],shorts=[],even_qty=False):
        '''Returns proper format for PF Weights'''

        #longs = ['SPY','GLD','AMZN']
        #shorts = ['AAPL','TLT','XOM']

        #account = self.api.get_account()
        #portfolio_val = float(account.portfolio_value) 
        
        #self.log(len(shorts),len(longs))
        #self.log(len(LE),len(SE))
        
        qty = float(1.0 / (len(longs) + len(shorts) + 1))                     #Ensure all fit, Eliminate DBZ errors
        
        if even_qty:
            qty_l = float( .5 / (len(longs) + 1))
            qty_s = -1 * float( .5 / (len(shorts) + 1))
        else:
            qty_l = qty 
            qty_s = -qty
            
        print('QTY -- ',qty, qty_l)
        self.log(f'LQty {qty_l} : SQty {qty_s}')                  
        arg = []
        if len(longs) > 0:
            for l in longs:
                arg.append([l,qty_l])
                
        if len(shorts) > 0:
            for s in shorts:
                arg.append([s,qty_s])


        return arg
    
    def parse(self,path=None):
        #Vectorized operations
        
        global LE, SE
        
        #old_long = LE
        #old_short = SE
        
        #DF = pd.DataFrame({'RA_T':[-1,3,5],'SYMBOL':['AAPL','INTC','AMD'],'P_Change':[1.5,2.5,3.5]})  #TEMPORARY !!!
        if path is not None:
            DF = pd.read_csv(path,header=None)
            DF.columns = ['Exchange','SYMBOL','RA_Y','RA_T','Close','P_Change']
            #DF = pd.read_csv("C:\\Users\\Administrator\\Downloads\\rankanalysis_05-05-2020.csv",header=None)
        
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
        
        #Could re - call self.long_OK + self.short_OK here ? (rather than in run...)
        
        
        shorts_by_volume = [ticker for ticker in self.short_OK if ticker in SE]     #Sorted SIGNALS by Volume!
        longs_by_volume = [ticker for ticker in self.long_OK if ticker in LE]
        
        #Ensure ALWAYS 100 ! (or as close as possible!)
        if len(longs_by_volume) < 100:
            print('Not using pct Filter today...')
            LE = strong.SYMBOL.to_list()
            longs_by_volume = [ticker for ticker in self.long_OK if ticker in LE]
        
        LE = longs_by_volume[:100]
        SE = shorts_by_volume[:SHORTS]
        
        return LE, SE
    
    def run(self,path=None,order='timeout',even_qty=False):                                    #Added Param!!
        '''
        For Even Position Sizing (50% shorts, 50% longs, 
        no matter final size of LE, SE, even_qty = True
        '''
        #from portfolio_manager import PortfolioManager
        #ep = 'https://paper-api.alpaca.markets'
        #api_key = 'PK4LB1H4BH7YUF4XOODN'
        #secret_key = '7EuTmI/C/7FwtNkEzqzFD7yiVMqaI16XfuerzHgR'
        #global LE, SE                                                          #Removed So it RECHECKS -- INSTEAD used locals (lowercase)

        os.environ['APCA_API_KEY_ID'] = api_key
        os.environ['APCA_API_SECRET_KEY'] = secret_key

        #manager = PortfolioManager()

        now = dt.datetime.now()
        self.log('Waiting for Warmup + Market Open...') 
        while True:                                                              #NEW CHECK
            time.sleep(5)
            now = dt.datetime.now()
            if now.minute >= 35 and now.hour >= 8:
                break
        
        
        open = False
        warm_up = False
        self.log('Market Open.')
        while(True):
            
            now = dt.datetime.now()

                
            if now.hour >= 8 and now.minute >= 35:                              #UPDATED TO >= 8          
                if not warm_up:
                    self.log(f'Algorithm Warming up...')
                    #if SKIP_WARM_UP is False:
                    self.log(f'Reloading long_ok short_ok')  # COULD add this to a block in parse()
                    self.long_OK = self.get_tickers(min_price = _MIN, max_price = _MAX,etb=False)
                    self.short_OK = self.get_tickers(min_price = _MIN, max_price = _MAX,etb=True)
                    l, s = self.parse(path)
                    self.log(f'Parsed: LE {len(l)} -- SE {len(s)}')
                    arg = self.format_holdings(l,s,even_qty)                #DONT use globals (LE, SE) -- will reference initial calc, not 8:35 calc                         
                    manager.add_items(arg)
                    
                    
                if len(l) > 95:
                    self.log(f'Warmup Complete: {dt.datetime.now()}')          #Trying this OUTSIDE
                    warm_up = True                                              #UNCOMMENTED THESE GLOBALS !!
                    global LE, SE
                    LE, SE = l, s
                    
                else:
                    self.log(f'Only {len(l)} instruments... skipped entry and resetting long_ok _ short_ok')
                    #self.long_OK = self.get_tickers(min_price = _MIN, max_price = _MAX,etb=False)
                    #self.short_OK = self.get_tickers(min_price = _MIN, max_price = _MAX,etb=True)
                    #break
                        
                        
                       
            if now.hour >= 8 and now.minute >= 35 and warm_up:                  #UPDATED TO >= 8            
                self.log('Algorithm Open for Day')
                open = True
                
            elif now.hour == 14 and now.minute == 50 and EOD_EXIT:
                self.log('End Of Day Exit')
                self.liquidate_all()
                open = False
                return 1
            
            elif now.hour > 14:
                self.log('Market Closed.')
                open = False
                break

            if open:
                self.log(f'Begin Execution: {dt.datetime.now()}')
                manager.percent_rebalance(order)                                #Paramterized THIS
                
                self.get_fill_data()                                    #Try uncommenting to see if this works 
                #^^ could be too soon after?
                return 0
            
        return -1
    
    def liquidate_all(self):
        '''Liquidate ALL positions and orders'''
        orders = self.api.list_orders(status='open')
        positions = self.api.list_positions()
        
        rejected = {}
        if orders or positions:
            if positions:
                self.log(positions)

            if orders:
                self.log("Canceling open orders:")
                self.log([o.id for o in orders])
                result = [self.api.cancel_order(o.id) for o in orders]
                self.log(result)

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
                self.log("Submitted Orders:", closed)
            
            
            for o in closed:
                status = self.api.get_order(o.id)
                if status.status == 'rejected':
                    self.log("ORDER FAILED: Your Order was Rejected!!!")
                    rejected[o.oid] = status.status
                    
        return f'{len(rejected)} Orders Rejected.'
    
    
    def get_tickers(self,min_price=10,max_price=500,etb=True):
        '''
        Returns JSON element (sort_lst) or (uses ticker.ticker for t in res lc)
        SORTED BY VOLUME
        '''
        self.log('Getting current ticker data...')
        tickers = self.api.polygon.all_tickers() #ALL SYMBOLS 
        self.log('All symbols loaded')
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
    
    def get_fill_data(self):
        '''

        Parameters
        ----------
        LE, SE: global list of long/ short entries for day
            If not specified, uses Global.

        Returns
        -------
        float fill_pct
            percent of fills today out of total signals (200).
            1% = 1
        float partial_pct
            percent of partial fills today out of total orders.
        '''
        global LE, SE

        
        #if dt.datetime.now().hour <= 8 and dt.datetime.now().minute <= 45:
        #    self.log('Check for fill data after 8:45AM')
        #    return -1
        
        #Total Position Metrics
        n_pos = len(self.api.list_positions())
        n = len(LE) + len(SE) #Replaced w below...
        if n == 0 or n_pos == 0:  
            self.log(f'No Active Positions -- No fills')
            return -1
        

        
        fill_pct = (n_pos / n) * 100
        
        self.log(f'Number Positions: {n} -- Fills: {fill_pct} %')
        
        #Partial Position Metrics
        partial_fills = len(self.api.list_orders(status='partially_filled'))
        partial_pct = (partial_fills / n_pos) * 100
        #long_orders = self.api.list_orders(side='buy')
        
        #self.log(f'Total Partial Fills: {partial_fills} -- PF Pct {partial_pct} %')
 
                
        td = dt.datetime.today()
        self.fill_history[td] = [fill_pct,partial_pct]                          #Not sure if will work ? 
        

        return fill_pct, partial_pct
    
    
            
###############################################################################
     
if __name__ == '__main__':

    '''Safety Checks -- Path checks -- other initial tests'''
    
    path = '/Users/zoakes/Desktop/rank/rankanalysis_07-31-2020.csv'

    
    #An Extra Check (If AM, day of -- around Entry time) + not monday
    t = dt.date.today().weekday()
    y = dt.datetime.now() - dt.timedelta(days=1)
    if y.hour < 10 and t != 0:
        print('Testing CSV...')
        '''After Holidays - need to comment this'''
        assert (path[-14:-4] == y.strftime('%m-%d-%Y')) , 'Check that CSV is current date' 
        
    try:
        df = pd.read_csv(path)
        #print(df.head())
    except Exception as e:
        print(f'Error -- Please Check that CSV file is present. \n {e}')
        sys.exit()
    finally:
        print(f'Safety Checks done for file: {path[-27:]}') 

    
    manager = PortfolioManager()
    
    SKIP_WARM_UP = True              #True skips today warmup    -- trial to see qty at runtime
    '''may need to SKIP in order to run a fresh one at 8:30 -- ORRR need to re-initialize! for (long_OK)'''
    if not SKIP_WARM_UP:
        LE, SE = manager.parse(path)  
        manager.log(f'LE: {len(LE)} \n {LE} \n  SE: {len(SE)} \n {SE}')
    

    
    '''
    To Run Algorithm ... 
    
    1. Check path is to PREVIOUS date csv
    2. Confirm manager = PortfolioManager() and manager.run() lines are uncommented
    '''
    

    manager.run(path,order='send',even_qty=False)  #Only Enters if hour == 8 #Trying BLOCK vs TIMEOUT
    manager.log('Entry Logic Done running for the day.')
    
    # ----- Tools ----- #
    #Can use as CLI type command in console ->
    
    #Clear ALL positions and orders:
    #manager.liquidate_all() 
    
    #Check on fill data:
    #manager.get_fill_data()
    
    '''
    Changes -- Recent
    --  
    Globals back in -- so fill pct will work -- at ~440
    OR 461 - 443 (changed to >= 8 not == 8) 
    
    '''
    



    
