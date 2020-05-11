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
    
"""

import alpaca_trade_api as tradeapi
import threading
import time
import os
import datetime as dt
import pandas as pd

import logging

LE = []
SE = []

EOD_EXIT = False
TIMEOUT = 120 #Seconds

ep = 'https://paper-api.alpaca.markets'
api_key = 'PK4LB1H4BH7YUF4XOODN'
secret_key = '7EuTmI/C/7FwtNkEzqzFD7yiVMqaI16XfuerzHgR'


class PortfolioManager():
    def __init__(self):
        self.api = tradeapi.REST(base_url = ep, key_id = api_key, secret_key=secret_key)
        
        self.long_OK = self.get_tickers(min_price=10,max_price=10000,etb=False)
        self.short_OK = self.get_tickers(min_price=10,max_price=1000,etb=True)
        
        #self.ETB = self.short_OK
        self.r_positions = {}
        
        #Logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.handler = logging.FileHandler('APM1.2.py.log')
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
                
                self.log('Testing self.r_positions',self.r_positions.get(sym)[0], self.r_positions.get(sym)[1])
                self.log('Testing looped -- qty:',qty)
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
        
        '''To Go back to original, COMMENT NEXT 4 LINES '''
        
        shorts_by_volume = [ticker for ticker in self.short_OK if ticker in SE]     #Sorted SIGNALS by Volume!
        longs_by_volume = [ticker for ticker in self.long_OK if ticker in LE]
        
        LE = longs_by_volume[:100]
        SE = shorts_by_volume[:100]
        
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

        os.environ['APCA_API_KEY_ID'] = api_key
        os.environ['APCA_API_SECRET_KEY'] = secret_key

        #manager = PortfolioManager()

        now = dt.datetime.now()
        if now.hour < 8:
            self.log('Waiting for Warmup + Market Open') 
        
        
        open = False
        warm_up = False
        
        while(True):
            
            now = dt.datetime.now()

        
            if now.hour == 8: #and now.minute < 30:                            
                if not warm_up:
                    self.log(f'Algorithm Warming up...')
                    LE, SE = self.parse(path)
                    self.log(f'Parsed LE {len(LE)} and SE {len(SE)}')
                    
                    arg = self.format_holdings(LE,SE,even_qty)                #Added parameterization for Even Qty Split                           
                    manager.add_items(arg)
                    
                    if len(LE) > 0:
                        self.log(f'Warmup Complete: {dt.datetime.now()}')
                        warm_up = True
                        
                        
                       
            if now.hour == 8 and now.minute >= 30 and warm_up:                 
                self.log('Algorithm Open for Day')
                open = True
                
            elif now.hour == 14 and now.minute == 30 and EOD_EXIT:
                self.log('End Of Day Exit')
                self.liquidate_all()
                open = False
                return 1
            
            elif now.hour > 14:
                self.log('Market Closed.')
                open = False
                break

            if open:
                manager.percent_rebalance(order)                                #Paramterized THIS
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
    
            
###############################################################################
     
if __name__ == '__main__':
    t1 = time.time_ns()

    manager = PortfolioManager()
    
    path = '/Users/zoakes/Desktop/rank/rankanalysis_05-08-2020.csv'
    LE, SE = manager.parse(path)

    
    #An Extra Check
    #assert (path[-14:-4] == dt.datetime.now().strftime('%m-%d-%Y')) , 'Check that CSV is current date'  

    print(f'LE: {len(LE)} \n {LE} \n  SE: {len(SE)} \n {SE}')
    
    arg = manager.format_holdings(LE,SE) #Works -- Add True to split evenly between longs and shorts
    #print('Short OK:')
    #print(manager.short_OK)
    

    manager.run(path,order='block',even_qty=False)  #Time not correct, so not running  : ) #Trying BLOCK
    
    #manager.liquidate_all()
    
    '''
    To Run Algorithm ... 
    1. Check path is to current date
    2.Confirm manager = PortfolioManager() and manager.run() lines are uncommented
    '''