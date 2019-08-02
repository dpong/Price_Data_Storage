#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 21:52:10 2019

@author: dpong
"""

import pandas_datareader as pdr
import pandas as pd
import sqlite3, pickle, os
from datetime import datetime, date, timedelta



class Collecting_data():

    def __init__(self,dataset):
        self.dataset = dataset
        self.conn = sqlite3.connect(
                '/Users/dpong/Documents/Personal/Trading_Python_Code/Data_storage/Price_Data/{}_Stock_Price.db'.format(self.dataset))
        self.cursor = self.conn.cursor()
        self.tickers = []
        
        if os.path.exists('/Users/dpong/Documents/Personal/Trading_Python_Code/Data_storage/Price_Data/{}_Exclude_list.pickle'.format(self.dataset)):
            with open('/Users/dpong/Documents/Personal/Trading_Python_Code/Data_storage/Price_Data/{}_Exclude_list.pickle'.format(self.dataset),'rb') as d:
                self.ticker_excluded = pickle.load(d)
        else:
            self.ticker_excluded = []
    
    def close_connection(self):
        self.conn.close()
 
    def get_tickers(self):
        with open('/Users/dpong/Documents/Personal/Trading_Python_Code/Data_storage/sp500tickers.pickle','rb') as f: 
            self.tickers = pickle.load(f)
    
    def drop_duplicate_rows(self):
        #理論上不會發生，但寫著預備萬一
        for ticker in self.tickers:
            if not ticker in self.ticker_excluded: 
                self.cursor.execute('delete from US_{} where rowid not in (select min(rowid) from US_{} group by Date);'.format(ticker,ticker))
        print('Done!')
        #清理所有重複的日期 
    
    def update_sqlite(self):
        
        today = date.today()
        df = pdr.DataReader('AAPL','yahoo',start=today) #測一下最新資料的日期
        data_date = str(df.index[0])
        data_date=datetime.strptime(data_date, "%Y-%m-%d %H:%M:%S")
        data_date = datetime.date(data_date)
        
        for last_date in self.cursor.execute('select Date from US_AAPL order by Date desc limit 1;'): 
            pass      
        last_end = datetime.strptime(last_date[0], "%Y-%m-%d %H:%M:%S")
        last_end = datetime.date(last_end)
        
        if not last_end == data_date:
            print('Last update is on: {}, start updating...'.format(last_end))
            new_start = last_end + timedelta(days = 1)
            new_start = str(new_start)
            for ticker in self.tickers:
                if not ticker in self.ticker_excluded: 
                    try:
                        df = pdr.DataReader('{}'.format(ticker),'yahoo',start=new_start)
                        df.rename(columns={'Adj Close':'Adj_Close'},inplace=True) #避免sqlite的警告
                        df.to_sql('US_{}'.format(ticker),con=self.conn,if_exists='append')
                        print('{} is updated!'.format(ticker))
                    except:
                        pass
            self.conn.commit()
        else:
            print('Already up to date!')  
            
            
    def to_sqlite(self):
        
        for table_num in self.cursor.execute('SELECT count(*) FROM sqlite_master WHERE type="table";'):
            print(table_num[0])
        if table_num[0] == 0:
            for ticker in self.tickers:
                try:
                    df = pdr.DataReader('{}'.format(ticker),'yahoo')
                    df.rename(columns={'Adj Close':'Adj_Close'},inplace=True)
                    df.to_sql('US_{}'.format(ticker),con=self.conn,if_exists='replace')
                    print('{} is completed!'.format(ticker))
                except:
                    self.ticker_excluded.append(ticker)
                    print('{} is excluded!'.format(ticker))
            self.conn.commit()
         
            with open('/Users/dpong/Documents/Personal/Trading_Python_Code/Data_storage/Price_Data/{}_Exclude_list.pickle'.format(self.dataset),'wb') as f:
                pickle.dump(self.ticker_excluded, f)
                
        
