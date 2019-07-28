# Price_Data_Storage

用pandas datareader 抓股價資料並存到 sqlite，目的是建立一個歷史資料的本機資料庫，從 sqlite 存取資料都可以用 pandas 完成，速度比每次都跑一次 datareader 快了不少，再寫個排程就能自動更新資料庫了。

程式碼是以 S&P 列表來蒐集資料，換成台股的代碼一樣可行，自行在 get_ticker function 內改寫就好。

起始：
c = Collecting_data()

取得代碼 list：
c.get_tickers()

資料寫入資料庫：
c.to_sqlite()

資料庫資料更新：
c.update_sqlite()

資料庫清理重複日期的rows:(理論上用不到，備用。)
c.drop_duplicate_rows()

結束：
c.close_connection()


