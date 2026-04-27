import pyodbc
import pandas as pd
import mplfinance as mpf

# 1. & 2. 設定資料庫連接並利用 SQL 語法讀取數據
# 請根據你的資料庫實際情況修改連線字串 (ConnectionString)
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=ncu_database;'
    'UID=a159753de;'
    'PWD=g5982734de;'
)

sql_query = """
SELECT 
    date, 
    [Open], 
    High, 
    Low, 
    [Close], 
    Transcation AS Volume 
FROM StockTrading_TA 
WHERE StockCode = '2330' 
  AND date >= '2025-01-01' 
  AND date <= '2025-12-31'
"""

# 建立連線並讀取資料進入 pandas DataFrame
try:
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(sql_query, conn)
    conn.close()
    print("資料讀取成功！")
except Exception as e:
    print(f"資料庫連線或讀取失敗: {e}")

# 若讀取成功，開始執行後續處理
if 'df' in locals():
    # 3. 將讀進的數據欄位賦予正確格式名稱 (已在 SQL 中將 Transcation 命名為 Volume)
    # 若 SQL 沒改名，可以使用：df.rename(columns={'Transcation': 'Volume'}, inplace=True)

    # 4. 將 'date' 列轉為 datetime 類型
    df['date'] = pd.to_datetime(df['date'])

    # 5. 將 dataframe 的 date 欄位設定為索引
    df.set_index('date', inplace=True)

    # 5. (重複編號項) 改變顯示顏色 (上漲為紅 red, 下跌為綠 green)
    # 在台灣股市慣例中，紅漲綠跌
    mc = mpf.make_marketcolors(
        up='r',          # 上漲為紅
        down='g',        # 下跌為綠
        edge='inherit',  # 邊框顏色繼承
        wick='inherit',  # 影線顏色繼承
        volume='inherit' # 成交量顏色繼承
    )
    
    # 建立自定義風格
    s = mpf.make_mpf_style(marketcolors=mc)

    # 4. (重複編號項) 繪製 K 線蠟燭圖
    # type='candle' 為蠟燭圖, volume=True 顯示成交量
    mpf.plot(df, type='candle', style=s, title='TSMC (2330) 2025 Full Year', volume=True)