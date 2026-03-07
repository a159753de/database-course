from datetime import datetime, timedelta
import pymssql
from apscheduler.schedulers.blocking import BlockingScheduler
import requests

# MSSQL 設定
db_settings = {
    "host": "127.0.0.1",
    "user": "",
    "password": "",
    "database": "",
    "charset": "utf8"
}

# 建立排程器
scheduler = BlockingScheduler(timezone='Asia/Taipei')

# 記錄上次插入的股票數據
last_record = {}

def fetch_stock_data(stock_type, stock_code):
    """ 從 API 抓取指定股票的數據 """
    base_url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={stock_type}_{stock_code}.tw&json=1&delay=0"
    url = base_url.format(stock_type=stock_type, stock_code=stock_code)

    try:
        response = requests.get(url, verify=False)
        data = response.json()

        if "msgArray" in data and len(data["msgArray"]) > 0:
            return data["msgArray"][0]  # 取得股票數據
    except Exception as e:
        print(f"❌ 無法獲取 {stock_code} 的數據: {e}")
    
    return None

def parse_stock_data(stock_data):
    """ 解析 API 數據，並確保所有 `REAL` 類型數據為 float """
    def safe_float(value):
        """ 將字串轉換為浮點數，若為 '-' 則回傳 0.0 """
        try:
            return float(value.replace(',', '').replace('-', '0'))
        except ValueError:
            return 0.0

    latest_price = safe_float(stock_data.get("z", "0"))  # 最新成交價
    prev_close_price = safe_float(stock_data.get("y", "0"))  # 昨日收盤價
    price_change = latest_price - prev_close_price if latest_price != 0 else 0.0  # 只在有數據時計算

    return {
        "trade_time": stock_data.get("t", "00:00:00"),  # 交易時間
        "trade_volume": int(stock_data.get("tv", "0").replace("-", "0")),  # 成交股數
        "latest_price": latest_price,  # 最新成交價
        "high_price": safe_float(stock_data.get("h", "0")),  # 最高價
        "low_price": safe_float(stock_data.get("l", "0")),  # 最低價
        "open_price": safe_float(stock_data.get("o", "0")),  # 開盤價
        "price_change": price_change,  # 漲跌價差
        "trade_value": 0,  # 成交金額 (API 無提供，手動設為 0)
        "trade_count": 0  # 成交筆數 (API 無提供，手動設為 0)
    }


def daily_crawler():
    """ 每次偵測股票變動，只有數據變動時才寫入資料庫 """
    stock_list = [
        # TODO : 練習3
        
    ]

    conn = pymssql.connect(**db_settings)
    cursor = conn.cursor()

    today = datetime.today().strftime('%Y-%m-%d')

    for stock in stock_list:
        stock_code = stock["stock_code"]
        stock_name = stock["stock_name"]
        stock_type = stock["stock_type"]

        # 取得 API 數據
        stock_data = fetch_stock_data(stock_type, stock_code)
        if not stock_data:
            print(f"⚠️ {stock_name}({stock_code}) 無法獲取數據，跳過")
            continue

        # 解析數據
        parsed_data = parse_stock_data(stock_data)

        # 構造新數據
        new_record = (
            stock_code, today, parsed_data["trade_time"], parsed_data["trade_volume"],
            parsed_data["trade_value"], parsed_data["open_price"], parsed_data["high_price"],
            parsed_data["low_price"], parsed_data["latest_price"], parsed_data["price_change"],
            parsed_data["trade_count"]
        )

        # 檢查是否與上次相同
        prev_record = last_record.get(stock_code, None)
        if prev_record and prev_record == new_record:
            print(f"🔄 {stock_name}({stock_code}) 數據未變動，跳過")
            continue  # 如果沒有變化，跳過寫入資料庫

        # 更新記錄
        last_record[stock_code] = new_record

        # **將資料寫入資料庫**
        # TODO : 練習3
        
    conn.close()

# 讓 Schedular 在設定的時間可以正常關閉
def end_program():
    print("Program ends.")
    scheduler.shutdown(wait=False)  # 直接關閉 Scheduler

# 檢查當天是否為交易日
conn = pymssql.connect(**db_settings)
with conn.cursor() as cursor:
    today = datetime.today().strftime('%Y%m%d')
    command = f"SELECT * FROM [dbo].[calendar] WHERE date = '{today}'"
    cursor.execute(command)
    result = cursor.fetchone()
conn.commit()
conn.close()

# 如果當天不休市，則開始排程
if result and result[1] != -1:
    # 每 10 秒執行一次 daily_crawler，但只有數據變化時才寫入資料庫
    scheduler.add_job(daily_crawler, 'interval', seconds=10)

    # 設定在 1 分鐘後關閉程式
    run_time = datetime.now() + timedelta(minutes=1)
    scheduler.add_job(end_program, 'date', run_date=run_time)

    try:
        # 啟動排程
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()
        print("Program stopped by user.")
else:
    print("今天休市，程式結束。")