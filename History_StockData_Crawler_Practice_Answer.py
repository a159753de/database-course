import requests
import pymssql
import time
import urllib3
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor

# 1. 環境設定，忽略SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

db_settings = {
    "host": "127.0.0.1",
    "user": "",
    "password": "",
    "database": "",
    "charset": "utf8"
}

stock_list = [
    {"stock_code": "2330", "stock_name": "台積電"},
    {"stock_code": "2317", "stock_name": "鴻海"},
    {"stock_code": "2454", "stock_name": "聯發科"},
    {"stock_code": "2308", "stock_name": "台達電"},
    {"stock_code": "3711", "stock_name": "日月光投控"},
    {"stock_code": "2891", "stock_name": "中信金"},
    {"stock_code": "2382", "stock_name": "廣達"},
    {"stock_code": "2881", "stock_name": "富邦金"},
    {"stock_code": "2345", "stock_name": "智邦"},
    {"stock_code": "2303", "stock_name": "聯電"},
]

# 工具：數列處理
def safe_float(value):
    try:
        if isinstance(value, str):
            value = value.replace(',', '').replace('X', '').replace('+', '').strip()
            if value in ['--', 'None', '', '-']: return 0.0
        return float(value)
    except: return 0.0

def crawl_stock_task(task):
    code, name, date_str = task['code'], task['name'], task['date']
    
    # 每個thread使用獨立的 Session 以提升效能
    session = requests.Session()
    session.verify = False
    
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date_str}&stockNo={code}"
    
    # --- 第一階段：獲取資料 ---
    data = None
    for attempt in range(5):
        try:
            time.sleep(random.uniform(2, 6)) # 安全等待
            response = session.get(url, timeout=15)
            temp_data = response.json()
            if temp_data.get("stat") == "OK":
                data = temp_data
                break
            elif "查詢日期小於99年1月4日" in temp_data.get("stat", ""): # API retrun的錯誤訊息(但)
                time.sleep((attempt + 1) * 30)
            else: break
        except: time.sleep(10)
    
    if not data or "data" not in data: return

    # --- 第二階段：整理資料庫批量格式 ---
    batch_data = []
    for row in data["data"]:
        roc_year, m, d = row[0].split('/')
        db_date = f"{int(roc_year) + 1911}-{m}-{d}"
        
        # 整理成符合 SQL 欄位順序的 tuple
        batch_data.append((
            code, db_date, "13:30:00.0000000", # 歷史 API 沒給時間，我們手動定義收盤時間並補齊 7 位小數
            int(row[1].replace(',', '')), safe_float(row[2]),
            safe_float(row[3]), safe_float(row[4]), safe_float(row[5]),
            safe_float(row[6]), safe_float(row[7]), int(row[8].replace(',', ''))
        ))

    # --- 第三階段：批量寫入資料庫 ---
    for db_attempt in range(3):
        try:
            conn = pymssql.connect(**db_settings)
            cursor = conn.cursor()

            # 1. 建立臨時表 (只存在於此連線中，速度極快)
            cursor.execute("""
                CREATE TABLE #TempStock (
                    stock_code VARCHAR(10), [date] DATE, [time] VARCHAR(20),
                    tv BIGINT, t FLOAT, o FLOAT, h FLOAT, l FLOAT, c FLOAT, d FLOAT, v INT
                )
            """)

            # 2. 使用 executemany 批量塞入臨時表，減少與SQL Server的溝通次數
            insert_temp_sql = "INSERT INTO #TempStock VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(insert_temp_sql, batch_data)

            # 3. 使用 SQL 的集合運算，一次性將「不存在」的資料搬移到正式表
            upsert_sql = """
                INSERT INTO [dbo].[stock_info] (stock_code, [date], [time], tv, t, o, h, l, c, d, v)
                SELECT t.stock_code, t.[date], t.[time], t.tv, t.t, t.o, t.h, t.l, t.c, t.d, t.v
                FROM #TempStock t
                WHERE NOT EXISTS (
                    SELECT 1 FROM [dbo].[stock_info] s 
                    WHERE s.stock_code = t.stock_code AND s.[date] = t.[date]
                )
            """
            cursor.execute(upsert_sql)
            
            conn.commit()
            conn.close()
            print(f"🚀 {name} ({date_str[:6]}) 批量寫入完成 ({len(batch_data)} 筆)")
            break
        except Exception as e:
            if '1205' in str(e): # deadlock重試
                time.sleep(random.uniform(2, 5))
            else:
                print(f"❌ {code} DB Error: {e}")
                break

def main():
    all_tasks = []
    start_date = datetime(2023, 1, 1) #從20230101開始爬，直到現在
    end_date = datetime.now()

    for stock in stock_list:
        curr = start_date
        while curr <= end_date:
            all_tasks.append({'code': stock['stock_code'], 'name': stock['stock_name'], 'date': curr.strftime("%Y%m%d")})
            curr += relativedelta(months=1)

    # max_workers 可以試著自己改改看，過多可能會被 API Ban 掉，過少則無法充分利用資源
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(crawl_stock_task, all_tasks)

if __name__ == "__main__":
    start_t = time.time()
    main()
    print(f"✨ 任務全部結束！總計耗時: {(time.time()-start_t)/60:.2f} 分鐘")