import sqlite3
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import os

# ===================== 配置 =====================
# 上交所9只沪深300ETF
TARGET_ETF = {
    "510300": "沪深300ETF华泰柏瑞",
    "510310": "沪深300ETF易方达",
    "510320": "沪深300ETF中金",
    "510330": "沪深300ETF华夏",
    "510350": "沪深300ETF工银",
    "510360": "沪深300ETF广发",
    "510370": "沪深300ETF兴业",
    "510380": "沪深300ETF国寿安保",
    "510390": "沪深300ETF平安"
}

# 上交所官方数据URL
SSE_ETF_URL = "https://www.sse.com.cn/market/funddata/volumn/etfvolumn/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.sse.com.cn/",
    "Accept-Language": "zh-CN,zh;q=0.9"
}

# 数据库和数据文件路径
DB_PATH = "crawler/etf_data.db"
JSON_OUTPUT_PATH = "data/etf_share_data.json"

# 初始爬取起始日期
FIRST_START_DATE = datetime(2024, 9, 1)

# ===================== 数据库初始化 =====================
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 创建表，主键是etf_code和trade_date，避免重复
    c.execute('''
        CREATE TABLE IF NOT EXISTS etf_share (
            etf_code TEXT,
            trade_date TEXT,
            share REAL,
            PRIMARY KEY (etf_code, trade_date)
        )
    ''')
    conn.commit()
    return conn

# ===================== 获取最新已爬取日期 =====================
def get_latest_date(conn):
    c = conn.cursor()
    c.execute('SELECT MAX(trade_date) FROM etf_share')
    latest = c.fetchone()[0]
    if latest:
        return datetime.strptime(latest, "%Y-%m-%d")
    else:
        return FIRST_START_DATE

# ===================== 爬取单个日期数据 =====================
def fetch_day_data(trade_date):
    date_str = trade_date.strftime("%Y-%m-%d")
    params = {"date": date_str}
    try:
        resp = requests.get(SSE_ETF_URL, params=params, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            return []
        
        data = []
        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = [col.text.strip() for col in row.find_all("td")]
            if len(cols) >=4 and cols[1] in TARGET_ETF.keys():
                share = float(cols[3].replace(",", ""))
                data.append((cols[1], date_str, share))
        return data
    except Exception as e:
        print(f"爬取 {date_str} 失败: {e}")
        return []

# ===================== 导出数据到JSON =====================
def export_to_json(conn):
    os.makedirs(os.path.dirname(JSON_OUTPUT_PATH), exist_ok=True)
    c = conn.cursor()
    # 读取所有数据
    c.execute('SELECT etf_code, trade_date, share FROM etf_share ORDER BY trade_date')
    rows = c.fetchall()
    
    etf_data = {}
    for code, name in TARGET_ETF.items():
        etf_data[code] = {
            "name": name,
            "data": []
        }
    
    for code, date, share in rows:
        etf_data[code]["data"].append({
            "date": date,
            "share": share
        })
    
    # 最后更新时间
    last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    output = {
        "etfs": etf_data,
        "last_update": last_update
    }
    
    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"数据已导出到 {JSON_OUTPUT_PATH}")

# ===================== 主函数 =====================
def main():
    print("===== 开始运行爬虫 =====")
    conn = init_db()
    
    # 获取要爬取的日期范围
    latest_date = get_latest_date(conn)
    today = datetime.now()
    # 今天如果还没到18点，就爬昨天的？不对，上交所的数据是收盘后更新，所以18点的时候，当天的数据已经有了
    start_date = latest_date + timedelta(days=1)
    end_date = today
    
    if start_date > end_date:
        print("没有新数据需要爬取")
        conn.close()
        return
    
    print(f"需要爬取的日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
    
    # 遍历日期爬取
    current = start_date
    while current <= end_date:
        day_data = fetch_day_data(current)
        if day_data:
            # 插入到数据库
            c = conn.cursor()
            c.executemany('INSERT OR IGNORE INTO etf_share VALUES (?, ?, ?)', day_data)
            conn.commit()
            print(f"成功插入 {current.strftime('%Y-%m-%d')} 的 {len(day_data)} 条数据")
        current += timedelta(days=1)
    
    # 导出JSON
    export_to_json(conn)
    
    conn.close()
    print("===== 爬虫运行完成 =====")

if __name__ == "__main__":
    main()
