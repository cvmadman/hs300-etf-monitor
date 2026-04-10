import requests
from bs4 import BeautifulSoup
import json
import datetime
import time

# 9只沪深300ETF代码
ETF_CODES = [
    '510300', '510310', '510330', '510360',
    '510350', '510390', '510320', '510380', '510370'
]

# 数据存储文件
DATA_FILE = 'etf_data.json'

def load_existing_data():
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {
            'dates': [],
            'total': [],
            '510300': [], '510310': [], '510330': [], '510360': [],
            '510350': [], '510390': [], '510320': [], '510380': [], '510370': []
        }

def fetch_etf_share(code, date):
    """从上海证券交易所官网抓取单只ETF的当日份额"""
    url = f'https://www.sse.com.cn/assortment/fund/list/etfinfo/basic/index.shtml?FUNDID={code}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        # 找到基金规模表格
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    row_date = cols[0].text.strip()
                    if row_date == date:
                        try:
                            share = float(cols[3].text.strip()) / 10000  # 万份转亿份
                            return share
                        except:
                            pass
        return None
    except Exception as e:
        print(f'抓取 {code} {date} 失败: {e}')
        return None

def update_daily_data():
    """每日更新最新数据"""
    data = load_existing_data()
    today = datetime.date.today()
    # 只处理交易日
    if today.weekday() >= 5:  # 周末跳过
        print('周末休市，跳过更新')
        return
    
    date_str = today.strftime('%Y-%m-%d')
    if date_str in data['dates']:
        print('今日数据已更新，跳过')
        return

    print(f'开始更新 {date_str} 的数据...')
    
    # 抓取9只ETF的份额
    shares = {}
    total = 0
    for code in ETF_CODES:
        share = fetch_etf_share(code, date_str)
        if share:
            shares[code] = share
            total += share
            print(f'{code}: {share:.2f} 亿份')
        time.sleep(1)  # 避免请求过快
    
    if total > 0:
        # 更新数据
        data['dates'].append(date_str)
        data['total'].append(total)
        for code in ETF_CODES:
            if code in shares:
                data[code].append(shares[code])
        
        # 保存到文件
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f'更新完成！合计: {total:.2f} 亿份')
    else:
        print('未获取到有效数据')

if __name__ == '__main__':
    update_daily_data()
