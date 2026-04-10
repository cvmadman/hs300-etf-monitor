import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import pandas as pd

# 9只沪深300ETF代码
ETF_CODES = [
    '510300', '510310', '510330', '510360',
    '510350', '510390', '510320', '510380', '510370'
]

# 数据存储文件
DATA_FILE = 'etf_data.json'
# 历史数据起始日期（改为2024-09-01）
START_DATE = '2024-09-01'

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

def fetch_etf_share_by_date(code, date_str):
    """从上海证券交易所官网抓取单只ETF指定日期的份额"""
    url = f'https://www.sse.com.cn/assortment/fund/list/etfinfo/basic/index.shtml?FUNDID={code}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        res = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        # 找到基金规模表格
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    row_date = cols[0].text.strip()
                    if row_date == date_str:
                        try:
                            share = float(cols[3].text.strip()) / 10000  # 万份转亿份
                            return share
                        except:
                            pass
        return None
    except Exception as e:
        print(f'抓取 {code} {date_str} 失败: {e}')
        return None

def get_trading_days(start_date, end_date):
    """生成交易日列表（自动跳过周末）"""
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 周一到周五
            days.append(current.strftime('%Y-%m-%d'))
        current += datetime.timedelta(days=1)
    return days

def update_all_data():
    """全量更新2024-09-01以来的所有数据"""
    data = load_existing_data()
    today = datetime.date.today()
    end_date = today.strftime('%Y-%m-%d')
    
    # 获取所有需要处理的交易日
    all_days = get_trading_days(START_DATE, end_date)
    # 过滤掉已经处理过的日期
    new_days = [d for d in all_days if d not in data['dates']]
    
    if not new_days:
        print('所有数据已是最新，无需更新')
        return
    
    print(f'开始更新 {len(new_days)} 个交易日的数据...')
    
    # 按日期升序处理
    for date_str in new_days:
        print(f'处理 {date_str}...')
        # 抓取9只ETF的份额
        shares = {}
        total = 0
        for code in ETF_CODES:
            share = fetch_etf_share_by_date(code, date_str)
            if share:
                shares[code] = share
                total += share
                print(f'  {code}: {share:.2f} 亿份')
            time.sleep(0.5)  # 避免请求过快
        
        if total > 0:
            # 更新数据
            data['dates'].append(date_str)
            data['total'].append(total)
            for code in ETF_CODES:
                if code in shares:
                    data[code].append(shares[code])
                else:
                    # 缺失的话补前一天的
                    if data[code]:
                        data[code].append(data[code][-1])
                    else:
                        data[code].append(0)
        
        # 保存中间结果，避免断了重爬
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    # 最终排序，保证日期是升序的
    # 把数据转成DataFrame排序
    df = pd.DataFrame({
        'date': data['dates'],
        'total': data['total'],
        '510300': data['510300'],
        '510310': data['510310'],
        '510330': data['510330'],
        '510360': data['510360'],
        '510350': data['510350'],
        '510390': data['510390'],
        '510320': data['510320'],
        '510380': data['510380'],
        '510370': data['510370'],
    })
    df = df.sort_values('date', ascending=True).reset_index(drop=True)
    
    # 转回原来的结构
    data['dates'] = df['date'].tolist()
    data['total'] = df['total'].tolist()
    for code in ETF_CODES:
        data[code] = df[code].tolist()
    
    # 保存最终结果
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f'更新完成！共 {len(data["dates"])} 条记录，最新日期: {data["dates"][-1]}')

if __name__ == '__main__':
    update_all_data()
