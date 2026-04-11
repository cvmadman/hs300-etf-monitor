import requests
from bs4 import BeautifulSoup
import sqlite3
import json
import time
import os
from datetime import datetime, timedelta
import re
import schedule

class ETFScraper:
    def __init__(self):
        self.db_path = 'etf_data.db'
        self.url = 'https://www.sse.com.cn/market/funddata/volumn/etfvolumn/'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.create_database()

    def create_database(self):
        """创建SQLite数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建ETF份额表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etf_shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                fund_name TEXT NOT NULL,
                trade_date DATE NOT NULL,
                shares REAL NOT NULL,
                UNIQUE(fund_code, trade_date)
            )
        ''')
        
        # 创建爬取日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scrape_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def parse_etf_data(self, html_content):
        """解析ETF数据"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 查找包含ETF数据的表格
        # 根据实际页面结构调整选择器
        tables = soup.find_all('table')
        
        etf_data = []
        
        for table in tables:
            # 查找表头
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            
            # 如果表头包含我们需要的字段，则处理该表格
            if any('基金代码' in h or '证券代码' in h for h in headers) and \
               any('基金简称' in h or '证券简称' in h for h in headers) and \
               any('份额' in h or '股份数' in h for h in headers):
                
                rows = table.find_all('tr')[1:]  # 跳过表头
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        # 提取基金代码、基金名称和份额
                        fund_code = cells[0].get_text(strip=True)
                        fund_name = cells[1].get_text(strip=True)
                        shares_str = cells[2].get_text(strip=True)
                        
                        # 清理份额数据
                        shares_clean = re.sub(r'[^\d.]', '', shares_str)
                        if shares_clean:
                            shares = float(shares_clean)  # 假设单位为万份
                            etf_data.append({
                                'fund_code': fund_code,
                                'fund_name': fund_name,
                                'shares': shares
                            })
                break
        
        # 如果没找到表格，尝试查找JSON数据
        if not etf_data:
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and ('etf' in script.string.lower() or '份额' in script.string):
                    # 尝试提取JSON数据
                    json_match = re.search(r'(\{.*\}|\[.*\])', script.string, re.DOTALL)
                    if json_match:
                        try:
                            data = json.loads(json_match.group())
                            if isinstance(data, list):
                                for item in data:
                                    if 'fundCode' in item and 'fundName' in item and 'shares' in item:
                                        etf_data.append({
                                            'fund_code': item['fundCode'],
                                            'fund_name': item['fundName'],
                                            'shares': item['shares']
                                        })
                        except:
                            pass
        
        return etf_data

    def insert_data_to_db(self, etf_data, trade_date):
        """将ETF数据插入数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted_count = 0
        for item in etf_data:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO etf_shares 
                    (fund_code, fund_name, trade_date, shares) 
                    VALUES (?, ?, ?, ?)
                ''', (item['fund_code'], item['fund_name'], trade_date, item['shares']))
                inserted_count += 1
            except Exception as e:
                print(f"插入数据失败: {item}, 错误: {e}")
        
        # 记录爬取日志
        cursor.execute('''
            INSERT INTO scrape_log (trade_date, status, message) 
            VALUES (?, ?, ?)
        ''', (trade_date, 'SUCCESS', f'成功插入{inserted_count}条数据'))
        
        conn.commit()
        conn.close()
        return inserted_count

    def scrape_single_day(self, date_str):
        """爬取指定日期的ETF数据"""
        try:
            print(f"正在爬取 {date_str} 的数据...")
            
            # 构造带日期的URL（如果网站支持）
            formatted_date = date_str.replace('-', '')
            url_with_date = f"https://www.sse.com.cn/market/funddata/volumn/etfvolumn/{formatted_date}/"
            
            # 首先尝试带日期的URL
            response = None
            try:
                response = requests.get(url_with_date, headers=self.headers, timeout=10)
            except:
                # 如果带日期的URL不可用，使用基础URL
                response = requests.get(self.url, headers=self.headers, timeout=10)
            
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                print(f"警告: 无法获取 {date_str} 的数据，状态码: {response.status_code}")
                # 记录失败日志
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO scrape_log (trade_date, status, message) 
                    VALUES (?, ?, ?)
                ''', (date_str, 'FAILED', f'HTTP {response.status_code}'))
                conn.commit()
                conn.close()
                return False
            
            etf_data = self.parse_etf_data(response.text)
            
            if etf_data:
                count = self.insert_data_to_db(etf_data, date_str)
                print(f"成功获取 {date_str} 的数据，插入 {count} 条记录")
                return True
            else:
                print(f"警告: {date_str} 没有找到ETF数据")
                # 记录失败日志
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO scrape_log (trade_date, status, message) 
                    VALUES (?, ?, ?)
                ''', (date_str, 'NO_DATA', '未找到ETF数据'))
                conn.commit()
                conn.close()
                return False
                
        except Exception as e:
            print(f"爬取 {date_str} 数据时出错: {str(e)}")
            # 记录失败日志
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO scrape_log (trade_date, status, message) 
                VALUES (?, ?, ?)
            ''', (date_str, 'ERROR', str(e)))
            conn.commit()
            conn.close()
            return False

    def get_trading_days(self, start_date, end_date):
        """获取指定日期范围内的交易日（排除周末）"""
        trading_days = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            weekday = current_date.weekday()
            if weekday < 5:  # 0-4 代表周一到周五
                trading_days.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        return trading_days

    def get_already_scraped_dates(self):
        """获取数据库中已存在的交易日期"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT trade_date FROM etf_shares ORDER BY trade_date')
        dates = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return set(dates)

    def full_scrape_history(self, start_date='2024-09-01'):
        """全量爬取历史数据"""
        print(f"开始全量爬取从 {start_date} 至今的历史数据...")
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        all_trading_days = self.get_trading_days(start_date, end_date)
        already_scraped = self.get_already_scraped_dates()
        
        days_to_scrape = [day for day in all_trading_days if day not in already_scraped]
        
        print(f"总共需要爬取 {len(days_to_scrape)} 个交易日的数据")
        print(f"已存在 {len(already_scraped)} 个交易日的数据")
        
        success_count = 0
        for i, date in enumerate(days_to_scrape):
            print(f"[{i+1}/{len(days_to_scrape)}] 正在处理: {date}")
            if self.scrape_single_day(date):
                success_count += 1
            # 添加延时，避免请求过于频繁
            time.sleep(2)
        
        print(f"全量爬取完成！成功获取 {success_count}/{len(days_to_scrape)} 天的数据")

    def daily_update(self):
        """每日更新最新数据"""
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"开始更新今日数据: {today}")
        
        # 检查今天是否已经爬取过
        already_scraped = self.get_already_scraped_dates()
        if today in already_scraped:
            print(f"{today} 的数据已存在，跳过爬取")
            return
        
        # 爬取今日数据
        self.scrape_single_day(today)

    def run_scheduler(self):
        """启动定时任务"""
        # 每天18:00执行数据更新
        schedule.every().day.at("18:00").do(self.daily_update)
        
        print("定时任务已启动，每天18:00自动更新数据...")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

# 主程序入口
if __name__ == "__main__":
    scraper = ETFScraper()
    
    # 提供命令行选项
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'full':
            # 全量爬取历史数据
            start_date = sys.argv[2] if len(sys.argv) > 2 else '2024-09-01'
            scraper.full_scrape_history(start_date)
        elif sys.argv[1] == 'daily':
            # 只爬取今天的数据
            scraper.daily_update()
        elif sys.argv[1] == 'schedule':
            # 启动定时任务
            scraper.run_scheduler()
        else:
            print("用法:")
            print("  python script.py full [start_date]  # 全量爬取历史数据，默认从2024-09-01开始")
            print("  python script.py daily              # 爬取今天的数据")
            print("  python script.py schedule           # 启动定时任务")
    else:
        print("默认执行全量爬取历史数据...")
        scraper.full_scrape_history('2024-09-01')
