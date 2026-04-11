import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import sqlite3
import os
import logging
import shutil

# -------------------------- 全局配置项（无需修改） --------------------------
ETF_CODES = [
    '510300', '510310', '510330', '510360',
    '510350', '510390', '510320', '510380', '510370'
]
DB_FILE = 'etf.db'
DB_BACKUP_FILE = 'etf_backup.db'
JSON_FILE = 'etf_data.json'
START_DATE = '2024-09-01'
MAX_RETRY = 3
MAX_DAILY_RETRY = 2
REQUEST_INTERVAL = 0.8
DAILY_RETRY_INTERVAL = 5

# -------------------------- 日志初始化 --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# -------------------------- 数据库核心操作 --------------------------
def backup_database():
    if os.path.exists(DB_FILE):
        try:
            shutil.copy2(DB_FILE, DB_BACKUP_FILE)
            logger.info('数据库备份完成')
        except Exception as e:
            logger.warning(f'数据库备份失败: {e}，继续运行主流程')

def init_database():
    try:
        etf_fields = ', '.join([f'"{code}" REAL NOT NULL DEFAULT 0' for code in ETF_CODES])
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS etf_share (
            "date" TEXT PRIMARY KEY NOT NULL,
            "total" REAL NOT NULL DEFAULT 0,
            {etf_fields}
        )
        '''
        cursor.execute(create_table_sql)
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS crawl_progress (
            "date" TEXT PRIMARY KEY NOT NULL,
            "is_processed" INTEGER NOT NULL DEFAULT 0,
            "process_time" TEXT NOT NULL
        )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON etf_share("date")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed ON crawl_progress("is_processed")')
        conn.commit()
        conn.close()
        logger.info(f'数据库初始化完成，文件: {DB_FILE}')
    except Exception as e:
        logger.error(f'数据库初始化失败: {e}')
        if os.path.exists(DB_BACKUP_FILE):
            try:
                shutil.copy2(DB_BACKUP_FILE, DB_FILE)
                logger.info('已从备份文件恢复数据库')
                init_database()
            except Exception as restore_e:
                logger.error(f'数据库恢复失败: {restore_e}')
                exit(1)
        else:
            exit(1)

def get_earliest_processed_date():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT MIN("date") FROM crawl_progress WHERE "is_processed" = 1')
        earliest_date = cursor.fetchone()[0]
        conn.close()
        return earliest_date if earliest_date else (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f'获取最早处理日期失败: {e}')
        return (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

def check_date_is_processed(date_str: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM crawl_progress WHERE "date" = ? AND "is_processed" = 1', (date_str,))
    is_processed = cursor.fetchone() is not None
    conn.close()
    return is_processed

def mark_date_processed(date_str: str):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        process_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
        INSERT OR REPLACE INTO crawl_progress ("date", "is_processed", "process_time")
        VALUES (?, 1, ?)
        ''', (date_str, process_time))
        conn.commit()
        conn.close()
        logger.info(f'{date_str} 已标记为完整处理，断点已更新')
    except Exception as e:
        logger.error(f'标记日期处理状态失败: {e}')

def insert_data_to_db(date_str: str, share_data: dict):
    if not date_str:
        logger.error(f'{date_str} 日期为空，终止入库')
        return False
    
    for code in ETF_CODES:
        if share_data.get(code, 0) <= 0:
            logger.error(f'{date_str} {code} 无有效份额数据，终止入库')
            return False
    
    calc_total = sum([share_data[code] for code in ETF_CODES])
    if abs(share_data['total'] - calc_total) > 0.01:
        logger.error(f'{date_str} 总份额校验不匹配，终止入库')
        return False
    
    try:
        columns = ['"date"', '"total"'] + [f'"{code}"' for code in ETF_CODES]
        placeholders = [f':{col}' for col in ['date', 'total'] + ETF_CODES]
        insert_sql = f'''
        INSERT OR REPLACE INTO etf_share ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        '''
        
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(insert_sql, share_data)
        conn.commit()
        conn.close()

        logger.info('='*50)
        logger.info(f'✅ 数据库插入成功 | 日期：{date_str}')
        logger.info(f'📊 合计总份额：{share_data["total"]:.4f} 亿份')
        for code in ETF_CODES:
            logger.info(f'  {code}：{share_data[code]:.4f} 亿份')
        logger.info('='*50 + '\n')

        return True
    except Exception as e:
        logger.error(f'{date_str} 数据入库失败: {e}')
        return False

def get_all_data_from_db():
    try:
        etf_fields = ', '.join([f'"{code}"' for code in ETF_CODES])
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f'SELECT "date", "total", {etf_fields} FROM etf_share ORDER BY "date" ASC')
        all_rows = cursor.fetchall()
        conn.close()
        
        result = {
            'dates': [],
            'total': [],
        }
        for code in ETF_CODES:
            result[code] = []
        
        for row in all_rows:
            result['dates'].append(row[0])
            result['total'].append(row[1])
            for idx, code in enumerate(ETF_CODES):
                result[code].append(row[idx+2])
        
        logger.info(f'从数据库读取全量数据完成，共 {len(result["dates"])} 条完整记录')
        return result
    except Exception as e:
        logger.error(f'读取全量数据失败: {e}')
        return {}

def import_existing_json_to_db():
    if not os.path.exists(JSON_FILE):
        logger.info('无现有JSON文件，跳过导入步骤')
        return
    
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        if 'dates' not in json_data or 'total' not in json_data:
            logger.warning('JSON格式不符合要求，跳过导入')
            return
        
        import_count = 0
        for idx, date_str in enumerate(json_data['dates']):
            if check_date_is_processed(date_str):
                continue
            
            share_data = {
                'date': date_str,
                'total': json_data['total'][idx]
            }
            data_complete = True
            for code in ETF_CODES:
                code_data = json_data.get(code, [])
                if idx >= len(code_data) or code_data[idx] <= 0:
                    data_complete = False
                    break
                share_data[code] = code_data[idx]
            
            if not data_complete:
                logger.warning(f'{date_str} 数据不完整，跳过导入')
                continue
            
            if insert_data_to_db(date_str, share_data):
                mark_date_processed(date_str)
                import_count += 1
        
        if import_count > 0:
            logger.info(f'成功导入现有JSON数据，新增 {import_count} 条完整历史记录')
        else:
            logger.info('JSON数据已全部导入完成，无新增完整记录')
    except Exception as e:
        logger.error(f'导入JSON数据失败: {e}')

# -------------------------- 爬虫核心操作 --------------------------
def fetch_etf_share_by_date(code: str, date_str: str) -> float | None:
    url = f'https://www.sse.com.cn/assortment/fund/list/etfinfo/basic/index.shtml?FUNDID={code}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://www.sse.com.cn/',
        'Connection': 'keep-alive'
    }

    for retry in range(MAX_RETRY):
        try:
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'lxml')
            
            target_table = None
            tables = soup.find_all('table')
            for table in tables:
                table_text = table.get_text()
                if '基金规模' in table_text and '日期' in table_text and '基金总份额' in table_text:
                    target_table = table
                    break
            
            if not target_table:
                target_table = tables[0] if tables else None
            
            if not target_table:
                logger.warning(f'{code} {date_str} 未找到数据表格，单只重试次数: {retry+1}')
                time.sleep(REQUEST_INTERVAL * (retry + 1))
                continue
            
            rows = target_table.find_all('tr')
            for row in rows:
                cols = [col.text.strip() for col in row.find_all('td')]
                if len(cols) >= 4 and cols[0] == date_str:
                    share_value = cols[3].replace(',', '')
                    share = float(share_value) / 10000
                    if share > 0:
                        return share
                    else:
                        logger.warning(f'{code} {date_str} 份额数据为0，无效')
                        return None
            
            logger.info(f'{code} {date_str} 未查询到对应日期数据')
            return None
        
        except Exception as e:
            logger.warning(f'{code} {date_str} 抓取失败，单只重试次数: {retry+1}，错误: {e}')
            time.sleep(REQUEST_INTERVAL * (retry + 1))
    
    logger.error(f'{code} {date_str} 多次重试后抓取失败')
    return None

def get_trading_days(start_date: str, end_date: str) -> list:
    try:
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        trading_days = []
        current = start
        
        while current <= end:
            if current.weekday() < 5:
                trading_days.append(current.strftime('%Y-%m-%d'))
            current += datetime.timedelta(days=1)
        
        return trading_days
    except Exception as e:
        logger.error(f'生成交易日列表失败: {e}')
        return []

def crawl_single_day(date_str: str) -> bool:
    logger.info(f'===== 开始爬取交易日: {date_str} =====')
    share_dict = {}
    total_share = 0
    
    for code in ETF_CODES:
        share = fetch_etf_share_by_date(code, date_str)
        if share is not None and share > 0:
            share_dict[code] = share
            total_share += share
            logger.info(f'{code}: {share:.4f} 亿份')
        else:
            logger.error(f'{code} 抓取失败，当日完整数据获取终止')
            return False
        time.sleep(REQUEST_INTERVAL)
    
    insert_data = {
        'date': date_str,
        'total': total_share
    }
    for code in ETF_CODES:
        insert_data[code] = share_dict[code]
    
    if insert_data_to_db(date_str, insert_data):
        mark_date_processed(date_str)
        logger.info(f'===== 完成爬取交易日: {date_str}，数据完整入库 =====\n')
        return True
    else:
        logger.error(f'===== 交易日: {date_str} 数据入库失败 =====\n')
        return False

def incremental_crawl():
    """【新增：自动跳过未更新日期，永不卡死】"""
    earliest_processed_date = get_earliest_processed_date()
    logger.info(f'当前断点：最早已完整处理日期 {earliest_processed_date}')
    
    end_crawl_date = START_DATE
    today = datetime.date.today().strftime('%Y-%m-%d')
    
    end_crawl_date_for_list = (datetime.datetime.strptime(earliest_processed_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    need_crawl_days = get_trading_days(end_crawl_date, end_crawl_date_for_list)
    
    need_crawl_days = [day for day in need_crawl_days if not check_date_is_processed(day)]
    need_crawl_days.reverse()
    
    if not need_crawl_days:
        logger.info('所有日期均已完整处理，无需执行爬取')
        return
    
    logger.info(f'待爬取交易日列表(从新到旧): {need_crawl_days}，共 {len(need_crawl_days)} 个交易日')
    
    # ========== 核心修复：单日失败不终止，自动跳过，继续爬旧的 ==========
    skiped_days = []
    for date_str in need_crawl_days:
        daily_success = False
        for retry in range(MAX_DAILY_RETRY + 1):
            if retry > 0:
                logger.warning(f'{date_str} 第{retry}次重试，等待{DAILY_RETRY_INTERVAL}秒后开始')
                time.sleep(DAILY_RETRY_INTERVAL)
            
            if crawl_single_day(date_str):
                daily_success = True
                break
        
        if not daily_success:
            # 失败了，自动跳过这个日期，继续爬前一天的
            logger.error(f'⚠️ {date_str} 多次重试后仍未获取完整数据，自动跳过，下次运行将重新爬取该日期！\n')
            skiped_days.append(date_str)
            # 不要break！继续爬后面的旧日期！
            continue
    # ==============================================================
    
    if len(skiped_days) > 0:
        logger.info(f'本次运行跳过了 {len(skiped_days)} 个未更新的日期: {skiped_days}，下次运行将自动重试')

def export_db_to_json():
    all_data = get_all_data_from_db()
    # ========== 核心修复：即使没有新数据，只要有旧数据，也能导出，永不报错 ==========
    if not all_data or not all_data.get('dates'):
        # 如果数据库是空的，但是有旧的JSON，直接用旧的
        if os.path.exists(JSON_FILE):
            logger.info('数据库暂无新数据，沿用现有JSON文件，保证前端可用')
            return
        else:
            logger.error('无有效数据可导出，终止JSON生成')
            return
    # ==============================================================
    
    try:
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        logger.info(f'已成功导出全量数据到 {JSON_FILE}，共 {len(all_data["dates"])} 条完整记录')
    except Exception as e:
        logger.error(f'导出JSON文件失败: {e}')

# -------------------------- 主流程 --------------------------
if __name__ == '__main__':
    logger.info('===== ETF数据增量更新任务开始（自动跳过未更新版）=====')
    backup_database()
    init_database()
    import_existing_json_to_db()
    incremental_crawl()
    export_db_to_json()
    logger.info('===== ETF数据增量更新任务执行完成 =====')
