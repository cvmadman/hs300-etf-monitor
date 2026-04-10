import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import sqlite3
import os
import logging
import shutil

# -------------------------- 全局配置项（无需修改，适配原有环境） --------------------------
# 9只沪深300ETF代码，与原有配置完全一致
ETF_CODES = [
    '510300', '510310', '510330', '510360',
    '510350', '510390', '510320', '510380', '510370'
]
# 数据库文件路径
DB_FILE = 'etf.db'
# 数据库备份文件路径
DB_BACKUP_FILE = 'etf_backup.db'
# 前端用的JSON数据文件路径，与原有文件完全兼容
JSON_FILE = 'etf_data.json'
# 历史数据起始日期，与原有数据起始时间一致
START_DATE = '2024-09-01'
# 请求重试次数
MAX_RETRY = 3
# 请求基础间隔（秒），防反爬
REQUEST_INTERVAL = 0.8

# -------------------------- 日志初始化（方便Actions排查问题） --------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# -------------------------- 数据库核心操作（优化版，增加容错与备份） --------------------------
def backup_database():
    """运行前自动备份数据库，防止文件损坏"""
    if os.path.exists(DB_FILE):
        try:
            shutil.copy2(DB_FILE, DB_BACKUP_FILE)
            logger.info('数据库备份完成')
        except Exception as e:
            logger.warning(f'数据库备份失败: {e}，继续运行主流程')

def init_database():
    """初始化数据库，创建表结构（不存在则创建，已存在不影响）"""
    try:
        # 提前拼接ETF字段，避免f-string嵌套语法错误
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
        # 新增索引优化查询速度
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON etf_share("date")')
        conn.commit()
        conn.close()
        logger.info(f'数据库初始化完成，文件: {DB_FILE}')
    except Exception as e:
        logger.error(f'数据库初始化失败: {e}')
        # 数据库损坏时，自动从备份恢复
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

def get_latest_date_in_db():
    """获取数据库中最新的日期，用于增量爬取，无数据返回起始日期"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT MAX("date") FROM etf_share')
        latest_date = cursor.fetchone()[0]
        conn.close()
        return latest_date if latest_date else START_DATE
    except Exception as e:
        logger.error(f'获取最新数据日期失败: {e}')
        return START_DATE

def check_date_exists_in_db(date_str: str) -> bool:
    """检查指定日期是否已存在数据库中，避免重复导入"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM etf_share WHERE "date" = ?', (date_str,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def insert_data_to_db(date_str: str, share_data: dict):
    """插入单日数据到数据库，自动去重，带数据校验"""
    # 数据强校验：日期非空、总份额大于0、无负数
    if not date_str or share_data.get('total', 0) <= 0:
        logger.warning(f'{date_str} 数据校验不通过，跳过入库')
        return
    for code in ETF_CODES:
        if share_data.get(code, 0) < 0:
            logger.warning(f'{date_str} {code} 份额为负数，跳过入库')
            return
    
    try:
        # 提前拼接字段，避免语法错误
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
        logger.info(f'{date_str} 数据已成功存入数据库，合计份额: {share_data["total"]:.2f} 亿份')
    except Exception as e:
        logger.error(f'{date_str} 数据入库失败: {e}')

def get_all_data_from_db():
    """从数据库获取全量有序数据，用于导出JSON，与原有前端格式100%兼容"""
    try:
        # 提前拼接字段，避免f-string嵌套语法错误
        etf_fields = ', '.join([f'"{code}"' for code in ETF_CODES])
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f'SELECT "date", "total", {etf_fields} FROM etf_share ORDER BY "date" ASC')
        all_rows = cursor.fetchall()
        conn.close()
        
        # 严格按照原有JSON格式转换，前端无需任何修改
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
        
        logger.info(f'从数据库读取全量数据完成，共 {len(result["dates"])} 条记录')
        return result
    except Exception as e:
        logger.error(f'读取全量数据失败: {e}')
        return {}

def import_existing_json_to_db():
    """导入已有的etf_data.json数据到数据库，仅导入缺失数据，避免重复爬取"""
    if not os.path.exists(JSON_FILE):
        logger.info('无现有JSON文件，跳过导入步骤')
        return
    
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # 校验JSON格式是否与原有格式一致
        if 'dates' not in json_data or 'total' not in json_data:
            logger.warning('JSON格式不符合要求，跳过导入')
            return
        
        # 仅导入数据库中不存在的日期，提升导入速度
        import_count = 0
        for idx, date_str in enumerate(json_data['dates']):
            if check_date_exists_in_db(date_str):
                continue
            
            share_data = {
                'date': date_str,
                'total': json_data['total'][idx]
            }
            for code in ETF_CODES:
                share_data[code] = json_data.get(code, [])[idx] if idx < len(json_data.get(code, [])) else 0
            
            insert_data_to_db(date_str, share_data)
            import_count += 1
        
        if import_count > 0:
            logger.info(f'成功导入现有JSON数据，新增 {import_count} 条历史记录')
        else:
            logger.info('JSON数据已全部存在于数据库中，无新增导入')
    except Exception as e:
        logger.error(f'导入JSON数据失败: {e}')

# -------------------------- 爬虫核心操作（最新优化版，高容错+高成功率） --------------------------
def fetch_etf_share_by_date(code: str, date_str: str) -> float | None:
    """从上海证券交易所官网抓取单只ETF指定日期的份额，带重试机制，适配最新页面结构"""
    url = f'https://www.sse.com.cn/assortment/fund/list/etfinfo/basic/index.shtml?FUNDID={code}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://www.sse.com.cn/',
        'Connection': 'keep-alive'
    }

    # 失败重试机制
    for retry in range(MAX_RETRY):
        try:
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()  # 捕获HTTP错误
            soup = BeautifulSoup(res.text, 'lxml')
            
            # 精准定位基金规模表格，适配上交所最新DOM结构，多层容错
            target_table = None
            tables = soup.find_all('table')
            for table in tables:
                table_text = table.get_text()
                if '基金规模' in table_text and '日期' in table_text and '基金总份额' in table_text:
                    target_table = table
                    break
            
            # 兜底方案：未找到带标题的表格，遍历所有表格匹配日期
            if not target_table:
                target_table = tables[0] if tables else None
            
            if not target_table:
                logger.warning(f'{code} {date_str} 未找到数据表格，重试次数: {retry+1}')
                time.sleep(REQUEST_INTERVAL * (retry + 1))
                continue
            
            # 解析表格行，匹配目标日期
            rows = target_table.find_all('tr')
            for row in rows:
                cols = [col.text.strip() for col in row.find_all('td')]
                if len(cols) >= 4 and cols[0] == date_str:
                    # 基金总份额(万份) 转换为 亿份，与原有单位完全一致
                    share_value = cols[3].replace(',', '')
                    share = float(share_value) / 10000
                    return share
            
            # 未匹配到日期，说明当日无数据（休市/未更新）
            logger.info(f'{code} {date_str} 未查询到对应日期数据')
            return None
        
        except Exception as e:
            logger.warning(f'{code} {date_str} 抓取失败，重试次数: {retry+1}，错误: {e}')
            time.sleep(REQUEST_INTERVAL * (retry + 1))
    
    # 重试全部失败
    logger.error(f'{code} {date_str} 多次重试后抓取失败')
    return None

def get_trading_days(start_date: str, end_date: str) -> list:
    """生成交易日列表，自动跳过周末，后续通过数据校验过滤法定休市日"""
    try:
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        trading_days = []
        current = start
        
        while current <= end:
            # 周一到周五为潜在交易日，周末直接跳过
            if current.weekday() < 5:
                trading_days.append(current.strftime('%Y-%m-%d'))
            current += datetime.timedelta(days=1)
        
        return trading_days
    except Exception as e:
        logger.error(f'生成交易日列表失败: {e}')
        return []

def incremental_crawl():
    """增量爬取核心逻辑，仅爬取数据库缺失的交易日数据"""
    # 1. 获取数据库最新日期，确定爬取起始点
    latest_date_in_db = get_latest_date_in_db()
    # 2. 爬取结束日期为当日
    today = datetime.date.today().strftime('%Y-%m-%d')
    
    # 3. 生成需要爬取的交易日列表
    need_crawl_days = get_trading_days(latest_date_in_db, today)
    # 过滤掉已存在的最新日期，避免重复爬取
    if latest_date_in_db in need_crawl_days:
        need_crawl_days.remove(latest_date_in_db)
    
    if not need_crawl_days:
        logger.info('所有数据已是最新，无需执行爬取')
        return
    
    logger.info(f'开始增量爬取，共 {len(need_crawl_days)} 个待爬取交易日: {need_crawl_days}')
    
    # 4. 逐天爬取数据
    for date_str in need_crawl_days:
        logger.info(f'===== 处理交易日: {date_str} =====')
        share_dict = {}
        total_share = 0
        
        # 逐只抓取ETF份额
        for code in ETF_CODES:
            share = fetch_etf_share_by_date(code, date_str)
            if share is not None and share > 0:
                share_dict[code] = share
                total_share += share
                logger.info(f'{code}: {share:.2f} 亿份')
            # 动态请求间隔，防反爬
            time.sleep(REQUEST_INTERVAL)
        
        # 仅当有有效数据时入库，过滤休市日无数据的情况
        if total_share > 0:
            # 构建入库数据
            insert_data = {
                'date': date_str,
                'total': total_share
            }
            for code in ETF_CODES:
                insert_data[code] = share_dict.get(code, 0)
            # 写入数据库
            insert_data_to_db(date_str, insert_data)
        else:
            logger.warning(f'{date_str} 未获取到任何有效数据，跳过入库（大概率为休市日）')

def export_db_to_json():
    """从数据库导出全量数据到JSON文件，与原有前端格式100%兼容"""
    all_data = get_all_data_from_db()
    if not all_data or not all_data.get('dates'):
        logger.error('无有效数据可导出，终止JSON生成')
        return
    
    try:
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        logger.info(f'已成功导出全量数据到 {JSON_FILE}，共 {len(all_data["dates"])} 条记录')
    except Exception as e:
        logger.error(f'导出JSON文件失败: {e}')

# -------------------------- 主流程（一键执行，零配置） --------------------------
if __name__ == '__main__':
    logger.info('===== ETF数据增量更新任务开始 =====')
    # 1. 运行前备份数据库
    backup_database()
    # 2. 初始化数据库
    init_database()
    # 3. 导入现有JSON历史数据（首次运行兼容，重复运行无影响）
    import_existing_json_to_db()
    # 4. 增量爬取缺失数据
    incremental_crawl()
    # 5. 导出最新全量数据到JSON，供前端使用
    export_db_to_json()
    logger.info('===== ETF数据增量更新任务执行完成 =====')
