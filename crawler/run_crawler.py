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
# 9只沪深300ETF代码，与上交所官网完全匹配
ETF_CODES = [
    '510300', '510310', '510330', '510360',
    '510350', '510390', '510320', '510380', '510370'
]
# 数据库文件路径
DB_FILE = 'etf.db'
# 数据库备份文件路径
DB_BACKUP_FILE = 'etf_backup.db'
# 前端兼容的JSON数据文件
JSON_FILE = 'etf_data.json'
# 历史数据起始日期
START_DATE = '2026-04-01'
# 单只ETF请求重试次数
MAX_RETRY = 3
# 单日完整爬取重试次数
MAX_DAILY_RETRY = 2
# 请求基础间隔（防反爬）
REQUEST_INTERVAL = 0.8
# 单日重试间隔
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
    """运行前自动备份数据库，防止文件损坏"""
    if os.path.exists(DB_FILE):
        try:
            shutil.copy2(DB_FILE, DB_BACKUP_FILE)
            logger.info('数据库备份完成')
        except Exception as e:
            logger.warning(f'数据库备份失败: {e}，继续运行主流程')

def init_database():
    """初始化数据库，创建份额表和进度表"""
    try:
        # 提前拼接字段，避免f-string嵌套语法错误
        etf_fields = ', '.join([f'"{code}" REAL NOT NULL DEFAULT 0' for code in ETF_CODES])
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # ETF份额数据表（仅存储9只ETF全部成功的完整数据）
        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS etf_share (
            "date" TEXT PRIMARY KEY NOT NULL,
            "total" REAL NOT NULL DEFAULT 0,
            {etf_fields}
        )
        '''
        cursor.execute(create_table_sql)
        
        # 爬取进度记录表（仅标记完整入库的日期）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS crawl_progress (
            "date" TEXT PRIMARY KEY NOT NULL,
            "is_processed" INTEGER NOT NULL DEFAULT 0,
            "process_time" TEXT NOT NULL
        )
        ''')
        
        # 新增索引优化查询速度
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON etf_share("date")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed ON crawl_progress("is_processed")')
        conn.commit()
        conn.close()
        logger.info(f'数据库初始化完成，文件: {DB_FILE}')
    except Exception as e:
        logger.error(f'数据库初始化失败: {e}')
        # 数据库损坏自动从备份恢复
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
    """获取最早已完整处理的日期，作为反向爬取的断点锚点"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT MIN("date") FROM crawl_progress WHERE "is_processed" = 1')
        earliest_date = cursor.fetchone()[0]
        conn.close()
        # 无处理记录时，返回今日后一天，从今日开始爬
        return earliest_date if earliest_date else (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f'获取最早处理日期失败: {e}')
        return (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

def check_date_is_processed(date_str: str) -> bool:
    """检查指定日期是否已完整处理，避免重复爬取"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM crawl_progress WHERE "date" = ? AND "is_processed" = 1', (date_str,))
    is_processed = cursor.fetchone() is not None
    conn.close()
    return is_processed

def mark_date_processed(date_str: str):
    """仅在数据完整入库后标记进度，推进断点"""
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
    """【带完整入库日志】仅9只ETF全部有效才入库，确保数据完整"""
    # 强校验1：日期非空
    if not date_str:
        logger.error(f'{date_str} 日期为空，终止入库')
        return False
    
    # 强校验2：9只ETF全部有>0的有效数据
    for code in ETF_CODES:
        if share_data.get(code, 0) <= 0:
            logger.error(f'{date_str} {code} 无有效份额数据，终止入库')
            return False
    
    # 强校验3：总份额与单只合计匹配
    calc_total = sum([share_data[code] for code in ETF_CODES])
    if abs(share_data['total'] - calc_total) > 0.01:
        logger.error(f'{date_str} 总份额校验不匹配，终止入库')
        return False
    
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

        # ========== 数据库插入完整详情日志 ==========
        logger.info('='*60)
        logger.info(f'✅ 数据库插入成功 | 日期：{date_str}')
        logger.info(f'📊 合计总份额：{share_data["total"]:.4f} 亿份')
        for code in ETF_CODES:
            logger.info(f'  {code}：{share_data[code]:.4f} 亿份')
        logger.info('='*60 + '\n')
        # =============================================

        return True
    except Exception as e:
        logger.error(f'{date_str} 数据入库失败: {e}')
        return False

def get_all_data_from_db():
    """从数据库获取全量有序数据，100%兼容前端JSON格式"""
    try:
        etf_fields = ', '.join([f'"{code}"' for code in ETF_CODES])
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f'SELECT "date", "total", {etf_fields} FROM etf_share ORDER BY "date" ASC')
        all_rows = cursor.fetchall()
        conn.close()
        
        # 严格匹配前端需要的JSON格式
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
    """导入现有JSON历史数据，仅导入完整数据"""
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
            
            # 校验该日期9只ETF数据全部完整
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
            
            # 导入数据并标记进度
            if insert_data_to_db(date_str, share_data):
                mark_date_processed(date_str)
                import_count += 1
        
        if import_count > 0:
            logger.info(f'成功导入现有JSON数据，新增 {import_count} 条完整历史记录')
        else:
            logger.info('JSON数据已全部导入完成，无新增完整记录')
    except Exception as e:
        logger.error(f'导入JSON数据失败: {e}')

# -------------------------- 核心爬虫逻辑（适配上交所官网结构） --------------------------
def fetch_etf_share_by_date(code: str, date_str: str) -> float | None:
    """从上海证券交易所官网抓取单只ETF指定日期的份额，100%适配官网表格结构"""
    url = f'https://www.sse.com.cn/assortment/fund/list/etfinfo/basic/index.shtml?FUNDID={code}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://www.sse.com.cn/',
        'Connection': 'keep-alive'
    }

    # 单只ETF失败重试机制
    for retry in range(MAX_RETRY):
        try:
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # ========== 核心修复：精准匹配基金规模表格 ==========
            target_table = None
            # 遍历所有表格，找到带「日期」和「基金总份额」表头的目标表格
            for table in soup.find_all('table'):
                th_texts = [th.get_text(strip=True) for th in table.find_all('th')]
                if '日期' in th_texts and '基金总份额' in th_texts:
                    target_table = table
                    break
            
            if not target_table:
                logger.warning(f'{code} {date_str} 未找到基金规模表格，单只重试次数: {retry+1}')
                time.sleep(REQUEST_INTERVAL * (retry + 1))
                continue
            
            # 解析表格行，自动去除单元格前后空白字符，解决日期匹配失败问题
            for row in target_table.find_all('tr'):
                cols = [td.get_text(strip=True) for td in row.find_all('td')]
                # 匹配日期列，基金总份额在第4列（索引3），单位：万份
                if len(cols) >= 4 and cols[0] == date_str:
                    share_value = cols[3].replace(',', '')
                    share = float(share_value) / 10000  # 万份转亿份
                    if share > 0:
                        return share
                    else:
                        logger.warning(f'{code} {date_str} 份额数据为0，无效')
                        return None
            
            # 未匹配到目标日期，当日无数据
            logger.info(f'{code} {date_str} 未查询到对应日期数据')
            return None
        
        except Exception as e:
            logger.warning(f'{code} {date_str} 抓取失败，单只重试次数: {retry+1}，错误: {str(e)[:50]}')
            time.sleep(REQUEST_INTERVAL * (retry + 1))
    
    # 多次重试全部失败
    logger.error(f'{code} {date_str} 多次重试后抓取失败')
    return None

def get_trading_days(start_date: str, end_date: str) -> list:
    """生成交易日列表，自动跳过周末"""
    try:
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        trading_days = []
        current = start
        
        while current <= end:
            if current.weekday() < 5:  # 周一到周五为潜在交易日
                trading_days.append(current.strftime('%Y-%m-%d'))
            current += datetime.timedelta(days=1)
        
        return trading_days
    except Exception as e:
        logger.error(f'生成交易日列表失败: {e}')
        return []

def crawl_single_day(date_str: str) -> bool:
    """单日完整爬取逻辑，仅9只ETF全部成功返回True"""
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
        # 请求间隔防反爬
        time.sleep(REQUEST_INTERVAL)
    
    # 构建入库数据
    insert_data = {
        'date': date_str,
        'total': total_share
    }
    for code in ETF_CODES:
        insert_data[code] = share_dict[code]
    
    # 入库并返回结果
    if insert_data_to_db(date_str, insert_data):
        mark_date_processed(date_str)
        logger.info(f'===== 完成爬取交易日: {date_str}，数据完整入库 =====\n')
        return True
    else:
        logger.error(f'===== 交易日: {date_str} 数据入库失败 =====\n')
        return False

def incremental_crawl():
    """反向增量爬取：从新往旧爬，永不卡死，自动跳过未更新日期"""
    earliest_processed_date = get_earliest_processed_date()
    logger.info(f'当前断点：最早已完整处理日期 {earliest_processed_date}')
    
    end_crawl_date = START_DATE  # 爬取终点：2024-09-01
    today = datetime.date.today().strftime('%Y-%m-%d')
    
    # 生成待爬取日期列表：从起始日期到断点前一天，反转后从新往旧爬
    end_crawl_date_for_list = (datetime.datetime.strptime(earliest_processed_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    need_crawl_days = get_trading_days(end_crawl_date, end_crawl_date_for_list)
    
    # 过滤已处理日期，反转顺序从新往旧爬
    need_crawl_days = [day for day in need_crawl_days if not check_date_is_processed(day)]
    need_crawl_days.reverse()
    
    if not need_crawl_days:
        logger.info('所有日期均已完整处理，无需执行爬取')
        return
    
    logger.info(f'待爬取交易日列表(从新到旧): {need_crawl_days}，共 {len(need_crawl_days)} 个交易日')
    
    skiped_days = []
    # 严格按顺序爬取，单日失败不终止，自动跳过，继续爬旧日期
    for date_str in need_crawl_days:
        daily_success = False
        # 单日完整重试机制
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
            continue
    
    if len(skiped_days) > 0:
        logger.info(f'本次运行跳过了 {len(skiped_days)} 个未更新的日期: {skiped_days}，下次运行将自动重试')

def export_db_to_json():
    """导出全量数据到JSON，100%兼容前端，无数据也不报错"""
    all_data = get_all_data_from_db()
    
    # 核心容错：如果数据库暂无新数据，但有旧JSON，直接沿用，保证前端可用
    if not all_data or not all_data.get('dates'):
        if os.path.exists(JSON_FILE):
            logger.info('数据库暂无新数据，沿用现有JSON文件，前端可正常访问')
            return
        else:
            logger.error('无有效数据可导出，终止JSON生成')
            return
    
    try:
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        logger.info(f'已成功导出全量数据到 {JSON_FILE}，共 {len(all_data["dates"])} 条完整记录')
    except Exception as e:
        logger.error(f'导出JSON文件失败: {e}')

# -------------------------- 主流程 --------------------------
if __name__ == '__main__':
    logger.info('===== ETF数据增量更新任务开始（上交所适配版）=====')
    backup_database()
    init_database()
    import_existing_json_to_db()
    incremental_crawl()
    export_db_to_json()
    logger.info('===== ETF数据增量更新任务执行完成 =====')
