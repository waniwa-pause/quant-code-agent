import os
import io
import time
import shutil
import rarfile
import pandas as pd
import re  # ✅ 新增：用于正则提取文件名中的日期
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ================= ⚙️ 配置区域 =================
DB_CONFIG = {
    "user": "user",
    "password": "password",
    "host": "localhost",
    # 注意：Docker 映射出来的端口是 5433
    "port": "5433",
    # 数据将存入 quant_db 库 (您刚才找到数据的地方)
    "dbname": "quant_db" 
}

# 数据源根目录
DATA_ROOT = r'D:\商品数据'

# ✅ 修改 1: 补充了 '2010' 以及其他年份，脚本将依次处理这些文件夹
TARGET_FOLDERS = ['2010', '2011', '2012', '2013', '2014', '2015', '2016']

# 临时解压目录 (脚本运行完会自动清理)
TEMP_FOLDER = './temp_extracted_data'

# 数据库目标表名
TARGET_TABLE = 'futures_tick_data'

# ✅ 修改 2: 定义需要的“标准列”
# 脚本会丢弃 CSV 中不在此列表里的其他列（如全0列）
COLUMN_MAPPING = [
    '市场代码', '合约代码', '时间', '最新', '持仓', '增仓', 
    '成交额', '成交量', '开仓', '平仓', '成交类型', '方向', 
    '买一价', '卖一价', '买一量', '卖一量'
]
# ===========================================

def get_engine():
    """建立数据库连接引擎"""
    uri = f"postgresql+psycopg2://{DB_CONFIG['user']}:{quote_plus(DB_CONFIG['password'])}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(uri)

def fast_copy_to_db(df, engine):
    """
    使用 PostgreSQL 的 COPY FROM 指令进行极速入库。
    比普通的 to_sql 快很多，适合大批量 Tick 数据。
    """
    if df.empty: return True
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    # 确保 DataFrame 列名与映射一致
    if len(df.columns) == len(COLUMN_MAPPING): 
        df.columns = COLUMN_MAPPING
        
    # 将 NaN/None 替换为空字符串，防止 COPY 报错
    df = df.where(pd.notnull(df), None)
    
    # 使用内存缓冲区模拟文件对象
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    
    try:
        # 核心：直接把内存中的 CSV 数据流 copy 进数据库
        cursor.copy_from(output, TARGET_TABLE, null='', sep='\t')
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"X({e})", end="", flush=True)
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    engine = get_engine()
    
    # 初始化：清理并重建临时目录
    if os.path.exists(TEMP_FOLDER): shutil.rmtree(TEMP_FOLDER)
    os.makedirs(TEMP_FOLDER)

    # 批处理大小：每累积 100 个 CSV 文件入库一次
    BATCH_SIZE = 100 
    
    print(f"🎯 准备处理年份: {TARGET_FOLDERS}")

    # --- 第一层循环：遍历年份文件夹 ---
    for year_folder in TARGET_FOLDERS:
        current_path = os.path.join(DATA_ROOT, year_folder)
        
        print(f"\n{'='*50}")
        print(f"📂 进入文件夹: {current_path}")

        if not os.path.exists(current_path):
            print(f"⚠️  [跳过] 找不到路径: {current_path}")
            continue

        # 寻找 .rar 文件
        rar_files = [f for f in os.listdir(current_path) if f.lower().endswith('.rar')]
        rar_files.sort()
        
        if not rar_files:
            print(f"⚠️  [跳过] {year_folder} 里没有 .rar 文件")
            continue

        print(f"✅ 发现 {len(rar_files)} 个压缩包，开始处理...")

        # --- 第二层循环：处理每个压缩包 ---
        for idx, rar_file in enumerate(rar_files):
            rar_path = os.path.join(current_path, rar_file)
            print(f"[{idx+1}/{len(rar_files)}] {year_folder}/{rar_file} ...", end="", flush=True)
            
            try:
                # 使用 rarfile 库解压
                with rarfile.RarFile(rar_path) as rf:
                    rf.extractall(TEMP_FOLDER)
                    csv_files = [f for f in rf.namelist() if f.lower().endswith('.csv')]
                    
                    print(f" 解压 {len(csv_files)} CSV | 清洗入库", end="", flush=True)
                    
                    dfs_buffer = [] 
                    
                    # --- 第三层循环：读取 CSV 并清洗 ---
                    for i, csv_f in enumerate(csv_files):
                        full_path = os.path.join(TEMP_FOLDER, csv_f)
                        try:
                            # 尝试不同编码读取
                            try: df = pd.read_csv(full_path, encoding='gbk')
                            except: df = pd.read_csv(full_path, encoding='utf-8')
                            
                            # ==================== ✅ 核心修改 A: 智能筛选列 ====================
                            # 目的：去除多余的全0列，只保留 COLUMN_MAPPING 里的列
                            if set(COLUMN_MAPPING).issubset(df.columns):
                                # 如果表头齐全，直接按列名提取（最安全）
                                df = df[COLUMN_MAPPING]
                            else:
                                # 如果表头对不上，或者有多余列，强制截取前 N 列
                                df = df.iloc[:, :len(COLUMN_MAPPING)]
                                df.columns = COLUMN_MAPPING
                            
                            # ==================== ✅ 核心修改 B: 补全日期 ====================
                            # 目的：将 "09:15:00" 变成 "2010-01-04 09:15:00"
                            
                            # 1. 优先从 RAR 文件名找8位数字 (如 20100104.rar)
                            date_match = re.search(r"(\d{8})", rar_file)
                            
                            # 2. 找不到则去 CSV 文件名找
                            if not date_match:
                                date_match = re.search(r"(\d{8})", csv_f)
                                
                            if date_match:
                                raw_date = date_match.group(1) # 拿到 "20100104"
                                # 格式化为 "2010-01-04"
                                date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
                                
                                # 3. 拼接：日期 + 空格 + 原时间列
                                if '时间' in df.columns:
                                    # astype(str) 防止时间列被识别为对象导致报错
                                    df['时间'] = date_str + ' ' + df['时间'].astype(str)
                            # ==============================================================

                            dfs_buffer.append(df)
                            
                            # 缓冲区满或文件处理完时，执行入库
                            if len(dfs_buffer) >= BATCH_SIZE or i == len(csv_files) - 1:
                                if dfs_buffer:
                                    big_df = pd.concat(dfs_buffer, ignore_index=True)
                                    if fast_copy_to_db(big_df, engine):
                                        print(".", end="", flush=True)
                                    else:
                                        print("X", end="", flush=True)
                                    dfs_buffer = [] # 清空缓冲
                        except Exception as e:
                            # 单个CSV出错不中断整体
                            pass
                        finally:
                            # 读完即删，节省磁盘空间
                            if os.path.exists(full_path): os.remove(full_path)
                
                print(" 完成", flush=True)
                
            except Exception as e:
                print(f"\n[错误] 处理压缩包失败: {e}", flush=True)
        # --- End of RAR loop ---

    print(f"\n{'='*50}")
    print(f"🎉🎉🎉 所有任务处理完成！请去数据库检查数据。")

if __name__ == "__main__":
    main()