import os
import io
import time
import shutil
import rarfile
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ================= 配置区域 =================
DB_CONFIG = {
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5433",
    "dbname": "quant_db"
}

# ✅ 1. 设置根目录 (脚本会去这个目录下找子文件夹)
DATA_ROOT = r'D:\商品数据'

# ✅ 2. 指定要进入的子文件夹名称
# 脚本会依次进入 D:\商品数据\2011, D:\商品数据\2012 等文件夹
TARGET_FOLDERS = ['2011', '2012', '2013', '2014', '2015', '2016']

TEMP_FOLDER = './temp_extracted_data'
TARGET_TABLE = 'futures_tick_data'
COLUMN_MAPPING = [
    '市场代码', '合约代码', '时间', '最新', '持仓', '增仓', 
    '成交额', '成交量', '开仓', '平仓', '成交类型', '方向', 
    '买一价', '卖一价', '买一量', '卖一量'
]
# ===========================================

def get_engine():
    uri = f"postgresql+psycopg2://{DB_CONFIG['user']}:{quote_plus(DB_CONFIG['password'])}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(uri)

def fast_copy_to_db(df, engine):
    if df.empty: return True
    conn = engine.raw_connection()
    cursor = conn.cursor()
    if len(df.columns) == len(COLUMN_MAPPING): df.columns = COLUMN_MAPPING
    df = df.where(pd.notnull(df), None)
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    try:
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
    
    if os.path.exists(TEMP_FOLDER): shutil.rmtree(TEMP_FOLDER)
    os.makedirs(TEMP_FOLDER)

    BATCH_SIZE = 100 
    
    print(f"🎯 准备处理以下子文件夹: {TARGET_FOLDERS}")

    # --- 外层循环：遍历年份文件夹 ---
    for year_folder in TARGET_FOLDERS:
        # 拼凑完整路径，例如 D:\商品数据\2011
        current_path = os.path.join(DATA_ROOT, year_folder)
        
        print(f"\n{'='*50}")
        print(f"📂 进入文件夹: {current_path}")

        # 1. 检查文件夹是否存在
        if not os.path.exists(current_path):
            print(f"⚠️  [跳过] 找不到文件夹: {current_path}")
            print(f"   (请确认 D:\\商品数据 下是否有 {year_folder} 这个文件夹)")
            continue

        # 2. 找该文件夹里的 RAR
        rar_files = [f for f in os.listdir(current_path) if f.lower().endswith('.rar')]
        rar_files.sort()
        
        if not rar_files:
            print(f"⚠️  [跳过] 文件夹 {year_folder} 里没有找到 .rar 文件。")
            continue

        print(f"✅ 发现 {len(rar_files)} 个压缩包，开始处理...")

        # --- 内层循环：处理每个 RAR ---
        for idx, rar_file in enumerate(rar_files):
            rar_path = os.path.join(current_path, rar_file)
            print(f"[{idx+1}/{len(rar_files)}] {year_folder}/{rar_file} ...", end="", flush=True)
            
            try:
                with rarfile.RarFile(rar_path) as rf:
                    rf.extractall(TEMP_FOLDER)
                    csv_files = [f for f in rf.namelist() if f.lower().endswith('.csv')]
                    
                    print(f" 解压 {len(csv_files)} CSV | 入库", end="", flush=True)
                    
                    dfs_buffer = [] 
                    for i, csv_f in enumerate(csv_files):
                        full_path = os.path.join(TEMP_FOLDER, csv_f)
                        try:
                            try: df = pd.read_csv(full_path, encoding='gbk')
                            except: df = pd.read_csv(full_path, encoding='utf-8')
                            
                            dfs_buffer.append(df)
                            
                            if len(dfs_buffer) >= BATCH_SIZE or i == len(csv_files) - 1:
                                if dfs_buffer:
                                    big_df = pd.concat(dfs_buffer, ignore_index=True)
                                    if fast_copy_to_db(big_df, engine):
                                        print(".", end="", flush=True)
                                    else:
                                        print("X", end="", flush=True)
                                    dfs_buffer = []
                        except: pass
                        finally:
                            if os.path.exists(full_path): os.remove(full_path)
                
                print(" 完成", flush=True)
                
            except Exception as e:
                print(f"\n[错误] {e}", flush=True)
        # --- End of RAR loop ---

    print(f"\n{'='*50}")
    print(f"🎉🎉🎉 所有指定的年份文件夹全部处理完成！")

if __name__ == "__main__":
    main()