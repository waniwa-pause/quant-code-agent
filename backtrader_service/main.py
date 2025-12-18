import backtrader as bt
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import sys
import io
import datetime

app = FastAPI()

class BacktestRequest(BaseModel):
    code: str
    start_cash: float = 100000.0

# 模拟数据生成
def get_dummy_data():
    dates = pd.date_range(start='2023-01-01', periods=100)
    data = pd.DataFrame({
        'open': [20000 + i*10 for i in range(100)],
        'high': [20500 + i*10 for i in range(100)],
        'low': [19500 + i*10 for i in range(100)],
        'close': [20200 + i*10 for i in range(100)],
        'volume': [1000] * 100
    }, index=dates)
    return data

@app.post("/run_backtest")
def run_backtest(request: BacktestRequest):
    cerebro = bt.Cerebro()
    
    local_scope = {}
    try:
        exec(request.code, {"bt": bt, "datetime": datetime}, local_scope)
        StrategyClass = local_scope.get('GeneratedStrategy')
        if not StrategyClass:
            return {"status": "error", "message": "代码中未找到 'GeneratedStrategy' 类"}
    except Exception as e:
        return {"status": "error", "message": f"代码编译失败: {str(e)}"}

    cerebro.addstrategy(StrategyClass)
    
    data = bt.feeds.PandasData(dataname=get_dummy_data())
    cerebro.adddata(data)
    
    cerebro.broker.setcash(request.start_cash)
    start_val = cerebro.broker.getvalue()

    capture = io.StringIO()
    sys.stdout = capture
    
    try:
        cerebro.run()
    except Exception as e:
        sys.stdout = sys.__stdout__
        return {"status": "error", "message": f"回测运行报错: {str(e)}"}
        
    sys.stdout = sys.__stdout__
    end_val = cerebro.broker.getvalue()
    
    return {
        "status": "success",
        "initial_cash": start_val,
        "final_value": end_val,
        "pnl": end_val - start_val,
        "logs": capture.getvalue()
    }