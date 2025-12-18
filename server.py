import os
import requests  # 新增：用于调用 Backtrader API
from contextlib import asynccontextmanager
from typing import Annotated, Literal
from typing_extensions import TypedDict

from fastapi import FastAPI
from pydantic import BaseModel

# LangChain 核心
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.documents import Document
from langchain_core.tools import tool # 新增：工具定义

# LangGraph 图构建
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.prebuilt import ToolNode # 新增：工具节点

# 数据库与向量
from psycopg_pool import ConnectionPool
from langchain_postgres import PGVector
from langchain_huggingface import HuggingFaceEmbeddings

# --- 1. 数据库与向量库初始化 ---
DB_URI = os.getenv("DB_URI")
connection_pool = ConnectionPool(conninfo=DB_URI, min_size=1, max_size=10, kwargs={"autocommit": True})

print("🔄 初始化 Embedding 模型...")
embeddings = HuggingFaceEmbeddings(
    model_name="BAAI/bge-small-zh-v1.5",
    model_kwargs={'device': 'cpu'},
    encode_kwargs={'normalize_embeddings': True}
)

vector_store = PGVector(
    embeddings=embeddings,
    collection_name="knowledge_base",
    connection=DB_URI,
    use_jsonb=True,
)
print("✅ 向量数据库就绪")

# --- 2. 定义回测工具 (Backtrader Tool) ---
@tool
def execute_backtest(strategy_code: str, start_cash: float = 100000.0):
    """
    执行量化策略回测。
    Args:
        strategy_code: Python 代码字符串。
                       要求：必须包含一个名为 'GeneratedStrategy' 的类，继承自 bt.Strategy。
                       逻辑必须写在 next(self) 方法中。
        start_cash: 初始资金，默认 100000。
    """
    # Docker 内部网络：直接访问服务名 'backtrader_engine'
    url = "http://backtrader_engine:8001/run_backtest"
    payload = {
        "code": strategy_code,
        "start_cash": start_cash
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json() # 返回回测结果（盈亏、日志）
        else:
            return f"回测服务报错: {response.text}"
    except Exception as e:
        return f"无法连接到回测引擎: {str(e)}"

# --- 3. 定义 Agent 模型与状态 ---
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]

api_key = os.getenv("DEEPSEEK_API_KEY")
model = ChatOpenAI(
    model="deepseek-chat",
    openai_api_key=api_key,
    base_url="https://api.deepseek.com",
    temperature=0.7
)

# 绑定工具到模型 (让 Agent 知道它能做什么)
tools = [execute_backtest]
model_with_tools = model.bind_tools(tools)

# --- 4. 核心逻辑节点 ---
def agent_node(state: AgentState):
    messages = state["messages"]
    last_user_msg = messages[-1]
    
    # A. RAG 检索 (仅对用户消息进行检索)
    if isinstance(last_user_msg, HumanMessage):
        query = last_user_msg.content
        try:
            # 检索相关的 1 条知识
            docs = vector_store.similarity_search(query, k=1)
            if docs:
                context = docs[0].page_content
                print(f"📚 RAG 命中: {context[:20]}...")
                # 将知识作为 SystemMessage 插入到历史消息前，或者拼接到最后一条
                # 这里简单处理：拼接到 Prompt
                query = f"【参考背景知识】：{context}\n\n用户问题：{query}"
                # 更新最后一条消息的内容（不改变类型，仅增强上下文）
                messages[-1] = HumanMessage(content=query)
        except Exception as e:
            print(f"⚠️ RAG 检索跳过: {e}")

    # B. 调用模型
    response = model_with_tools.invoke(messages)
    return {"messages": [response]}

def should_continue(state: AgentState) -> Literal["tools", END]:
    last_message = state["messages"][-1]
    # 如果模型决定调用工具，跳转到 tools 节点
    if last_message.tool_calls:
        return "tools"
    return END

# --- 5. 构建图 (Workflow) ---
workflow = StateGraph(AgentState)

# 添加节点
workflow.add_node("agent", agent_node)
workflow.add_node("tools", ToolNode(tools)) # 专门执行工具的节点

# 定义边
workflow.add_edge(START, "agent")
workflow.add_conditional_edges("agent", should_continue)
workflow.add_edge("tools", "agent") # 工具执行完，结果回传给 Agent 继续思考

# --- 6. FastAPI 服务 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Agent 全功能服务启动 (RAG + Backtrader)...")
    yield
    print("🛑 服务关闭...")
    connection_pool.close()

app = FastAPI(lifespan=lifespan)

class ChatRequest(BaseModel):
    message: str
    thread_id: str

@app.post("/chat")
def chat_endpoint(request: ChatRequest):
    with connection_pool.connection() as conn:
        checkpointer = PostgresSaver(conn)
        checkpointer.setup()
        app_graph = workflow.compile(checkpointer=checkpointer)
        
        config = {"configurable": {"thread_id": request.thread_id}}
        # 这里的 messages 必须是列表
        final_state = None
        for event in app_graph.stream(
            {"messages": [HumanMessage(content=request.message)]}, 
            config=config
        ):
            final_state = event
            
        # 获取最后一条消息
        last_msg = final_state[list(final_state.keys())[0]]["messages"][-1]
        return {"response": last_msg.content}

# 知识入库接口 (保持不变)
class IngestRequest(BaseModel):
    text: str

@app.post("/ingest")
def ingest_endpoint(request: IngestRequest):
    doc = Document(page_content=request.text, metadata={"source": "api"})
    try:
        vector_store.add_documents([doc])
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
