# Dockerfile
# 1. 选择一个包含 Python 的基础镜像
FROM python:3.11-slim
# 2. 设置工作目录
WORKDIR /app

# 3. 复制依赖文件并安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 4. 暴露应用程序端口
EXPOSE 8000

# 5. 默认启动命令（使用 uvicorn 运行 server.py 中的应用，并在开发时启用热重载）
# 注意：实际代码将在 docker-compose 中通过卷挂载覆盖。
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]