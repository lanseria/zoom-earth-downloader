# 使用官方的 Python slim 镜像作为基础
FROM docker.m.daocloud.io/python:3.12-slim-bookworm

# 设置工作目录
WORKDIR /app

# 安装 poetry (如果使用 poetry) 或直接使用 pip
# 这里我们使用 pip，它已经内置
# 更新 pip
RUN pip install --upgrade pip

# 复制项目依赖定义文件
COPY pyproject.toml .

# 安装依赖
# `pip install .` 会读取 pyproject.toml 并安装 'dependencies' 下的所有包
RUN pip install .

# 复制源代码到工作目录
COPY ./src ./src

# 设置容器启动时执行的命令
CMD ["python", "src/main.py"]