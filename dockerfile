# 选择一个基础镜像，这里我们使用官方的 Python 3.11 镜像
FROM python:3.11-slim

# 设置工作目录为 /cal_code
WORKDIR /cal_code
ENV DEBIAN_FRONTEND=noninteractive TZ=Asia/Shanghai PYTHONPATH=/cal_code

# 复制当前目录中的所有文件到容器的 /app 目录
COPY . /cal_code

# 安装 Python 依赖
# 假设你的依赖列表在 requirements.txt 文件中
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    sed -i 's|deb.debian.org|mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list.d/debian.sources \
    && sed -i 's|security.debian.org|mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources \
    && apt update -y
RUN RUN --mount=type=cache,target=/root/.cache pip install -r requirements.txt


# 清理apt缓存，以减小镜像大小
RUN apt clean && rm -rf /var/lib/apt/lists/*
# 设置容器启动时默认执行的命令
CMD ["python", "tick2trade.py"]
