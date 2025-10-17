# Zoom Earth Tile Downloader

这是一个自动化的服务，用于定期从 Zoom Earth 下载指定卫星的图像瓦片。它被设计为在 Docker 容器中长时间运行，并将下载的瓦片存储在本地，方便前端地图或其他应用使用。

## 特点

- **自动化**: 使用 `schedule` 库，可配置为每N小时自动运行一次。
- **增量下载**: 自动跳过已存在的瓦片，只下载缺失的部分。
- **高并发**: 使用线程池并发下载，提高效率。
- **容器化**: 提供 `Dockerfile` 和 `docker-compose.yml`，一键部署。
- **可配置**: 通过 `.env` 文件轻松配置目标卫星、Zoom范围和执行周期。
- **调试友好**: 支持在本地直接运行，无需 Docker。
- **清晰的目录结构**: 下载的瓦片按 `/<satellite>/<zoom>/<x>/<y>/<timestamp>.jpg` 结构存储，非常适合前端查询。

## 生产环境部署 (使用 Docker)

### 先决条件

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)

### 步骤

1.  **克隆或下载项目**

    ```bash
    git clone <your-repo-url>
    cd zoom-earth-downloader
    ```

2.  **创建配置文件**

    复制配置模板文件 `.env.example` 并重命名为 `.env`。

    ```bash
    cp .env.example .env
    ```

    打开 `.env` 文件，根据你的需求修改配置，例如 `SATELLITE`, `MIN_ZOOM`, `MAX_ZOOM` 等。

3.  **启动服务**

    使用 Docker Compose 在后台构建并启动服务。

    ```bash
    docker compose up -d --build
    ```

    -   第一次运行时，Docker 会根据 `Dockerfile` 构建镜像，这可能需要几分钟。
    -   服务启动后，会立即执行一次下载任务，然后按照 `.env` 中设置的 `CHECK_INTERVAL_HOURS` 定期执行。

4.  **查看日志**

    你可以查看容器的实时日志来监控下载进度。

    ```bash
    docker compose logs -f
    ```

5.  **访问瓦片**

    所有下载的瓦片文件都会出现在你项目根目录下的 `data/tiles/` 文件夹中。

6.  **停止服务**

    ```bash
    docker compose down
    ```

---

## 本地开发与调试 (不使用 Docker)

如果你想在本地修改和调试代码，可以不通过 Docker 直接运行。

### 先决条件

- Python 3.9+
- `uv` 或 `pip` (推荐 `uv`，速度更快)

### 步骤

1.  **创建并激活虚拟环境**

    ```bash
    # 使用 Python 内置的 venv
    python3 -m venv .venv
    source .venv/bin/activate  # macOS/Linux
    # .\.venv\Scripts\Activate.ps1  # Windows PowerShell
    ```

2.  **安装依赖**

    使用 `uv` (推荐) 或 `pip` 安装项目依赖。

    ```bash
    # 使用 uv
    uv pip install -r <(grep -E '^[a-zA-Z]' pyproject.toml | sed -e 's/ = .*//' -e 's/"//g')

    # 或者使用 pip
    # pip 会自动解析 pyproject.toml
    pip install .
    ```

3.  **创建配置文件**

    和 Docker 部署一样，确保你已经创建了 `.env` 文件。本地运行时，脚本会直接读取这个文件。

4.  **运行脚本**

    你可以用两种模式运行脚本：

    -   **单次运行模式 (用于调试)**:
        脚本会执行一次下载任务，然后立即退出。这非常适合测试代码改动。

        ```bash
        # 默认测试 zoom=3 的情况
        python src/main.py run-once --zoom 3

        # 测试 zoom=5 的情况
        python src/main.py run-once --zoom 5
        ```

    -   **服务模式 (模拟生产环境)**:
        脚本会像在 Docker 中一样，先运行一次，然后进入定时循环。

        ```bash
        python src/main.py
        ```

5.  **退出虚拟环境**

    完成调试后，可以退出虚拟环境。

    ```bash
    deactivate
    ```

## 目录结构

下载的瓦片将存储在 `./data/tiles` 目录下，结构如下：

```
tiles/
└── himawari/
├── 1/
│ ├── 13/
│ │ ├── 8/
│ │ │ └── 1743139200.jpg
│ │ └── 9/
│ │ ├── 1743139200.jpg
│ │ └── 1743139800.jpg
│ └── ...
├── 2/
│ └── ...
└── ...
```

这种结构使得按瓦片坐标 `(zoom, x, y)` 查询所有可用的时间戳变得非常高效。