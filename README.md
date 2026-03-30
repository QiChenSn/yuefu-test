# Yuefu 测试工具
用于解码提供的 Parquet 测试数据，并调用 `/omr` 接口的命令行辅助工具。

## 前置条件
- Python 3.11 及以上版本
- 已激活 `uv` 虚拟环境（终端显示 `(yuefu-test)` 提示符）
- 具备访问 Yuefu OMR 服务的网络权限

安装依赖：
```powershell
cd D:\Code\Project\yuefu-test
uv pip install -e .[dev]
```

## 使用方法
```powershell
cd D:\Code\Project\yuefu-test
uv run yuefu-test --base-url https://example.com --row 0
```

可选参数：
- `--data` – Parquet 数据分片文件（默认值：`test_data/test-00000-of-00001.parquet`）
- `--row` – 待解码的行索引（默认值：`0`）
- `--max-attempts` – 最大上传重试次数（默认值：`3`）
- `--poll-timeout` / `--poll-interval` – 配置轮询时长与轮询间隔
- `-H/--header` – 自定义请求头（格式：`键:值`）
- `--verify-ssl/--no-verify-ssl` – 开启/关闭 TLS 证书验证

可设置环境变量 `OMR_BASE_URL`，无需每次手动传入 `--base-url`。

该命令会输出结构化 JSON 数据，汇总执行次数、重试策略与成功率。若重试耗尽后仍执行失败，将返回非零退出码。

## 测试
```powershell
cd D:\Code\Project\yuefu-test
uv run pytest
```