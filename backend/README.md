# 星语后端（Feishu Bot + 网盘 MCP 代理）

后端代码分支，与前端镜像分支（mirror）分离管理。

## 文件说明
- `feishu_bot.py` — 飞书桥接后端：长连接交互 + SQLite 权威数据源 + AI 待办代理 + 定时调度 + Webhook 通知
- `baidu_netdisk_mcp_server.py` — 本地百度网盘 MCP 代理（只读：列目录/查容量/下载）
- `xy_backend_patch.js` — 前端云端对接补丁

## 说明
- 不含 `xingyu.db`（运行时数据）、`feishu_data.json`（含运行时凭据）、`downloads/`（临时文件）
- 完整部署见项目记忆 MEMORY.md
