#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度网盘 MCP 本地代理服务器（个人自建应用版）
- 对外提供标准 MCP SSE 接口 (http://127.0.0.1:8765/sse)
- 内部调用百度网盘普通 OpenAPI（个人自建应用 access_token）
- 个人应用权限说明：只读类 API 可用（列表/配额/用户信息/文件元信息），
  写入类（上传/建文件夹/搜索）被百度侧拒绝（Unsupported open api），已不暴露。

用法: python3 baidu_netdisk_mcp_server.py
"""
import json
import os
import queue
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ==================== 配置 ====================
ACCESS_TOKEN = os.environ.get("PAN_ACCESS_TOKEN", "你的百度网盘access_token")
HOST = "127.0.0.1"
PORT = 8765

PAN_APP_ID = "250528"
PAN_CHANNEL = "chunlei"

# ==================== SSE 会话管理 ====================
sessions = {}
sessions_lock = threading.Lock()


def new_session():
    sid = "%032x" % int(time.time() * 1e6)
    with sessions_lock:
        sessions[sid] = queue.Queue()
    return sid


def push_event(sid, event, data):
    with sessions_lock:
        q = sessions.get(sid)
    if q is not None:
        q.put(f"event: {event}\ndata: {data}\n\n")


def drop_session(sid):
    with sessions_lock:
        sessions.pop(sid, None)


# ==================== 百度网盘 API 封装 ====================
def _baidu_get(url, params):
    params.update({"access_token": ACCESS_TOKEN, "channel": PAN_CHANNEL,
                   "web": 1, "app_id": PAN_APP_ID})
    qs = urllib.parse.urlencode(params)
    full = f"{url}?{qs}"
    req = urllib.request.Request(full, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def api_user_info():
    return _baidu_get("https://pan.baidu.com/rest/2.0/xpan/nas", {"method": "uinfo"})


def api_quota():
    return _baidu_get("https://pan.baidu.com/api/quota", {"checkexpire": 1, "checkfree": 1})


def api_file_list(dir_path="/", num=50):
    return _baidu_get("https://pan.baidu.com/api/list",
                      {"dir": dir_path, "num": num, "order": "time", "desc": 1})


def api_file_meta(fsids):
    """fsids: list[int]，返回文件元信息；对文件返回 dlink 下载链接"""
    return _baidu_get("https://pan.baidu.com/rest/2.0/xpan/multimedia",
                      {"method": "filemetas", "fsids": json.dumps(fsids), "dlink": 1})


def find_fsid_by_path(path):
    """按网盘绝对路径找到 fs_id（在父目录列表中查找）"""
    dir_path, filename = path.rsplit("/", 1)
    if not dir_path:
        dir_path = "/"
    r = api_file_list(dir_path, 200)
    if r.get("errno") != 0:
        raise RuntimeError(f"列目录失败: {r.get('errmsg', r)}")
    for f in r.get("list", []):
        if f.get("server_filename") == filename and not f.get("isdir"):
            return f["fs_id"]
    raise RuntimeError(f"在 {dir_path} 下找不到文件: {filename}")


def api_download(path, save_dir="downloads"):
    """按网盘路径下载文件到本地，返回 (本地路径, 大小, 错误或None)"""
    try:
        fsid = find_fsid_by_path(path)
        meta = api_file_meta([fsid])
        item = (meta.get("list") or [{}])[0]
        dlink = item.get("dlink")
        filename = item.get("filename") or path.rsplit("/", 1)[-1]
        size = item.get("size", 0)
        if not dlink:
            return None, 0, f"未获取到 dlink（文件可能不存在或无权限）: {meta}"
        sep = "&" if "?" in dlink else "?"
        url = f"{dlink}{sep}access_token={ACCESS_TOKEN}"
        os.makedirs(save_dir, exist_ok=True)
        # 清理本地文件名中的非法字符
        safe = "".join(c for c in filename if c not in '\\/:*?"<>|')
        local_path = os.path.join(save_dir, safe)
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "Chrome/120.0 Safari/537.36",
            "Referer": "https://pan.baidu.com/",
        })
        with urllib.request.urlopen(req, timeout=120) as resp, open(local_path, "wb") as f:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                f.write(chunk)
        got = os.path.getsize(local_path)
        if got != size:
            return local_path, got, f"大小不一致（预期{size}，实际{got}），可能下载不完整"
        return local_path, got, None
    except Exception as e:
        return None, 0, f"下载失败: {e}"


# ==================== MCP 工具定义 ====================
TOOLS = [
    {
        "name": "user_info",
        "description": "获取当前百度网盘账号基本信息（昵称、头像、uk）。",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_quota",
        "description": "获取网盘空间使用情况（总容量、已用、剩余）。",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "file_list",
        "description": "列出指定目录下的文件和文件夹。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dir": {"type": "string", "description": "目录绝对路径，默认 /"},
                "num": {"type": "number", "description": "返回条数，默认 50"},
            },
        },
    },
    {
        "name": "file_meta",
        "description": "按 fs_id 查询文件/文件夹元信息；查询文件时返回 dlink 下载链接。fs_id 可从 file_list 结果中获取。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "fsids": {"type": "array", "items": {"type": "number"},
                          "description": "fs_id 列表，最多10个，如 [512980898571766]"},
            },
            "required": ["fsids"],
        },
    },
    {
        "name": "download_file",
        "description": "把网盘中的文件下载到代理运行的这台机器的 downloads 目录，返回本地保存路径。不依赖分享链接。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string",
                         "description": "网盘文件绝对路径，如 /测试/回响AI虚拟社区 (14).html"},
            },
            "required": ["path"],
        },
    },
]


def call_tool(name, arguments):
    """执行工具，返回 (isError, text)"""
    args = arguments or {}
    try:
        if name == "user_info":
            r = api_user_info()
        elif name == "get_quota":
            r = api_quota()
        elif name == "file_list":
            r = api_file_list(args.get("dir", "/"), int(args.get("num", 50)))
        elif name == "file_meta":
            r = api_file_meta(args["fsids"])
        elif name == "download_file":
            local_path, size, err = api_download(args["path"])
            if err:
                return True, err
            return False, json.dumps(
                {"local_path": local_path, "size": size, "message": "下载完成"}, ensure_ascii=False)
        else:
            return True, f"未知工具: {name}"
        return False, json.dumps(r, ensure_ascii=False)
    except Exception as e:
        return True, f"调用失败: {e}"


# ==================== HTTP 处理 ====================
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/sse":
            sid = new_session()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(
                f"event: endpoint\ndata: /message?sessionId={sid}\n\n".encode())
            self.wfile.flush()
            with sessions_lock:
                q = sessions.get(sid)
            try:
                while True:
                    try:
                        evt = q.get(timeout=30)
                        self.wfile.write(evt.encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                drop_session(sid)
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/message":
            self.send_error(404)
            return
        qs = urllib.parse.parse_qs(parsed.query)
        sid = (qs.get("sessionId") or [""])[0]
        with sessions_lock:
            exists = sid in sessions
        if not exists:
            self._json_resp({"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32602, "message": "Invalid session ID"}})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self._json_resp({"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32700, "message": "Parse error"}})
            return

        method = body.get("method")
        msg_id = body.get("id")

        if method == "initialize":
            result = {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "baidu-netdisk-proxy", "version": "1.1.0"},
            }
            resp = {"jsonrpc": "2.0", "id": msg_id, "result": result}
        elif method == "notifications/initialized":
            resp = None
        elif method == "tools/list":
            resp = {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS}}
        elif method == "tools/call":
            name = body["params"]["name"]
            arguments = body["params"].get("arguments", {})
            is_error, text = call_tool(name, arguments)
            content = [{"type": "text", "text": text}]
            result = {"content": content, "isError": is_error}
            resp = {"jsonrpc": "2.0", "id": msg_id, "result": result}
        elif method == "ping":
            resp = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
        else:
            resp = {"jsonrpc": "2.0", "id": msg_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"}}
            push_event(sid, "message", json.dumps(resp, ensure_ascii=False))

        if resp is not None:
            raw = json.dumps(resp, ensure_ascii=False)
            self._json_resp(resp)
            push_event(sid, "message", raw)
        else:
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def _json_resp(self, obj):
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(raw)


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"百度网盘 MCP 代理已启动: http://{HOST}:{PORT}/sse")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
