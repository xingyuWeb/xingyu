# -*- coding: utf-8 -*-
"""
星语 · 待办助手飞书桥接服务
============================
职责：
  1. 长连接接收飞书消息（im.message.receive_v1）
  2. 交给后端 AI（云端配置+云端上下文）理解并读写待办；无配置时退化为关键词队列
  3. 前端执行完 ack 后，把结果回发到飞书会话
  4. 定时检查未完成/逾期待办，通过飞书提醒用户

部署：
  pip install lark-oapi
  修改下方 APP_ID / APP_SECRET
  运行：python feishu_bot.py

前端配合（部署好后由豆包把前端代码并入星语 HTML）：
  GET  /api/poll  拉取待执行指令  {"commands":[{seq,cmd,data,raw,ts}]}
  POST /api/ack   执行结果回报    {"seq":1,"ok":true,"result":"..."}
  POST /api/sync  合并同步待办    {"todos":[...],"deletedIds":[...]} -> 返回云端权威列表
  GET  /api/todos 读取云端待办
  GET  /api/health 连通性检查
"""
import base64
import hashlib
import hmac
import json
import os as _os
import re
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import requests

# 容器系统时区为 UTC，强制按北京时间运行，避免 _is_overdue 解析本地时间字符串偏 8 小时
_os.environ.setdefault("TZ", "Asia/Shanghai")
try:
    time.tzset()
except AttributeError:
    pass

# ============ 配置区（必填） ============
# 两种方式：
#   A. 直接改下面默认值（本地/临时跑）—— 已填入当前应用的凭证
#   B. 环境变量覆盖（服务器部署推荐，密钥不写死在脚本里）
#      export FEISHU_APP_ID="cli_xxx"
#      export FEISHU_APP_SECRET="xxx"
#      export FEISHU_BOT_PORT=8848
APP_ID = _os.environ.get("FEISHU_APP_ID", "cli_xxxxxxxxxxxx")       # 飞书开放平台 → 应用凭证 → App ID
APP_SECRET = _os.environ.get("FEISHU_APP_SECRET", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")  # App Secret（妥善保管）
PORT = int(_os.environ.get("FEISHU_BOT_PORT", "8848"))                  # 前端轮询服务端口
# ========================================

DATA_FILE = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "feishu_data.json")

# ============ 本地数据 ============
DATA = {
    "todos": [],          # 前端同步上来的全量待办
    "commands": [],       # 指令队列（待前端拉取）
    "chat_id": None,      # 最近活跃会话（定时提醒发到这里）
    "last_remind": {},    # 每个待办上次提醒时间戳（去重，避免轰炸）
    "seq": 0,             # 指令序号
}
_lock = threading.Lock()


def load():
    global DATA
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            DATA = json.load(f)
    except Exception:
        DATA = {
            "todos": [], "commands": [], "chat_id": None,
            "last_remind": {}, "seq": 0,
        }


def save():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(DATA, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[save] 失败:", e)


# ============ SQLite 落盘（配置 / 会话 / 消息 / LLM 审计） ============
DB_FILE = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "xingyu.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at REAL
);
CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY,
  type TEXT,
  name TEXT,
  meta TEXT,
  created_at REAL,
  updated_at REAL
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  role TEXT,
  sender_id TEXT,
  sender_name TEXT,
  content TEXT,
  image TEXT,
  meta TEXT,
  created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_msg_conv ON messages(conversation_id, created_at);
CREATE TABLE IF NOT EXISTS llm_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id TEXT,
  model TEXT,
  request TEXT,
  response TEXT,
  status INTEGER,
  created_at REAL
);
CREATE TABLE IF NOT EXISTS todos (
  id TEXT PRIMARY KEY,
  title TEXT,
  done INTEGER DEFAULT 0,
  deleted INTEGER DEFAULT 0,
  chat_id TEXT,
  created_at_ts REAL,
  due_at_ts REAL,
  meta TEXT,
  updated_at REAL
);
CREATE TABLE IF NOT EXISTS schedules (
  id TEXT PRIMARY KEY,
  todo_id TEXT,
  title TEXT,
  fire_at REAL,
  repeat TEXT,
  tone TEXT,
  last_fired REAL,
  sent_count INTEGER DEFAULT 0,
  active INTEGER DEFAULT 1,
  created_at REAL
);
"""


def db_conn():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db_conn()
    try:
        conn.executescript(SCHEMA)
        # 轻量迁移：老库补 deleted 列（表已存在时 CREATE 不会加列）
        try:
            conn.execute("ALTER TABLE todos ADD COLUMN deleted INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.commit()
    finally:
        conn.close()


def kv_get(key, default=None):
    conn = db_conn()
    try:
        row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default
    except Exception:
        return default
    finally:
        conn.close()


def kv_set(key, value):
    conn = db_conn()
    try:
        conn.execute(
            "INSERT INTO kv(key,value,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, json.dumps(value, ensure_ascii=False), time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def conv_upsert(cid, ctype="private", name="", meta=None):
    if not cid:
        return
    now = time.time()
    conn = db_conn()
    try:
        conn.execute(
            "INSERT INTO conversations(id,type,name,meta,created_at,updated_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET type=excluded.type, name=excluded.name, "
            "meta=excluded.meta, updated_at=excluded.updated_at",
            (cid, ctype, name, json.dumps(meta or {}, ensure_ascii=False), now, now),
        )
        conn.commit()
    finally:
        conn.close()


def _parse_ts(s):
    """前端时间字符串（'YYYY-MM-DDTHH:MM' 或 'YYYY-MM-DD HH:MM' 或 ISO）转时间戳，失败返回 None"""
    if not s:
        return None
    try:
        return time.mktime(time.strptime(str(s).replace("T", " ")[:16], "%Y-%m-%d %H:%M"))
    except Exception:
        return None


def _todo_row(conn, t, chat_id=None):
    """按 id 落一行待办；done 粘滞（一旦完成不会被旧数据翻回未完成）。"""
    tid = t.get("id") or ""
    if not tid:
        return False
    prev = conn.execute("SELECT done, deleted FROM todos WHERE id=?", (tid,)).fetchone()
    prev_done = prev["done"] if prev else 0
    prev_deleted = prev["deleted"] if prev else 0
    new_done = 1 if (t.get("done") or prev_done) else 0
    conn.execute(
        "INSERT OR REPLACE INTO todos"
        "(id,title,done,deleted,chat_id,created_at_ts,due_at_ts,meta,updated_at) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (
            tid,
            t.get("title") or "",
            new_done,
            prev_deleted,
            chat_id or "",
            _parse_ts(t.get("createdAt")),
            _parse_ts(t.get("dueTime")),
            json.dumps(t, ensure_ascii=False),
            time.time(),
        ),
    )
    return True


def todo_sync(incoming, deleted_ids=None, chat_id=None):
    """前端全量同步：按 id 合并、done 粘滞、尊重删除标记；返回云端权威列表。"""
    conn = db_conn()
    try:
        for tid in (deleted_ids or []):
            if tid:
                conn.execute("UPDATE todos SET deleted=1, updated_at=? WHERE id=?", (time.time(), str(tid)))
        for t in (incoming or []):
            if not isinstance(t, dict):
                continue
            tid = t.get("id")
            if not tid:
                continue
            row = conn.execute("SELECT deleted FROM todos WHERE id=?", (tid,)).fetchone()
            if row and row["deleted"]:
                continue  # 云端已删，忽略前端旧数据
            _todo_row(conn, t, chat_id=chat_id)
        conn.commit()
    finally:
        conn.close()
    return todo_list()


def todo_list():
    """读回未删除的全量待办（meta 存的是前端原始对象）。"""
    conn = db_conn()
    try:
        rows = conn.execute(
            "SELECT meta FROM todos WHERE deleted=0 ORDER BY COALESCE(due_at_ts, 9e18)"
        ).fetchall()
        out = []
        for r in rows:
            try:
                out.append(json.loads(r["meta"]))
            except Exception:
                pass
        return out
    finally:
        conn.close()


def _new_todo_id():
    return "ai_" + uuid.uuid4().hex[:12]


def _match_todos(conn, match):
    """按标题/内容关键词匹配未删除待办，空关键词匹配全部。"""
    m = (match or "").strip().lower()
    rows = conn.execute("SELECT id, meta FROM todos WHERE deleted=0").fetchall()
    out = []
    for r in rows:
        try:
            obj = json.loads(r["meta"])
        except Exception:
            continue
        title = (obj.get("title") or "").lower()
        content = (obj.get("content") or "").lower()
        if (not m) or (m in title) or (m in content):
            out.append((r["id"], obj))
    return out


def _match_todos_pair(match):
    """独立连接版：返回 [(id, obj)]，供调度关联使用。"""
    conn = db_conn()
    try:
        return _match_todos(conn, match)
    finally:
        conn.close()


def todo_add(title, content="", due_time="", chat_id=None):
    """AI 新增一条待办，返回该待办对象。"""
    t = {
        "id": _new_todo_id(),
        "title": title or "未命名待办",
        "content": content or "",
        "done": False,
        "createdAt": time.strftime("%Y-%m-%dT%H:%M"),
        "dueTime": due_time or "",
    }
    conn = db_conn()
    try:
        _todo_row(conn, t, chat_id=chat_id)
        conn.commit()
    finally:
        conn.close()
    return t


def todo_set_done(match):
    conn = db_conn()
    try:
        hits = _match_todos(conn, match)
        for tid, obj in hits:
            obj["done"] = True
            conn.execute(
                "UPDATE todos SET done=1, meta=?, updated_at=? WHERE id=?",
                (json.dumps(obj, ensure_ascii=False), time.time(), tid),
            )
        conn.commit()
        return [o.get("title") or tid for tid, o in hits]
    finally:
        conn.close()


def todo_delete(match):
    conn = db_conn()
    try:
        hits = _match_todos(conn, match)
        for tid, _obj in hits:
            conn.execute("UPDATE todos SET deleted=1, updated_at=? WHERE id=?", (time.time(), tid))
        conn.commit()
        return [o.get("title") or tid for tid, o in hits]
    finally:
        conn.close()


def todo_update(match, title=None, due_time=None):
    conn = db_conn()
    try:
        hits = _match_todos(conn, match)
        out = []
        for tid, obj in hits:
            if title:
                obj["title"] = title
            if due_time:
                obj["dueTime"] = due_time
            conn.execute(
                "UPDATE todos SET title=?, due_at_ts=?, meta=?, updated_at=? WHERE id=?",
                (obj.get("title") or "", _parse_ts(obj.get("dueTime")),
                 json.dumps(obj, ensure_ascii=False), time.time(), tid),
            )
            out.append(obj.get("title") or tid)
        conn.commit()
        return out
    finally:
        conn.close()


# ============ 调度表（定时提醒 / 每日汇总） ============
def _today_ts(hh, mm, day_offset=0):
    lt = time.localtime(time.time() + day_offset * 86400)
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hh, mm, 0, 0, 0, -1))


def schedule_add(title, fire_at=None, repeat="", tone="", todo_id=""):
    sid = "sc_" + uuid.uuid4().hex[:12]
    conn = db_conn()
    try:
        conn.execute(
            "INSERT INTO schedules(id,todo_id,title,fire_at,repeat,tone,last_fired,sent_count,active,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (sid, todo_id or "", title or "", fire_at or 0, repeat or "", tone or "",
             0, 0, 1, time.time()),
        )
        conn.commit()
    finally:
        conn.close()
    return sid


def schedule_list_active():
    conn = db_conn()
    try:
        rows = conn.execute("SELECT * FROM schedules WHERE active=1").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def schedule_mark_fired(sid, is_once):
    conn = db_conn()
    try:
        conn.execute(
            "UPDATE schedules SET last_fired=?, sent_count=sent_count+1, active=? WHERE id=?",
            (time.time(), 0 if is_once else 1, sid),
        )
        conn.commit()
    finally:
        conn.close()


def schedule_deactivate(sid):
    conn = db_conn()
    try:
        conn.execute("UPDATE schedules SET active=0 WHERE id=?", (sid,))
        conn.commit()
    finally:
        conn.close()


def schedule_deactivate_by_todo(todo_id):
    if not todo_id:
        return
    conn = db_conn()
    try:
        conn.execute("UPDATE schedules SET active=0 WHERE todo_id=?", (todo_id,))
        conn.commit()
    finally:
        conn.close()


def todo_is_done(todo_id):
    if not todo_id:
        return False
    conn = db_conn()
    try:
        row = conn.execute("SELECT done, deleted FROM todos WHERE id=?", (todo_id,)).fetchone()
        return bool(row and (row["done"] or row["deleted"]))
    finally:
        conn.close()


def _schedule_next_fire(s, now):
    """算出该调度下一次应触发的时间戳；无则返回 None。"""
    rep = (s.get("repeat") or "").strip()
    lf = s.get("last_fired") or 0
    if rep in ("", "once"):
        if (s.get("sent_count") or 0) == 0:
            return s.get("fire_at") or None
        return None
    if rep.startswith("daily@"):
        parts = rep.split("@", 1)[1].split(":")
        hh, mm = int(parts[0]), int(parts[1])
        t = _today_ts(hh, mm, 0)
        if t <= lf:
            t = _today_ts(hh, mm, 1)
        return t
    return None


def msg_add(mid, cid, role, content, sender_id="", sender_name="", image="", meta=None, created_at=None):
    conn = db_conn()
    try:
        conn.execute(            "INSERT OR REPLACE INTO messages"
            "(id,conversation_id,role,sender_id,sender_name,content,image,meta,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (mid, cid, role, sender_id, sender_name, content or "", image or "",
             json.dumps(meta or {}, ensure_ascii=False), created_at or time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def llm_forward(api_config, payload, path="/chat/completions", timeout=120):
    """用云端保存的配置，服务端直连上游 OpenAI 兼容接口。返回原始字节，不解析。"""
    base = (api_config or {}).get("baseUrl") or ""
    key = (api_config or {}).get("apiKey") or ""
    if not base:
        return 400, json.dumps({"error": {"message": "no baseUrl configured"}}, ensure_ascii=False).encode("utf-8"), "application/json"
    url = base.rstrip("/") + path
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = "Bearer " + key
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    return resp.status_code, resp.content, resp.headers.get("Content-Type", "application/json")


# ============ 后端 AI 待办助手 ============
def _conv_history(cid, limit=12):
    """读云端该会话最近若干条 user/assistant 记录，作为上下文注入上游。"""
    conn = db_conn()
    try:
        rows = conn.execute(
            "SELECT role, content FROM messages WHERE conversation_id=? "
            "ORDER BY created_at DESC LIMIT ?",
            (cid, limit),
        ).fetchall()
    finally:
        conn.close()
    return [{"role": r["role"], "content": r["content"]}
            for r in reversed(rows) if r["role"] in ("user", "assistant")]


def _parse_ai_json(content):
    """从模型输出里抠出 {reply, actions}；失败则整段当回复，无动作。"""
    s = (content or "").strip()
    if s.startswith("```"):
        s = s.strip("`").strip()
        if s[:4].lower() == "json":
            s = s[4:].strip()
    start, end = s.find("{"), s.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(s[start:end + 1])
            if isinstance(obj, dict):
                actions = obj.get("actions")
                return (obj.get("reply") or ""), (actions if isinstance(actions, list) else [])
        except Exception:
            pass
    return (content or "").strip(), []


_AI_SYSTEM_TMPL = (
    "你是「星语」的待办助手，通过飞书与用户用中文对话。"
    "你可以读写用户的待办，还能设置定时提醒，用自然语言理解意图。\n"
    "规则：只能依据下方给出的当前待办/当前时间操作，不要编造；"
    "只输出一个 JSON 对象，不要输出额外说明、不要用代码块。\n"
    '输出格式：{"reply":"给用户的回复","actions":[ ... ]}\n'
    "actions 可为空数组，支持的操作：\n"
    '  {"op":"add","title":"标题","dueTime":"YYYY-MM-DDTHH:MM"}\n'
    '  {"op":"schedule","title":"标题","fireAt":"YYYY-MM-DDTHH:MM","repeat":"","tone":"提醒语气"}\n'
    '  {"op":"done","match":"标题关键词"}\n'
    '  {"op":"delete","match":"标题关键词"}\n'
    '  {"op":"update","match":"关键词","title":"新标题","dueTime":"..."}\n'
    "说明：\n"
    "- 时间一律用北京时间、0-23 时、补零，如 2026-09-11T08:00。\n"
    "- add 带 dueTime 时会自动生成到点提醒；用户明确要「提醒我」时可用 schedule。\n"
    "- repeat 留空表示只提醒一次；每日固定提醒写 daily@HH:MM。\n"
    "- tone 是这条提醒希望的语气，用户没指定就留空。\n\n"
)


def _ai_system_prompt():
    now = time.strftime("%Y-%m-%d %H:%M %A")
    return (_AI_SYSTEM_TMPL
            + "当前时间（北京时间）：" + now + "\n"
            + "当前待办(JSON)：\n" + json.dumps(todo_list(), ensure_ascii=False))


def ai_compose_reminder(title, tone):
    """让 AI 按指定语气生成提醒文案；失败则用模板。返回文本。"""
    cfg = kv_get("ai_config", {})
    if not (cfg.get("baseUrl") and cfg.get("apiKey")):
        return None
    prob = ("用一句话提醒我这条待办，直接输出提醒内容本身，不要引号、不要解释：%s" % title)
    if tone:
        prob += "\n语气要求：" + tone
    payload = {"model": cfg.get("model") or "", "stream": False,
               "messages": [{"role": "user", "content": prob}]}
    code, raw, _ct = llm_forward(cfg, payload)
    if code != 200:
        return None
    try:
        txt = json.loads(raw.decode("utf-8", "replace"))["choices"][0]["message"]["content"].strip()
        return txt or None
    except Exception:
        return None


def ai_todo_agent(chat_id, text, sender_name=""):
    """处理一条飞书消息：注入云端上下文 → 调上游 → 执行待办动作 → 落库。返回回复文本。"""
    cfg = kv_get("ai_config", {})
    if not (cfg.get("baseUrl") and cfg.get("apiKey")):
        return None  # 未配置上游，交由关键词兜底

    conv_upsert(chat_id, "private", sender_name or "飞书用户")
    msg_add("u_" + uuid.uuid4().hex[:12], chat_id, "user", text, sender_name=sender_name)

    messages = [{"role": "system", "content": _ai_system_prompt()}]
    messages += _conv_history(chat_id, 12)
    if not messages or messages[-1].get("content") != text:
        messages.append({"role": "user", "content": text})

    payload = {"model": cfg.get("model") or "", "messages": messages, "stream": False}
    code, raw, _ct = llm_forward(cfg, payload)
    if code != 200:
        print("[ai] 上游错误:", code, raw[:200])
        return None
    try:
        content = json.loads(raw.decode("utf-8", "replace"))["choices"][0]["message"]["content"]
    except Exception as e:
        print("[ai] 解析响应失败:", e)
        return None

    reply, actions = _parse_ai_json(content)
    notes = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        op = a.get("op")
        try:
            if op == "add":
                t = todo_add(a.get("title") or "未命名待办", a.get("content") or "",
                             a.get("dueTime") or "", chat_id)
                # 带截止时间则自动登记到点提醒
                if a.get("dueTime"):
                    schedule_add(t["title"], _parse_ts(a.get("dueTime")),
                                 "", a.get("tone") or "", t["id"])
                notes.append("已新增「%s」" % t["title"])
            elif op == "schedule":
                fire = _parse_ts(a.get("fireAt"))
                if not fire:
                    notes.append("提醒时间没解析出来，未设置")
                else:
                    sid = schedule_add(a.get("title") or "提醒", fire,
                                       a.get("repeat") or "", a.get("tone") or "")
                    notes.append("已设置提醒「%s」" % (a.get("title") or "提醒"))
            elif op == "done":
                hits = todo_set_done(a.get("match") or "")
                for tid, _obj in _match_todos_pair(a.get("match") or ""):
                    schedule_deactivate_by_todo(tid)
                notes.append("已划掉：" + ("、".join(hits) if hits else "未找到匹配"))
            elif op == "delete":
                hits = todo_delete(a.get("match") or "")
                for tid, _obj in _match_todos_pair(a.get("match") or ""):
                    schedule_deactivate_by_todo(tid)
                notes.append("已删除：" + ("、".join(hits) if hits else "未找到匹配"))
            elif op == "update":
                hits = todo_update(a.get("match") or "", a.get("title"), a.get("dueTime"))
                notes.append("已更新：" + ("、".join(hits) if hits else "未找到匹配"))
        except Exception as e:
            print("[ai] 动作失败:", op, e)

    if not reply:
        reply = "；".join(notes) if notes else "收到"
    elif notes:
        reply = reply + "\n（" + "；".join(notes) + "）"
    msg_add("a_" + uuid.uuid4().hex[:12], chat_id, "assistant", reply)
    print("[ai]", chat_id, "->", reply[:80])
    return reply


# ============ 飞书客户端 ============
import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
)

client = lark.Client.builder() \
    .app_id(APP_ID) \
    .app_secret(APP_SECRET) \
    .log_level(lark.LogLevel.INFO) \
    .build()


def send_feishu(text, chat_id=None):
    """以机器人身份发文本消息到指定会话"""
    cid = chat_id or DATA.get("chat_id")
    if not cid:
        print("[send] 没有可用的 chat_id，跳过发送:", text)
        return False
    try:
        receive_id_type = "open_id" if str(cid).startswith("ou_") else "chat_id"
        req = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(str(cid))
                          .msg_type("text")
                          .content(json.dumps({"text": text}, ensure_ascii=False))
                          .build()) \
            .build()
        resp = client.im.v1.message.create(req)
        if resp.success():
            return True
        print("[send] 发送失败:", resp.code, resp.msg)
        return False
    except Exception as e:
        print("[send] 异常:", e)
        return False


# ============ 通知通道（Webhook 单向推送） ============
def notify_config():
    return kv_get("notify_config", {}) or {}


def _webhook_sign(secret, timestamp):
    """飞书自定义机器人签名：HMAC-SHA256(base64)"""
    string_to_sign = "%s\n%s" % (timestamp, secret)
    digest = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def webhook_send(text):
    """用自定义机器人 Webhook 单向推送文本。返回是否成功。"""
    cfg = notify_config()
    url = (cfg.get("webhookUrl") or "").strip()
    if not url:
        return False
    body = {"msg_type": "text", "content": {"text": text}}
    secret = (cfg.get("webhookSecret") or "").strip()
    if secret:
        ts = str(int(time.time()))
        body["timestamp"] = ts
        body["sign"] = _webhook_sign(secret, ts)
    try:
        resp = requests.post(url, json=body, timeout=15)
        if resp.status_code == 200:
            return True
        print("[webhook] 发送失败:", resp.status_code, resp.text[:200])
        return False
    except Exception as e:
        print("[webhook] 异常:", e)
        return False


def notify(text, kind="reminder"):
    """统一通知出口：提醒类优先走 Webhook，其余走长连接机器人；Webhook 不可用时回退长连接。"""
    if kind == "reminder":
        if webhook_send(text):
            print("[notify] webhook ->", text[:60])
            return True
        print("[notify] webhook 不可用，回退长连接")
    ok = send_feishu(text)
    print("[notify] longconn ->", text[:60])
    return ok


# ============ 指令解析 ============
def parse_command(raw_text):
    """把用户消息转成结构化指令（具体执行交给前端，后端只做粗解析）"""
    text = (raw_text or "").strip()
    if not text:
        return None

    # 完成 / 划掉 / 勾掉：完成待办
    for kw in ("完成待办", "划掉待办", "勾掉待办", "标记完成", "完成", "划掉", "勾掉"):
        if text.startswith(kw):
            match = text[len(kw):].strip()
            return {"cmd": "done", "data": {"match": match}, "raw": text}

    # 新增 / 添加 / 记录：新增待办
    for kw in ("新增待办", "添加待办", "记录待办", "记一下", "新增", "添加", "记录"):
        if text.startswith(kw):
            title = text[len(kw):].strip()
            if not title:
                title = "未命名待办"
            return {"cmd": "add", "data": {"title": title}, "raw": text}

    # 搜索待办：模糊匹配
    for kw in ("搜索待办", "查找待办", "搜待办", "查待办", "找待办"):
        if text.startswith(kw):
            keyword = text[len(kw):].strip()
            return {"cmd": "search", "data": {"keyword": keyword}, "raw": text}

    # 删除待办：删除匹配项
    for kw in ("删除待办", "删掉待办", "清除待办", "移除待办", "删除", "删掉"):
        if text.startswith(kw):
            keyword = text[len(kw):].strip()
            return {"cmd": "delete", "data": {"keyword": keyword}, "raw": text}

    # 列表 / 待办 / 看看：列出未完成
    if any(kw in text for kw in ("列出待办", "查看待办", "待办列表", "有哪些待办", "看看待办", "我的待办", "待办呢")):
        return {"cmd": "list", "data": {}, "raw": text}

    # 其他：原样回显 + 帮助提示
    return {"cmd": "echo", "data": {"raw": text}, "raw": text}


# ============ 长连接消息处理 ============
def do_p2_im_message_receive_v1(data: lark.im.v1.P2ImMessageReceiveV1):
    try:
        ev = data.event
        msg = ev.message
        sender = ev.sender

        # 忽略机器人自己发的消息
        if sender and sender.sender_type == "app":
            return

        chat_id = getattr(msg, "chat_id", None) or ""
        content_raw = getattr(msg, "content", None) or "{}"
        message_type = getattr(msg, "message_type", "") or "text"
        if message_type != "text":
            return

        try:
            text = json.loads(content_raw).get("text", "").strip()
        except Exception:
            text = content_raw.strip()
        # 剥掉 @机器人 占位符（群聊消息形如 "@_user_1 在吗"）
        text = re.sub(r"@_user_\d+\s*", "", text).strip()

        # 记住活跃会话，定时提醒发到这里
        if chat_id:
            with _lock:
                DATA["chat_id"] = chat_id
                save()

        print("[recv]", chat_id, "->", text)

        # 交给后端 AI 处理（后台线程，避免阻塞长连接）；无上游配置时退化为关键词指令
        def _run_ai(cid, txt):
            try:
                reply = ai_todo_agent(cid, txt)
                if reply:
                    send_feishu(reply, cid)
                else:
                    _queue_keyword_command(cid, txt)
            except Exception as e:
                print("[ai] 处理异常:", e)
                try:
                    _queue_keyword_command(cid, txt)
                except Exception:
                    pass

        threading.Thread(target=_run_ai, args=(chat_id, text), daemon=True).start()
    except Exception as e:
        print("[handler] 异常:", e)


def _queue_keyword_command(chat_id, text):
    """关键词兜底：入队让前端执行（沿用旧协议）。"""
    cmd = parse_command(text)
    if cmd is None:
        return
    with _lock:
        DATA["seq"] += 1
        cmd["seq"] = DATA["seq"]
        cmd["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
        DATA["commands"].append(cmd)
        save()
    send_feishu("已收到指令，正在处理：%s" % text, chat_id)


event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .build()

ws_client = lark.ws.Client(
    APP_ID, APP_SECRET,
    event_handler=event_handler,
    log_level=lark.LogLevel.INFO,
)


# ============ 本地 HTTP（供网页前端轮询） ============
class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _raw(self, code, text, content_type="application/json; charset=utf-8"):
        body = text.encode("utf-8") if isinstance(text, str) else text
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path.startswith("/api"):
            print("[GET]", path)
        if path == "/api/health":
            self._json(200, {"ok": True, "app": "feishu_bridge"})
            return
        if path == "/api/poll":
            with _lock:
                cmds = DATA["commands"]
                DATA["commands"] = []
                save()
            self._json(200, {"commands": cmds})
            return
        if path == "/api/config":
            self._json(200, {"ok": True, "config": kv_get("ai_config", {})})
            return
        if path == "/api/notify-config":
            c = notify_config()
            url = c.get("webhookUrl") or ""
            safe = dict(c)
            safe.pop("webhookUrl", None)
            safe.pop("webhookSecret", None)
            safe["hasWebhook"] = bool(url)
            safe["webhookMasked"] = ("…/" + url[-6:]) if len(url) > 6 else ("已配置" if url else "")
            safe["hasSecret"] = bool(c.get("webhookSecret"))
            self._json(200, {"ok": True, "config": safe})
            return
        if path == "/api/conversations":
            conn = db_conn()
            try:
                rows = conn.execute("SELECT * FROM conversations ORDER BY updated_at DESC").fetchall()
            finally:
                conn.close()
            self._json(200, {"ok": True, "conversations": [dict(r) for r in rows]})
            return
        if path == "/api/messages":
            qs = parse_qs(urlparse(self.path).query)
            cid = (qs.get("conversationId") or [""])[0]
            limit = int((qs.get("limit") or ["100"])[0])
            conn = db_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM messages WHERE conversation_id=? ORDER BY created_at DESC LIMIT ?",
                    (cid, limit),
                ).fetchall()
            finally:
                conn.close()
            self._json(200, {"ok": True, "messages": [dict(r) for r in rows][::-1]})
            return
        if path == "/api/history":
            qs = parse_qs(urlparse(self.path).query)
            t = (qs.get("type") or ["chat"])[0]
            self._json(200, {"ok": True, "data": kv_get("history_" + t, {})})
            return
        if path == "/api/todos":
            todos = todo_list()
            self._json(200, {"ok": True, "todos": todos, "count": len(todos)})
            return
        if path == "/api/schedules":
            self._json(200, {"ok": True, "schedules": schedule_list_active()})
            return
        self._json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        path = self.path.split("?")[0]
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        except Exception:
            body = {}

        if path.startswith("/api"):
            print("[POST]", path)

        if path == "/api/config":
            cfg = body.get("config", {}) or {}
            kv_set("ai_config", cfg)
            print("[config] 收到前端配置，字段:", list(cfg.keys()),
                  "model:", cfg.get("model", ""), "base:", cfg.get("baseUrl", ""))
            self._json(200, {"ok": True})
            return

        if path == "/api/notify-config":
            cfg = body.get("config", {}) or {}
            old = notify_config()
            old.update(cfg)
            kv_set("notify_config", old)
            print("[notify-config] 更新，字段:", list(cfg.keys()),
                  "hasWebhook:", bool(old.get("webhookUrl")))
            self._json(200, {"ok": True})
            return

        if path == "/api/schedules":
            # 直接新增一条调度（供网页/调试用）
            fire = _parse_ts(body.get("fireAt"))
            if not fire:
                self._json(400, {"ok": False, "error": "fireAt invalid"})
                return
            sid = schedule_add(body.get("title") or "提醒", fire,
                               body.get("repeat") or "", body.get("tone") or "",
                               body.get("todoId") or "")
            self._json(200, {"ok": True, "id": sid})
            return

        if path == "/api/conversations":
            conv_upsert(body.get("id", ""), body.get("type", "private"),
                        body.get("name", ""), body.get("meta"))
            self._json(200, {"ok": True})
            return

        if path == "/api/messages":
            msgs = body.get("messages")
            if msgs is None and (body.get("role") or body.get("content") is not None):
                msgs = [body]
            msgs = msgs or []
            n = 0
            for m in msgs:
                cid = m.get("conversationId") or body.get("conversationId") or ""
                if not cid:
                    continue
                msg_add(m.get("id") or ("m_" + str(time.time_ns())), cid,
                        m.get("role", "user"), m.get("content", ""),
                        m.get("sender_id", ""), m.get("sender_name", ""),
                        m.get("image", ""), m.get("meta"),
                        m.get("created_at"))
                n += 1
            self._json(200, {"ok": True, "count": n})
            return

        if path == "/api/chat":
            api_config = body.get("apiConfig") or kv_get("ai_config", {})
            payload = body.get("payload") or {}
            upstream_path = body.get("path") or "/chat/completions"
            cid = body.get("conversationId") or "default"
            try:
                code, raw, upstream_ct = llm_forward(api_config, payload, upstream_path)
            except Exception as e:
                print("[chat] 转发异常:", e, flush=True)
                self._json(502, {"ok": False, "error": "upstream failed: %s" % e})
                return
            text_for_log = raw.decode("utf-8", errors="replace")
            # 落盘：用户消息 + AI 回复（默认关闭，前端显式传 persist=true 才存）
            try:
                if body.get("persist", False):
                    conv_upsert(cid, body.get("type", "private"), body.get("name", ""))
                    last_user = None
                    for m in reversed(payload.get("messages") or []):
                        if m.get("role") == "user":
                            last_user = m
                            break
                    if last_user is not None:
                        content = last_user.get("content")
                        if isinstance(content, list):
                            content = json.dumps(content, ensure_ascii=False)
                        msg_add("u_" + str(time.time_ns()), cid, "user", content)
                    reply = ""
                    if code == 200:
                        try:
                            j = json.loads(text_for_log)
                            reply = (((j.get("choices") or [{}])[0].get("message") or {}).get("content")) or ""
                        except Exception:
                            reply = ""
                        if reply:
                            msg_add("a_" + str(time.time_ns()), cid, "assistant", reply)
                    print("[chat]", cid, payload.get("model", ""), "->", code,
                          (reply[:60].replace("\n", " ") if reply else "(no reply)"), flush=True)
            except Exception as e:
                print("[chat] 落盘失败:", e, flush=True)
            # 审计日志
            try:
                conn = db_conn()
                conn.execute(
                    "INSERT INTO llm_logs(conversation_id,model,request,response,status,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (cid, payload.get("model", ""),
                     json.dumps(payload, ensure_ascii=False)[:20000], text_for_log[:20000], code, time.time()),
                )
                conn.commit()
                conn.close()
            except Exception as e:
                print("[chat] 审计失败:", e, flush=True)
            self._raw(code, raw, upstream_ct)
            return

        if path == "/api/sync":
            if "todos" in body or "deletedIds" in body:
                canonical = todo_sync(body.get("todos", []), body.get("deletedIds", []),
                                      DATA.get("chat_id"))
            else:
                canonical = todo_list()
            if "chatSessions" in body:
                kv_set("history_chat", body["chatSessions"])
            if "groupChats" in body:
                kv_set("history_group", body["groupChats"])
            self._json(200, {"ok": True, "count": len(canonical), "todos": canonical})
            return

        if path == "/api/ack":
            seq = body.get("seq")
            ok = body.get("ok", True)
            result = body.get("result", "")
            print("[ack]", seq, ok, result)
            # 把执行结果回发到飞书
            if result and DATA.get("chat_id"):
                send_feishu(result if ok else "⚠️ " + result)
            self._json(200, {"ok": True})
            return

        self._json(404, {"ok": False, "error": "not found"})

    def log_message(self, *args):
        pass  # 关闭默认访问日志，保持控制台干净


def start_http():
    # 0.0.0.0：服务器部署时允许前端网页从公网访问；本地测试可改回 127.0.0.1
    bind_host = _os.environ.get("FEISHU_BOT_HOST", "0.0.0.0")
    server = ThreadingHTTPServer((bind_host, PORT), Handler)
    print("[http] 轮询服务已启动: http://%s:%d （前端 HTML 的 feishu.baseUrl 要填这个地址）" % (bind_host, PORT))
    server.serve_forever()


# ============ 调度循环（精确到点，到点才醒） ============
def _fire_schedule(s):
    title = s.get("title") or "待办"
    tone = s.get("tone") or ""
    text = None
    if tone:
        text = ai_compose_reminder(title, tone)
    if not text:
        text = "⏰ 提醒：" + title + "\n在飞书回复「完成 " + title + "」即可划掉。"
    notify(text, kind="reminder")
    schedule_mark_fired(s["id"], is_once=((s.get("repeat") or "").strip() in ("", "once")))


def schedule_loop():
    """每轮算出最近的触发点，睡到那一刻；到点则推送。"""
    while True:
        try:
            now = time.time()
            soonest = None
            for s in schedule_list_active():
                if s.get("todo_id") and todo_is_done(s["todo_id"]):
                    schedule_deactivate(s["id"])
                    continue
                nf = _schedule_next_fire(s, now)
                if nf is None:
                    continue
                if nf <= now:
                    is_once = (s.get("repeat") or "").strip() in ("", "once")
                    if now - nf > 3600:
                        # 错过超过 1 小时：直接作废，避免重启后补发一堆积压提醒
                        schedule_mark_fired(s["id"], is_once=is_once)
                        continue
                    _fire_schedule(s)
                elif soonest is None or nf < soonest:
                    soonest = nf
            if soonest is None:
                time.sleep(30)
            else:
                time.sleep(max(3, min(60, soonest - time.time())))
        except Exception as e:
            print("[schedule] 异常:", e)
            time.sleep(30)


# ============ 启动 ============
if __name__ == "__main__":
    init_db()
    load()
    threading.Thread(target=start_http, daemon=True).start()
    threading.Thread(target=schedule_loop, daemon=True).start()
    print("[boot] 星语待办助手 · 飞书桥接服务启动中...")
    print("[boot] 连接飞书长连接...")
    ws_client.start()
