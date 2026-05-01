#!/usr/bin/env python3
"""Partner Test Server - Zero external dependencies.
Uses only Python standard library (http.server + sqlite3 + hashlib + hmac).
"""
import http.server
import json
import sqlite3
import hashlib
import hmac
import base64
import uuid
import os
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs
import urllib.request
import urllib.error

PORT = int(os.environ.get("PORT", "8000"))
DB_PATH = os.path.join(os.path.dirname(__file__), "partner_test.db")
SECRET_KEY = os.environ.get("SECRET_KEY", "partner-test-secret-change-in-prod")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "123123")
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
QUESTION_COUNT_PER_TEST = 12
TOKEN_EXPIRY_HOURS = 72
BAIDU_API_KEY = os.environ.get("BAIDU_API_KEY", "")
BAIDU_SECRET_KEY = os.environ.get("BAIDU_SECRET_KEY", "")

MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".ico": "image/x-icon",
    ".svg": "image/svg+xml",
}

# ── Blocked words for nickname / username ────────────────
# Users cannot use these in their display names.
BLOCKED_WORDS = [
    # === Profanity / vulgar (脏话粗口) ===
    "操你妈", "操你", "操你祖宗", "操你大爷", "操你全家", "操你奶奶",
    "草泥马", "草你妈", "草你",
    "他妈", "特么", "他妈的", "他妈了个逼", "他奶奶",
    "fuck", "fucking", "fuckyou", "fuck you", "shit", "bitch", "asshole",
    "damn", "damnit", "bullshit", "motherfucker", "sonofabitch",
    "傻逼", "煞笔", "傻b", "傻B", "SB", "sb", "傻比", "莎比",
    "尼玛", "你妈", "你妈逼", "你妈b", "你妈的", "你大爷",
    "妈逼", "妈个逼", "妈蛋", "妈勒个逼", "妈的",
    "去死", "滚蛋", "混蛋", "王八蛋", "狗屎", "狗日", "狗日的", "畜生",
    "狗娘养的", "狗杂种", "狗东西",
    "婊子", "婊子养的", "妓女", "荡妇", "骚货", "骚逼",
    "贱人", "贱货", "贱婢", "小贱人",
    "废物", "垃圾", "人渣", "杂种", "野种", "杂碎",
    "蠢货", "蠢猪", "蠢驴", "笨蛋",
    "死全家", "全家死光", "出门被车撞", "不得好死", "天打雷劈",
    "脑残", "脑瘫", "智障", "弱智", "白痴", "神经病",
    "滚", "滚远点", "滚犊子", "滚一边去",
    "操", "干你", "日你", "日你妈", "艹", "肏",
    "魂淡", "八嘎", "八格牙路", "巴嘎",
    "死鬼", "老不死", "王八", "乌龟王八蛋",
    "屁话", "放屁", "胡说八道", "瞎扯",
    "你算老几", "你算什么东西", "什么东西",
    "不要脸", "臭不要脸", "厚颜无耻", "无耻",
    "禽兽", "禽兽不如", "衣冠禽兽",
    "挨千刀的", "杀千刀的", "砍头的",
    "妈的", "姥姥的", "奶奶的",
    "阴险", "卑鄙", "下流", "下贱",

    # === Politics (政治敏感) ===
    "习近平", "习大大", "习主席", "习总", "习总书记",
    "毛主席", "毛泽东", "毛太祖",
    "邓小平", "江泽民", "胡锦涛", "温家宝", "李克强",
    "江泽民", "李鹏", "朱镕基", "吴邦国",
    "共产党", "国民党", "民进党", "法轮功", "法轮",
    "六四", "六四事件", "天安门", "天安门事件",
    "台独", "台湾独立", "藏独", "西藏独立", "疆独", "新疆独立", "港独",
    "香港独立", "蒙古独立",
    "钓鱼岛", "钓鱼岛是中国的",
    "文革", "文化大革命", "八九",
    "政治风波", "学运", "民运",
    "塔利班", "ISIS", "东突", "东突厥斯坦",
    "邪教", "全能神", "呼喊派",
    "复辟", "推翻", "颠覆",
    "敏感词", "禁词", "违禁",
    "禁书", "抗议", "示威", "游行", "暴动", "暴乱",
    "特务", "间谍", "卖国贼", "汉奸",
    "独裁", "专制", "暴政",
    "镇压", "屠杀", "血腥镇压",
    "新冠", "新冠病毒", "武汉肺炎",
    "方方", "李医生", "李文亮",
    "PX项目", "群体事件", "上访",
    "三光", "大屠杀", "南京大屠杀",
    "天皇", "靖国神社", "神社",
    "占领", "侵略", "侵占",
    "纳粹", "法西斯", "军国主义",
    "宗教迫害", "政治迫害",
    "驱逐", "流放", "劳改",
    "集中营", "劳教",

    # === Racial / discriminatory (种族歧视) ===
    "黑鬼", "黑佬", "黑奴",
    "白皮猪", "白鬼",
    "支那", "支那人",
    "东亚病夫",
    "尼哥", "nigger", "nigga",
    "阿三", "印度阿三",
    "棒子", "高丽棒子", "韩国棒子",
    "鬼子", "日本鬼子", "小日本", "日寇",
    "台巴子",
    "港灿",
    "洋鬼子", "洋奴",
    "红脖子", "redneck",
    "蝗虫",
    "难民",
    "绿绿", "msl", "穆斯林",
    "犹太", "犹太人",
    "吉普赛",

    # === Vulgar sexual (色情露骨) ===
    "色情", "黄色", "黄片", "黄网",
    "裸", "裸体", "裸照", "裸聊", "一丝不挂",
    "自慰", "手淫", "打飞机", "打手枪",
    "做爱", "性交", "交配",
    "鸡巴", "鸡吧", "阴茎", "龟头",
    "阴道", "阴蒂", "阴唇", "阴部",
    "阳具", "阴茎",
    "精液", "精子", "射精",
    "乳房", "奶子", "咪咪", "胸部", "乳沟", "乳晕",
    "乳头", "乳头",
    "屁股", "肛门", "屁眼", "菊花",
    "春药", "催情", "迷药", "迷奸药", "催情药",
    "强奸", "轮奸", "迷奸", "强暴",
    "卖淫", "嫖", "嫖娼", "嫖客",
    "招嫖", "包夜", "出台", "上门服务",
    "约炮", "约啪", "约p",
    "SM", "s_m", "性虐", "虐待",
    "乱伦", "近亲",
    "幼齿", "萝莉", "正太",
    "三级片", "A片", "AV", "av",
    "成人电影", "成人网站", "成人视频",
    "情色", "情色电影",
    "一夜情", "一夜情",
    "裸聊", "视频裸聊",
    "同城约", "同城交友",
    "二奶", "小三", "包养", "情妇", "情夫",
    "处女", "破处",
    "性服务", "性交易", "性工作者",
    "同志", "gay", "les", "同性恋",
    "双性", "人妖",
    "口交", "肛交", "乳交",
    "群交", "杂交",
    "艳照", "裸照",
    "三级", "十八禁", "18禁",
    "色图", "色片", "色文",
    "成人", "色色",
    "偷拍", "走光", "漏点",
    "调教", "奴",

    # === Spam / scam (广告诈骗) ===
    "加微信", "加QQ", "加VX",
    "微信号", "QQ号", "VX号",
    "兼职", "日赚", "月入", "月入过万",
    "日结", "时薪", "高薪",
    "刷单", "刷信誉", "刷钻",
    "赌博", "赌场", "赌",
    "彩票", "六合彩", "时时彩", "福彩",
    "投资", "理财", "稳赚", "包赚",
    "返利", "返现", "回扣",
    "传销", "直销", "拉人头",
    "代理", "加盟", "招商",
    "微商", "代购", "代购",
    "淘宝", "天猫", "京东",
    "优惠券", "代金券", "折扣",
    "免费领取", "免费送", "免费试用",
    "中奖", "恭喜中奖", "幸运用户",
    "抽奖", "抽奖",
    "红包", "现金红包",
    "收款", "付款", "转账",
    "银行卡", "信用卡", "卡号",
    "验证码", "密码",
    "客服", "售后",
    "贷款", "借贷", "小额贷款", "无抵押",
    "催收", "追债",
    "股票", "炒股", "荐股",
    "外汇", "期货", "原油",
    "挖矿", "比特币", "区块链", "数字货币",
    "资金盘", "庞氏",

    # === URL / contact (联系方式) ===
    "https://", "http://", "www.",
    ".com", ".cn", ".net", ".org", ".top", ".xyz", ".cc", ".vip",
    "qq.com", "weixin", "wechat",
    "alipay", "支付宝",
    "电话", "手机号", "手机号码",
    "邮箱", "email", "e-mail",

    # === Violence / weapons (暴力武器) ===
    "杀人", "自杀", "谋杀", "凶杀",
    "碎尸", "分尸", "肢解",
    "爆炸", "炸弹", "炸药",
    "枪支", "手枪", "步枪", "冲锋枪", "狙击",
    "刀具", "管制刀具",
    "砍人", "捅人",
    "绑架", "劫持",
    "恐怖", "恐怖袭击", "恐怖分子",
    "人体炸弹", "汽车炸弹",
    "毒药", "剧毒", "砒霜",
    "纵火", "放火",
    "复仇", "报仇",
    "黑社会", "黑帮", "黑道",
    "火拼", "械斗", "群殴",
    "暴力", "血腥", "残忍",
    "死刑", "枪决", "注射死刑",
    "伤人", "故意伤害",
    "暗杀", "刺杀",
    "屠杀", "灭门",

    # === Drug / tobacco / alcohol (毒品烟酒) ===
    "毒品", "吸毒", "贩毒",
    "冰毒", "海洛因", "大麻", "可卡因",
    "摇头丸", "麻古", "k粉",
    "鸦片", "吗啡", "杜冷丁",
    "罂粟",
    "香烟", "烟草", "吸烟",
    "戒烟", "电子烟",
    "烈酒", "白酒",
    "酗酒", "醉酒",

    # === School bullying / insult (校园/人身攻击) ===
    "校霸", "校园暴力",
    "胖子", "死胖子", "肥猪",
    "矮子", "矬子",
    "四眼", "四眼田鸡",
    "书呆子",
    "丑八怪", "丑逼",
    "穷逼", "穷鬼", "穷光蛋",
    "土鳖", "土包子",
    "乡巴佬",
    "菜鸟", "弱鸡", "垃圾",
    "卢瑟", "loser",
    "吊丝", "屌丝",
    "直男癌", "直女癌",
    "剩女", "大龄剩女",
    "光棍", "单身狗",
    "接盘侠", "舔狗",
    "绿茶婊", "圣母婊", "心机婊",
    "白莲花",
    "渣男", "渣女",
    "妈宝", "妈宝男",
    "凤凰男", "孔雀女",
    "玻璃心",
    "巨婴", "巨婴男",
    # === Threat / intimidation (威胁恐吓) ===
    "弄死", "打死你", "杀了你", "砍死", "捅死",
    "一巴掌", "呼死你", "拍死你", "抽死你",
    "信不信我", "你等着", "你给我等着",
    "找人弄你", "找人群殴", "找人打你",
    # === Common profanity variants (常见脏话变体) ===
    "装逼", "装B", "装b", "装13",
    "老子", "老娘",
    "叫爸爸", "叫爷爷",
    "跪下来", "跪下叫",
    "孙子", "龟孙",
    "放狗屁", "扯淡",
    "有病", "有病吧",
    "恶心", "恶心人",
    "要脸吗", "要脸不",
    "配吗", "你也配",
    "惯的你", "惯的",
    "不知好歹", "给脸不要",
]

# ── Baidu Cloud content review (text censor) ────────────
# Uses Baidu AI Cloud text moderation API when credentials are configured.
# https://cloud.baidu.com/doc/ANTIPORN/s/nkfb6u3bi

_baidu_token = None
_baidu_token_expiry = 0


def _baidu_get_token():
    """Obtain Baidu API access token. Cached for 25 days."""
    global _baidu_token, _baidu_token_expiry
    now = datetime.now(timezone.utc).timestamp()
    if _baidu_token and now < _baidu_token_expiry:
        return _baidu_token
    url = ("https://aip.baidubce.com/oauth/2.0/token"
           f"?client_id={BAIDU_API_KEY}&client_secret={BAIDU_SECRET_KEY}&grant_type=client_credentials")
    data = json.dumps("").encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            token = result.get("access_token")
            if token:
                _baidu_token = token
                _baidu_token_expiry = now + 25 * 86400
                return token
    except Exception:
        pass
    return None


def _baidu_text_censor(text):
    """Call Baidu text censor API. Returns (is_valid, reason)."""
    token = _baidu_get_token()
    if not token:
        print(f"[BAIDU] 获取 token 失败，跳过 API 检测")
        return None, None
    url = f"https://aip.baidubce.com/rest/2.0/solution/v1/text_censor/v2/user_defined?access_token={token}"
    body = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            ct = result.get("conclusionType", 1)
            print(f"[BAIDU] 检测文本: {text!r} → conclusionType={ct} ({result.get('conclusion', 'unknown')})")
            if ct == 1:
                return True, None
            data = result.get("data", [])
            reason = data[0]["msg"] if data else "审核不通过"
            return False, reason
    except Exception as e:
        print(f"[BAIDU] API 调用异常: {e}")
        return None, None


def validate_text(text):
    """Check text via local blocked words + Baidu API. Returns (is_valid, reason)."""
    # 1) Local blocked words first (catches profanity Baidu may miss)
    lower = text.lower()
    for word in BLOCKED_WORDS:
        if word.lower() in lower:
            print(f"[VALIDATE] 本地词库拦截: {text!r} → 命中词: {word}")
            return False, word
    # 2) Baidu API (if configured) — catches things our list misses
    if BAIDU_API_KEY and BAIDU_SECRET_KEY:
        print(f"[VALIDATE] 本地词库通过，调用百度 API 检测: {text!r}")
        ok, reason = _baidu_text_censor(text)
        if ok is not None:
            if ok:
                print(f"[VALIDATE] 百度 API 通过: {text!r}")
                return True, None
            print(f"[VALIDATE] 百度 API 拦截: {text!r} → {reason}")
            return False, reason
        print(f"[VALIDATE] 百度 API 不可用，放行: {text!r}")
    else:
        print(f"[VALIDATE] 未配置百度 API（BAIDU_API_KEY 为空），仅走本地词库: {text!r}")
    return True, None

# ── Database ──────────────────────────────────────────────
# Supports both SQLite (local dev) and PostgreSQL (Railway).
# When DATABASE_URL is set, uses PostgreSQL via psycopg2.

# Lazy-load psycopg2 only when DATABASE_URL is present
_psycopg2 = None


def _get_psycopg2():
    global _psycopg2
    if _psycopg2 is None:
        import psycopg2 as p
        import psycopg2.extras
        _psycopg2 = p
    return _psycopg2


def get_db():
    db_url = os.environ.get("DATABASE_URL", "")
    if db_url:
        pg = _get_psycopg2()
        conn = pg.connect(db_url)
        conn.autocommit = True
        return _PgConnection(conn, pg)
    import sqlite3 as _sq
    conn = _sq.connect(DB_PATH)
    conn.row_factory = _sq.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class _PgRow:
    """Behaves like sqlite3.Row: supports both row[0] and row['name']."""
    def __init__(self, keys, values):
        self._keys = keys
        self._values = values
        self._key_map = {k: i for i, k in enumerate(keys)}
    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        return self._values[self._key_map[key]]
    def __len__(self):
        return len(self._values)
    def keys(self):
        return self._keys


class _PgCursor:
    """Wraps a RealDictCursor to behave like sqlite3 cursor results."""
    def __init__(self, cur):
        self._cur = cur
        self._keys = [d.name for d in cur.description] if cur.description else []
    def fetchone(self):
        r = self._cur.fetchone()
        if r is not None:
            return _PgRow(self._keys, [r[k] for k in self._keys])
        return None
    def fetchall(self):
        rows = self._cur.fetchall()
        return [_PgRow(self._keys, [r[k] for k in self._keys]) for r in rows]


class _PgConnection:
    """Wraps a psycopg2 connection to mimic sqlite3.Connection."""
    def __init__(self, conn, pg):
        self._conn = conn
        self._pg = pg
    def execute(self, sql, params=None):
        cur = self._conn.cursor(cursor_factory=self._pg.extras.RealDictCursor)
        pg_sql = sql
        had_ignore = "OR IGNORE" in pg_sql
        pg_sql = pg_sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        pg_sql = pg_sql.replace("?", "%s")
        if had_ignore:
            pg_sql += " ON CONFLICT DO NOTHING"
        if params:
            cur.execute(pg_sql, params)
        else:
            cur.execute(pg_sql)
        return _PgCursor(cur)
    def executescript(self, sql):
        cur = self._conn.cursor()
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                pg_stmt = stmt.replace("?", "%s")
                cur.execute(pg_stmt)
        cur.close()
    def commit(self):
        pass  # autocommit
    def close(self):
        self._conn.close()


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'contributor',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tags (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS questions (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            options TEXT NOT NULL,
            dimension TEXT,
            weight REAL DEFAULT 1.0,
            time_limit INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            submitter_id TEXT REFERENCES users(id),
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS question_tags (
            question_id TEXT REFERENCES questions(id),
            tag_id TEXT REFERENCES tags(id),
            PRIMARY KEY (question_id, tag_id)
        );
        CREATE TABLE IF NOT EXISTS test_records (
            id TEXT PRIMARY KEY,
            answers TEXT NOT NULL,
            surface_score REAL NOT NULL,
            real_score REAL NOT NULL,
            token TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS question_skips (
            id TEXT PRIMARY KEY,
            question_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS admin_logs (
            id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            ip TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tester_nicknames (
            token TEXT PRIMARY KEY,
            nickname TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS question_bank_log (
            id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            total INTEGER NOT NULL DEFAULT 0,
            approved INTEGER NOT NULL DEFAULT 0,
            pending INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
    """)
    # Add banned column to users if not exists
    try:
        conn.execute("ALTER TABLE users ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    # Add username column to users if not exists
    try:
        conn.execute("ALTER TABLE users ADD COLUMN username TEXT NOT NULL DEFAULT ''")
    except Exception:
        pass
    # Ensure test_uploader user exists for test-login feature
    conn.execute(
        "INSERT OR IGNORE INTO users (id, email, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?)",
        ("test_uploader", "test@local.dev", "", "user", datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
    )
    conn.commit()
    conn.close()


def _log_question_bank(conn, action):
    """Record current question bank stats after each add/delete/review."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    total = conn.execute("SELECT COUNT(*) as c FROM questions").fetchone()["c"]
    approved = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
    pending = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='pending'").fetchone()["c"]
    rejected = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='rejected'").fetchone()["c"]
    lid = uuid.uuid4().hex[:8]
    conn.execute(
        "INSERT INTO question_bank_log (id, action, total, approved, pending, rejected, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (lid, action, total, approved, pending, rejected, now)
    )


# ── Auth helpers ──────────────────────────────────────────

def hash_password(password):
    salt = os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return base64.b64encode(salt).decode() + ":" + base64.b64encode(h).decode()

def verify_password(password, stored):
    try:
        salt_b64, hash_b64 = stored.split(":")
        salt = base64.b64decode(salt_b64)
        stored_hash = base64.b64decode(hash_b64)
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
        return hmac.compare_digest(h, stored_hash)
    except Exception:
        return False

def make_token(user_id, role):
    exp = int((datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRY_HOURS)).timestamp())
    payload = json.dumps({"uid": user_id, "role": role, "exp": exp}, separators=(",", ":")).encode()
    sig = hmac.new(SECRET_KEY.encode(), payload, hashlib.sha256).hexdigest()
    data = json.dumps({"p": payload.decode(), "sig": sig})
    return base64.urlsafe_b64encode(data.encode()).decode()

def decode_token(token):
    try:
        data = json.loads(base64.urlsafe_b64decode(token.encode()).decode())
        payload = json.loads(data["p"])
        expected = hmac.new(SECRET_KEY.encode(), data["p"].encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, data["sig"]):
            return None
        if payload["exp"] < datetime.now(timezone.utc).timestamp():
            return None
        return payload
    except Exception:
        return None

def get_current_user(headers):
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    return decode_token(token)

def require_auth(headers):
    user = get_current_user(headers)
    if not user:
        return None, error_response("Unauthorized", 401)
    return user, None

def require_admin(headers):
    user = get_current_user(headers)
    if not user:
        return None, error_response("Unauthorized", 401)
    if user.get("role") != "admin":
        return None, error_response("Admin only", 403)
    return user, None

# ── Crypto for test results ──────────────────────────────

def generate_token(record_id, real_score):
    ts = int(datetime.now(timezone.utc).timestamp())
    payload = f"{record_id}|{real_score}|{ts}"
    sig = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    data = json.dumps({"rid": record_id, "rs": real_score, "t": ts, "sig": sig})
    return base64.urlsafe_b64encode(data.encode()).decode()

def decode_record_token(token):
    try:
        data = json.loads(base64.urlsafe_b64decode(token.encode()).decode())
        payload = f"{data['rid']}|{data['rs']}|{data['t']}"
        expected = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected, data["sig"]):
            return data
    except Exception:
        pass
    return None

def generate_short_code(real_score):
    """Simple base64-encoded short string for screenshot verification."""
    data = json.dumps({"s": real_score}, separators=(",", ":"))
    return base64.urlsafe_b64encode(data.encode()).decode()

# ── JSON response helpers ─────────────────────────────────

def json_response(data, status=200):
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    return (status, body, "application/json; charset=utf-8")

def error_response(msg, status=400):
    return json_response({"detail": msg}, status)

def read_body(handler):
    length = int(handler.headers.get("Content-Length", 0))
    if length == 0:
        return None
    return json.loads(handler.rfile.read(length).decode("utf-8"))

# ── API Handlers ──────────────────────────────────────────

def handle_register(body):
    email = body.get("email", "").strip()
    password = body.get("password", "")
    username = body.get("username", "").strip() or email.split("@")[0]
    valid, bad_word = validate_text(username)
    if not valid:
        return error_response(f"用户名包含敏感词「{bad_word}」，请修改")
    if not email or not password:
        return error_response("Email and password required")
    if len(password) < 6:
        return error_response("Password too short")
    conn = get_db()
    try:
        existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if existing:
            return error_response("Email already registered", 400)
        uid = uuid.uuid4().hex[:8]
        conn.execute(
            "INSERT INTO users (id, email, password_hash, role, created_at, username) VALUES (?, ?, ?, ?, ?, ?)",
            (uid, email, hash_password(password), "contributor", datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'), username)
        )
        conn.commit()
        token = make_token(uid, "contributor")
        return json_response({"access_token": token, "token_type": "bearer"})
    finally:
        conn.close()

def handle_login(body):
    email = body.get("email", "").strip()
    password = body.get("password", "")
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not row or not verify_password(password, row["password_hash"]):
            return error_response("Invalid credentials", 401)
        if row.get("banned"):
            return error_response("此账号已被封禁，无法登录", 403)
        token = make_token(row["id"], row["role"])
        return json_response({"access_token": token, "token_type": "bearer", "username": row["username"] or ""})
    finally:
        conn.close()

def handle_me(headers):
    user, err = require_auth(headers)
    if err:
        return err
    conn = get_db()
    try:
        row = conn.execute("SELECT id, email, role, created_at, username FROM users WHERE id = ?", (user["uid"],)).fetchone()
        if not row:
            return error_response("User not found", 404)
        return json_response({
            "id": row["id"], "email": row["email"],
            "role": row["role"], "created_at": row["created_at"],
            "username": row["username"] or ""
        })
    finally:
        conn.close()

def handle_test_login(headers, body):
    # Create a unique user each time so each uploader can set their own nickname
    uid = "test_" + uuid.uuid4().hex[:8]
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (id, email, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?)",
            (uid, f"test_{uuid.uuid4().hex[:6]}@local.dev", "", "test",
             datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
    finally:
        conn.close()
    token = make_token(uid, "user")
    return json_response({"access_token": token, "token_type": "bearer", "test_mode": True})

def handle_set_nickname(headers, body):
    user, err = require_auth(headers)
    if err:
        return err
    nickname = body.get("nickname", "").strip()
    if not nickname or len(nickname) > 20:
        return error_response("昵称无效（1-20字）")
    valid, bad_word = validate_text(nickname)
    if not valid:
        return error_response(f"昵称包含敏感词「{bad_word}」，请修改")
    conn = get_db()
    try:
        conn.execute("UPDATE users SET username = ? WHERE id = ?", (nickname, user["uid"]))
        conn.commit()
        return json_response({"status": "ok"})
    finally:
        conn.close()

def handle_get_questions(headers, query):
    count = int(query.get("count", [QUESTION_COUNT_PER_TEST])[0])
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM questions WHERE status = 'approved' ORDER BY RANDOM() LIMIT ?",
            (count,)
        ).fetchall()
        result = []
        for r in rows:
            tags = [t["name"] for t in conn.execute(
                "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
                (r["id"],)
            ).fetchall()]
            result.append({
                "id": r["id"],
                "content": r["content"],
                "options": json.loads(r["options"]),
                "dimension": r["dimension"],
                "weight": r["weight"],
                "time_limit": r["time_limit"],
                "status": r["status"],
                "tags": tags,
                "created_at": r["created_at"],
            })
        return json_response(result)
    finally:
        conn.close()

def handle_create_question(headers, body):
    user, err = require_auth(headers)
    if err:
        return err
    content = body.get("content", "").strip()
    options = body.get("options", [])
    tags = body.get("tags", [])
    dimension = body.get("dimension")
    weight = body.get("weight", 1.0)
    time_limit = body.get("time_limit", 0)
    if not content or len(options) < 2:
        return error_response("Content and at least 2 options required")
    conn = get_db()
    try:
        qid = uuid.uuid4().hex[:8]
        conn.execute(
            "INSERT INTO questions (id, content, options, dimension, weight, time_limit, status, submitter_id, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (qid, content, json.dumps(options, ensure_ascii=False), dimension, weight, time_limit, user["uid"], datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        for tag_name in tags:
            tag_name = tag_name.strip()
            if not tag_name:
                continue
            existing = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
            if existing:
                tid = existing["id"]
            else:
                tid = uuid.uuid4().hex[:8]
                conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
            conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
        conn.commit()
        _log_question_bank(conn, "create")
        return json_response({"id": qid, "status": "pending"}, 201)
    finally:
        conn.close()

def handle_pending_questions(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM questions WHERE status = 'pending' ORDER BY created_at DESC").fetchall()
        result = []
        for r in rows:
            tags = [t["name"] for t in conn.execute(
                "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
                (r["id"],)
            ).fetchall()]
            result.append({
                "id": r["id"],
                "content": r["content"],
                "options": json.loads(r["options"]),
                "dimension": r["dimension"],
                "weight": r["weight"],
                "time_limit": r["time_limit"],
                "status": r["status"],
                "submitter_id": r["submitter_id"],
                "tags": tags,
                "created_at": r["created_at"],
            })
        return json_response(result)
    finally:
        conn.close()

def handle_review_question(headers, qid, body):
    user, err = require_admin(headers)
    if err:
        return err
    status = body.get("status")
    if status not in ("approved", "rejected"):
        return error_response("Invalid status")
    conn = get_db()
    try:
        row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        conn.execute("UPDATE questions SET status = ? WHERE id = ?", (status, qid))
        conn.commit()
        _log_question_bank(conn, "review")
        return json_response({"id": qid, "status": status})
    finally:
        conn.close()

def handle_submit_test(headers, body):
    answers = body.get("answers", [])
    if not answers:
        return error_response("No answers provided")
    real_score = 0.0
    conn = get_db()
    try:
        for ans in answers:
            qid = ans.get("question_id")
            idx = ans.get("selected_index", -1)
            behavior = ans.get("behavior", "normal")
            time_taken = ans.get("time_taken", 0)
            row = conn.execute("SELECT * FROM questions WHERE id = ? AND status = 'approved'", (qid,)).fetchone()
            if not row:
                continue
            w = row["weight"]
            time_limit = row["time_limit"]
            if behavior == "complaint":
                real_score += 15.0 * w
                sid = uuid.uuid4().hex[:8]
                conn.execute(
                    "INSERT INTO question_skips (id, question_id, reason, created_at) VALUES (?, ?, 'complaint', ?)",
                    (sid, qid, datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
                )
            elif time_limit > 0 and time_taken > time_limit + 2:
                real_score += 10.0 * w
            else:
                options = json.loads(row["options"])
                if 0 <= idx < len(options):
                    real_score += options[idx].get("score", 0) * w
        real_score = round(real_score, 1)
        surface_score = round(max(0.0, 100.0 - real_score), 1)
        rid = uuid.uuid4().hex[:8]
        token = generate_token(rid, real_score)
        conn.execute(
            "INSERT INTO test_records (id, answers, surface_score, real_score, token, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (rid, json.dumps(answers, ensure_ascii=False), surface_score, real_score, token, datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
        return json_response({
            "record_id": rid,
            "surface_score": surface_score,
            "short_code": generate_short_code(real_score),
            "real_token": token,
            "created_at": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
    finally:
        conn.close()

def handle_verify(headers, record_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM test_records WHERE id = ?", (record_id,)).fetchone()
        if not row:
            return error_response("Record not found", 404)
        decoded = decode_record_token(row["token"])
        is_authentic = decoded is not None and decoded["rs"] == row["real_score"]
        return json_response({
            "record_id": row["id"],
            "real_score": row["real_score"],
            "surface_score": row["surface_score"],
            "is_authentic": is_authentic,
            "created_at": row["created_at"],
        })
    finally:
        conn.close()

def handle_verify_by_token(headers, query):
    token = query.get("token", [None])[0]
    if not token:
        return error_response("Token required", 400)
    decoded = decode_record_token(token)
    if not decoded:
        return json_response({"is_authentic": False})
    # Token is valid — look up record for created_at
    conn = get_db()
    try:
        row = conn.execute("SELECT created_at FROM test_records WHERE id = ?", (decoded["rid"],)).fetchone()
        return json_response({
            "is_authentic": True,
            "real_score": decoded["rs"],
            "created_at": row["created_at"] if row else None,
        })
    finally:
        conn.close()

def handle_get_record(headers, record_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM test_records WHERE id = ?", (record_id,)).fetchone()
        if not row:
            return error_response("Record not found", 404)
        return json_response({
            "record_id": row["id"],
            "real_score": row["real_score"],
            "surface_score": row["surface_score"],
            "short_code": generate_short_code(row["real_score"]),
            "token": row["token"],
            "created_at": row["created_at"],
        })
    finally:
        conn.close()

def handle_get_replacement(headers, query):
    exclude = query.get("exclude", [""])[0]
    exclude_ids = [e for e in exclude.split(",") if e] if exclude else []
    conn = get_db()
    try:
        if exclude_ids:
            ph = ",".join("?" for _ in exclude_ids)
            row = conn.execute(f"SELECT * FROM questions WHERE status = 'approved' AND id NOT IN ({ph}) ORDER BY RANDOM() LIMIT 1", exclude_ids).fetchone()
        else:
            row = conn.execute("SELECT * FROM questions WHERE status = 'approved' ORDER BY RANDOM() LIMIT 1").fetchone()
        if not row:
            return error_response("No replacement available", 404)
        tags = [t["name"] for t in conn.execute(
            "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
            (row["id"],)
        ).fetchall()]
        return json_response({
            "id": row["id"],
            "content": row["content"],
            "options": json.loads(row["options"]),
            "dimension": row["dimension"],
            "weight": row["weight"],
            "tags": tags,
        })
    finally:
        conn.close()

def handle_record_skip(headers, body):
    question_id = body.get("question_id", "")
    reason = body.get("reason", "skip")
    if reason not in ("skip", "complaint"):
        return error_response("Invalid reason", 400)
    conn = get_db()
    try:
        sid = uuid.uuid4().hex[:8]
        conn.execute(
            "INSERT INTO question_skips (id, question_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (sid, question_id, reason, datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
        return json_response({"id": sid})
    finally:
        conn.close()

def handle_get_complaints(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        # Get all unique question_ids that have complaints, with counts
        rows = conn.execute(
            "SELECT qs.question_id, COUNT(*) as cnt, q.content as q_content "
            "FROM question_skips qs JOIN questions q ON qs.question_id = q.id "
            "WHERE qs.reason = 'complaint' "
            "GROUP BY qs.question_id, q.content "
            "ORDER BY cnt DESC"
        ).fetchall()

        # Count how many times each question appeared in tests
        # test_records.answers is a JSON array of {question_id, ...}
        answer_counts = {}
        all_records = conn.execute("SELECT answers FROM test_records").fetchall()
        for rec in all_records:
            seen = set()
            try:
                ans_list = json.loads(rec["answers"])
                for a in ans_list:
                    qid = a.get("question_id")
                    if qid:
                        seen.add(qid)
            except (json.JSONDecodeError, TypeError):
                pass
            for qid in seen:
                answer_counts[qid] = answer_counts.get(qid, 0) + 1

        result = []
        for r in rows:
            qid = r["question_id"]
            complaint_cnt = r["cnt"]
            answered_cnt = answer_counts.get(qid, 0)
            ratio = round(complaint_cnt / answered_cnt, 4) if answered_cnt > 0 else 0
            result.append({
                "question_id": qid,
                "question_content": r["q_content"],
                "complaint_count": complaint_cnt,
                "answered_count": answered_cnt,
                "ratio": ratio,
            })
        return json_response(result)
    finally:
        conn.close()

def handle_delete_question(headers, qid):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM question_skips WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM questions WHERE id = ?", (qid,))
        conn.commit()
        _log_question_bank(conn, "delete")
        log_admin_action(headers, "delete_question", qid)
        return json_response({"id": qid, "status": "deleted"})
    finally:
        conn.close()


# ── Admin delete with password ──────────────────────

def handle_admin_delete_question(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("管理员密码错误", 401)
    qid = body.get("id", "")
    if not qid:
        return error_response("No ID provided", 400)
    conn = get_db()
    try:
        row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM question_skips WHERE question_id = ?", (qid,))
        conn.execute("DELETE FROM questions WHERE id = ?", (qid,))
        conn.commit()
        _log_question_bank(conn, "delete")
        log_admin_action(headers, "delete_question", qid)
        return json_response({"id": qid, "status": "deleted"})
    finally:
        conn.close()

def handle_admin_batch_delete(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("管理员密码错误", 401)
    ids = body.get("ids", [])
    if not ids:
        return error_response("No IDs provided", 400)
    conn = get_db()
    try:
        deleted = 0
        for qid in ids:
            row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
            if not row:
                continue
            conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
            conn.execute("DELETE FROM question_skips WHERE question_id = ?", (qid,))
            conn.execute("DELETE FROM questions WHERE id = ?", (qid,))
            deleted += 1
        conn.commit()
        _log_question_bank(conn, "batch_delete")
        log_admin_action(headers, "batch_delete_questions", f"Deleted {deleted} questions")
        return json_response({"deleted": deleted})
    finally:
        conn.close()

# ── Batch set time_limit ──────────────────────────────

def handle_admin_batch_set_timelimit(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("管理员密码错误", 401)
    ids = body.get("ids", [])
    time_limit = body.get("time_limit", 0)
    if not ids:
        return error_response("No IDs provided", 400)
    if not isinstance(time_limit, int) or time_limit < 0:
        return error_response("Invalid time_limit", 400)
    conn = get_db()
    try:
        updated = 0
        for qid in ids:
            row = conn.execute("SELECT id FROM questions WHERE id = ?", (qid,)).fetchone()
            if not row:
                continue
            conn.execute("UPDATE questions SET time_limit = ? WHERE id = ?", (time_limit, qid))
            updated += 1
        conn.commit()
        _log_question_bank(conn, "batch_set_timelimit")
        log_admin_action(headers, "batch_set_timelimit", f"Set time_limit={time_limit} for {updated} questions")
        return json_response({"updated": updated})
    finally:
        conn.close()

# ── Contributors ─────────────────────────────────────

def handle_contributors(headers):
    conn = get_db()
    try:
        sort = parse_qs(urlparse(headers.get("X-Original-URL", "")).query).get("sort", ["time"])[0]

        # Uploaders: users who submitted approved questions
        uploaders = conn.execute(
            "SELECT u.username, u.email, COUNT(q.id) as qcount, MAX(q.created_at) as last_time "
            "FROM users u INNER JOIN questions q ON q.submitter_id = u.id "
            "WHERE q.status = 'approved' "
            "GROUP BY u.id"
        ).fetchall()

        result = []
        for r in uploaders:
            name = r["username"] or r["email"].split("@")[0]
            result.append({
                "username": name,
                "count": r["qcount"],
                "time": r["last_time"]
            })

        if sort == "count":
            result.sort(key=lambda x: -x["count"])
        else:
            result.sort(key=lambda x: x.get("time", ""), reverse=True)

        return json_response(result)
    finally:
        conn.close()

def handle_set_tester_nickname(body):
    try:
        data = json.loads(body)
        token = data.get("token", "").strip()
        nickname = data.get("nickname", "").strip()
        if not token or not nickname:
            return json_response({"detail": "缺少 token 或昵称"}, 400)
        if len(nickname) > 20:
            return json_response({"detail": "昵称最长 20 个字符"}, 400)
        conn = get_db()
        try:
            now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            conn.execute(
                "INSERT OR REPLACE INTO tester_nicknames (token, nickname, created_at) VALUES (?, ?, ?)",
                (token, nickname, now)
            )
            conn.commit()
            return json_response({"ok": True})
        finally:
            conn.close()
    except json.JSONDecodeError:
        return json_response({"detail": "无效的请求"}, 400)

def handle_public_stats(headers, body, *args):
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM test_records").fetchone()["c"]
        return json_response({"total_tests": total})
    finally:
        conn.close()

# ── Admin all questions ─────────────────────────────

def handle_all_questions(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM questions ORDER BY created_at DESC").fetchall()
        result = []
        for r in rows:
            tags = [t["name"] for t in conn.execute(
                "SELECT t.name FROM tags t JOIN question_tags qt ON t.id = qt.tag_id WHERE qt.question_id = ?",
                (r["id"],)
            ).fetchall()]
            # Get submitter info
            submitter = None
            if r["submitter_id"]:
                u = conn.execute("SELECT username, email FROM users WHERE id = ?", (r["submitter_id"],)).fetchone()
                if u:
                    submitter = u["username"] or u["email"].split("@")[0]
            result.append({
                "id": r["id"],
                "content": r["content"],
                "options": json.loads(r["options"]),
                "dimension": r["dimension"],
                "weight": r["weight"],
                "time_limit": r["time_limit"],
                "status": r["status"],
                "submitter": submitter,
                "tags": tags,
                "created_at": r["created_at"],
            })
        return json_response(result)
    finally:
        conn.close()

# ── Admin question edit ──────────────────────────────

def handle_edit_question(headers, qid, body):
    # Verify admin password again before allowing edit
    user, err = require_admin(headers)
    if err:
        return err
    password = body.get("password", "")
    if not verify_admin_password(password):
        return error_response("Admin password required", 401)
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (qid,)).fetchone()
        if not row:
            return error_response("Question not found", 404)
        content = body.get("content", row["content"])
        options = body.get("options", json.loads(row["options"]))
        dimension = body.get("dimension", row["dimension"])
        weight = body.get("weight", row["weight"])
        time_limit = body.get("time_limit", row["time_limit"])
        conn.execute(
            "UPDATE questions SET content=?, options=?, dimension=?, weight=?, time_limit=? WHERE id=?",
            (content, json.dumps(options, ensure_ascii=False), dimension, weight, time_limit, qid)
        )
        # Handle tags update
        if "tags" in body:
            conn.execute("DELETE FROM question_tags WHERE question_id = ?", (qid,))
            for tag_name in body["tags"]:
                tag_name = tag_name.strip()
                if not tag_name:
                    continue
                existing = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
                if existing:
                    tid = existing["id"]
                else:
                    tid = uuid.uuid4().hex[:8]
                    conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
                conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
        conn.commit()
        return json_response({"id": qid, "status": "updated"})
    finally:
        conn.close()

# ── Admin auth & stats ──────────────────────────────

ADMIN_PASSWORD_HASH = os.environ.get("ADMIN_PASSWORD_HASH", "")
# Rate limiting for admin login
_admin_login_attempts = {}  # ip -> [timestamp, ...]
ADMIN_RATE_LIMIT = 5        # max attempts
ADMIN_RATE_WINDOW = 300     # 5 minutes

def check_admin_rate_limit(ip):
    now = datetime.now(timezone.utc).timestamp()
    attempts = _admin_login_attempts.get(ip, [])
    # Remove expired entries
    attempts = [t for t in attempts if now - t < ADMIN_RATE_WINDOW]
    _admin_login_attempts[ip] = attempts
    return len(attempts) >= ADMIN_RATE_LIMIT

def record_admin_attempt(ip):
    _admin_login_attempts.setdefault(ip, []).append(datetime.now(timezone.utc).timestamp())

def verify_admin_password(password):
    # 1. Check DB-stored hash first (set via change-password API)
    try:
        conn = get_db()
        row = conn.execute("SELECT value FROM admin_config WHERE key='password_hash'").fetchone()
        if row:
            return verify_password(password, row["value"])
        conn.close()
    except Exception:
        pass
    # 2. Fallback: env var or default 123123
    if not ADMIN_PASSWORD_HASH:
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), b"admin_salt", 100000)
        expected = base64.b64encode(h).decode()
        return expected == base64.b64encode(hashlib.pbkdf2_hmac("sha256", b"123123", b"admin_salt", 100000)).decode()
    return verify_password(password, ADMIN_PASSWORD_HASH)

def handle_admin_auth(headers, body):
    ip = headers.get("X-Forwarded-For", headers.get("Remote-Addr", "unknown"))
    if check_admin_rate_limit(ip):
        return error_response("登录尝试过于频繁，请 5 分钟后再试", 429)
    password = body.get("password", "")
    if not verify_admin_password(password):
        record_admin_attempt(ip)
        return error_response("密码错误", 403)
    token = make_token("admin", "admin")
    return json_response({"access_token": token, "token_type": "bearer"})

def log_admin_action(headers, action, detail=""):
    """Log an admin action to the admin_logs table."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO admin_logs (id, action, detail, ip, created_at) VALUES (?, ?, ?, ?, ?)",
            (uuid.uuid4().hex[:8], action, detail,
             headers.get("X-Forwarded-For", headers.get("Remote-Addr", "unknown")),
             datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        )
        conn.commit()
    finally:
        conn.close()

def handle_admin_stats(headers):
    # Allow either admin token or password header
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM test_records").fetchone()["c"]
        row = conn.execute("SELECT MAX(real_score) as mx, MIN(real_score) as mn, AVG(real_score) as av FROM test_records").fetchone()
        question_count = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        return json_response({
            "total_tests": total,
            "max_score": round(row["mx"], 1) if row["mx"] is not None else None,
            "min_score": round(row["mn"], 1) if row["mn"] is not None else None,
            "avg_score": round(row["av"], 1) if row["av"] is not None else None,
            "question_count": question_count,
        })
    finally:
        conn.close()

# ── New Admin API Handlers ───────────────────────────

def handle_admin_score_distribution(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT real_score FROM test_records").fetchall()
        buckets = {"0-10":0,"11-20":0,"21-30":0,"31-40":0,"41-50":0,"51-60":0,"61-70":0,"71-80":0,"81-90":0,"91-100":0}
        for r in rows:
            s = r["real_score"]
            if s <= 10: buckets["0-10"] += 1
            elif s <= 20: buckets["11-20"] += 1
            elif s <= 30: buckets["21-30"] += 1
            elif s <= 40: buckets["31-40"] += 1
            elif s <= 50: buckets["41-50"] += 1
            elif s <= 60: buckets["51-60"] += 1
            elif s <= 70: buckets["61-70"] += 1
            elif s <= 80: buckets["71-80"] += 1
            elif s <= 90: buckets["81-90"] += 1
            else: buckets["91-100"] += 1
        return json_response({"buckets": buckets, "total": len(rows)})
    finally:
        conn.close()

def handle_admin_test_trend(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT date(created_at) as day, COUNT(*) as cnt FROM test_records GROUP BY day ORDER BY day ASC LIMIT 30"
        ).fetchall()
        return json_response({"daily": [{"date": r["day"], "count": r["cnt"]} for r in rows]})
    finally:
        conn.close()

def handle_admin_question_stats(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM questions").fetchone()["c"]
        approved = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        pending = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='pending'").fetchone()["c"]
        rejected = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='rejected'").fetchone()["c"]
        return json_response({"total": total, "approved": approved, "pending": pending, "rejected": rejected})
    finally:
        conn.close()

def handle_admin_users(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, email, username, role, created_at, banned FROM users ORDER BY created_at DESC").fetchall()
        return json_response([dict(r) for r in rows])
    finally:
        conn.close()

def handle_admin_ban_user(headers, uid, body):
    user, err = require_admin(headers)
    if err:
        return err
    banned = body.get("banned", 1)
    conn = get_db()
    try:
        conn.execute("UPDATE users SET banned = ? WHERE id = ?", (banned, uid))
        conn.commit()
        log_admin_action(headers, "ban_user" if banned else "unban_user", uid)
        return json_response({"id": uid, "banned": banned})
    finally:
        conn.close()

def handle_admin_delete_user(headers, uid):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        row = conn.execute("SELECT id, role FROM users WHERE id = ?", (uid,)).fetchone()
        if not row:
            return error_response("用户不存在", 404)
        if row["role"] not in ("test", "user"):
            return error_response("只能删除测试账号", 403)
        conn.execute("DELETE FROM users WHERE id = ?", (uid,))
        conn.commit()
        log_admin_action(headers, "delete_user", uid)
        return json_response({"detail": "已删除"})
    finally:
        conn.close()

def handle_admin_tags(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT t.id, t.name, COUNT(qt.question_id) as q_count "
            "FROM tags t LEFT JOIN question_tags qt ON t.id = qt.tag_id "
            "GROUP BY t.id ORDER BY q_count DESC"
        ).fetchall()
        return json_response([{"id": r["id"], "name": r["name"], "question_count": r["q_count"]} for r in rows])
    finally:
        conn.close()

def handle_admin_export(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM test_records ORDER BY created_at DESC").fetchall()
        records = []
        for r in rows:
            records.append({
                "record_id": r["id"],
                "real_score": r["real_score"],
                "surface_score": r["surface_score"],
                "created_at": r["created_at"],
            })
        return json_response(records)
    finally:
        conn.close()

def handle_admin_logs(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT 100").fetchall()
        return json_response([dict(r) for r in rows])
    finally:
        conn.close()

def handle_admin_change_pwd(headers, body):
    user, err = require_admin(headers)
    if err:
        return err
    old_pw = body.get("old_password", "")
    new_pw = body.get("new_password", "")
    if not new_pw or len(new_pw) < 6:
        return error_response("新密码至少 6 位", 400)
    if not verify_admin_password(old_pw):
        return error_response("旧密码错误", 403)
    # Update env var won't persist; store in DB for persistence
    conn = get_db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, value TEXT)")
        h = hash_password(new_pw)
        conn.execute("INSERT OR REPLACE INTO admin_config (key, value) VALUES ('password_hash', ?)", (h,))
        conn.commit()
        log_admin_action(headers, "change_password")
        return json_response({"status": "ok"})
    finally:
        conn.close()

def handle_admin_get_config(headers):
    user, err = require_admin(headers)
    if err:
        return err
    config = {}
    conn = get_db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, value TEXT)")
        rows = conn.execute("SELECT * FROM admin_config").fetchall()
        for r in rows:
            config[r["key"]] = r["value"]
    finally:
        conn.close()
    return json_response(config)

# ── Router ────────────────────────────────────────────────

API_ROUTES = []

def route(method, pattern):
    def wrapper(func):
        API_ROUTES.append((method, re.compile("^" + pattern + "$"), func))
        return func
    return wrapper

def dispatch_api(method, path, headers, body):
    for m, pat, handler in API_ROUTES:
        if method != m:
            continue
        m = pat.match(path)
        if m:
            return handler(headers, body, *m.groups())
    return error_response("Not Found", 404)

# Register routes
route("POST", r"/api/auth/register")(lambda h, b, *a: handle_register(b))
route("POST", r"/api/auth/login")(lambda h, b, *a: handle_login(b))
route("POST", r"/api/auth/test-login")(lambda h, b, *a: handle_test_login(h, b))
route("POST", r"/api/auth/nickname")(lambda h, b, *a: handle_set_nickname(h, b))
route("GET", r"/api/auth/me")(lambda h, b, *a: handle_me(h))
route("GET", r"/api/questions")(lambda h, b, *a: handle_get_questions(h, parse_qs(urlparse(a[0] if a else "/").query) if False else {}))
# We handle query params differently - the lambda above is a placeholder
route("POST", r"/api/questions")(lambda h, b, *a: handle_create_question(h, b))
route("GET", r"/api/questions/pending")(lambda h, b, *a: handle_pending_questions(h))
route("PATCH", r"/api/questions/([a-f0-9]+)")(lambda h, b, qid: handle_review_question(h, qid, b))
route("POST", r"/api/test/submit")(lambda h, b, *a: handle_submit_test(h, b))
route("GET", r"/api/test/verify/([a-f0-9]+)")(lambda h, b, rid: handle_verify(h, rid))
route("GET", r"/api/test/verify-token")(lambda h, b, *a: None)  # handled via query params in _handle
route("GET", r"/api/questions/replacement")(lambda h, b, *a: None)  # handled via query params in _handle
route("POST", r"/api/test/skip")(lambda h, b, *a: handle_record_skip(h, b))
route("GET", r"/api/questions/complaints")(lambda h, b, *a: handle_get_complaints(h))
route("DELETE", r"/api/questions/([a-f0-9]+)")(lambda h, b, qid: handle_delete_question(h, qid))
route("GET", r"/api/test/([a-f0-9]+)")(lambda h, b, rid: handle_get_record(h, rid))
route("POST", r"/api/admin/auth")(lambda h, b, *a: handle_admin_auth(h, b))
route("GET", r"/api/admin/stats")(lambda h, b, *a: handle_admin_stats(h))
route("GET", r"/api/admin/score-distribution")(lambda h, b, *a: handle_admin_score_distribution(h))
route("GET", r"/api/admin/test-trend")(lambda h, b, *a: handle_admin_test_trend(h))
route("GET", r"/api/admin/question-stats")(lambda h, b, *a: handle_admin_question_stats(h))
route("GET", r"/api/admin/users")(lambda h, b, *a: handle_admin_users(h))
route("PATCH", r"/api/admin/users/([a-f0-9]+)/ban")(lambda h, b, uid: handle_admin_ban_user(h, uid, b))
route("DELETE", r"/api/admin/users/([a-zA-Z0-9_]+)")(lambda h, b, uid: handle_admin_delete_user(h, uid))
route("GET", r"/api/admin/tags")(lambda h, b, *a: handle_admin_tags(h))
route("GET", r"/api/admin/export")(lambda h, b, *a: handle_admin_export(h))
route("GET", r"/api/admin/logs")(lambda h, b, *a: handle_admin_logs(h))
route("POST", r"/api/admin/change-password")(lambda h, b, *a: handle_admin_change_pwd(h, b))
route("GET", r"/api/admin/config")(lambda h, b, *a: handle_admin_get_config(h))
route("GET", r"/api/admin/question-counts")(lambda h, b, *a: handle_question_counts(h))
route("GET", r"/api/admin/question-bank-history")(lambda h, b, *a: handle_question_bank_history(h))
route("POST", r"/api/admin/batch-insert-test")(lambda h, b, *a: handle_batch_insert_test_questions(h, b))
route("GET", r"/api/contributors")(lambda h, b, *a: handle_contributors(h))
route("GET", r"/api/public-stats")(lambda h, b, *a: handle_public_stats(h, b))
route("POST", r"/api/tester/nickname")(lambda h, b, *a: handle_set_tester_nickname(b))
route("PATCH", r"/api/questions/([a-f0-9]+)/edit")(lambda h, b, qid: handle_edit_question(h, qid, b))
route("GET", r"/api/questions/all")(lambda h, b, *a: handle_all_questions(h))
route("POST", r"/api/admin/questions/delete")(lambda h, b, *a: handle_admin_delete_question(h, b))
route("POST", r"/api/admin/questions/batch-delete")(lambda h, b, *a: handle_admin_batch_delete(h, b))
route("POST", r"/api/admin/questions/batch-set-timelimit")(lambda h, b, *a: handle_admin_batch_set_timelimit(h, b))

# ── Test question pool for quick-fill ────────────────────

TEST_QUESTIONS_POOL = [
    {
        "content": "如果你和伴侣在消费观念上产生分歧，你更倾向于：",
        "dimension": "消费与风控",
        "tags": ["消费观"],
        "time_limit": 15,
        "weight": 1.5,
        "options": [
            {"text": "坐下来沟通，制定双方都能接受的预算方案", "score": 0},
            {"text": "各花各的，互不干涉", "score": 1},
            {"text": "坚持自己的观点，试图说服对方", "score": 2},
            {"text": "冷战直到对方妥协", "score": 3},
        ]
    },
    {
        "content": "加班到很晚回家，发现伴侣已经睡了但给你留了灯和饭菜，你会：",
        "dimension": "情绪与同理心",
        "tags": ["日常关怀"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "心里很暖，第二天当面感谢", "score": 0},
            {"text": "默默吃完，发条微信说晚安", "score": 0},
            {"text": "觉得这是应该的，没什么特别感觉", "score": 2},
            {"text": "嫌饭菜凉了，有点不高兴", "score": 3},
        ]
    },
    {
        "content": "伴侣突然获得一笔意外之财，你会建议他/她：",
        "dimension": "权责对等",
        "tags": ["财务管理"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "先存起来，作为家庭应急基金", "score": 0},
            {"text": "一部分存起来，一部分犒劳自己和家人", "score": 0},
            {"text": "全部用来买一直想要的东西", "score": 2},
            {"text": "拿出来投资高风险项目博一把", "score": 3},
        ]
    },
    {
        "content": "伴侣因为工作压力大对你发了脾气，事后道歉，你会：",
        "dimension": "情绪与同理心",
        "tags": ["包容", "冲突处理"],
        "time_limit": 10,
        "weight": 1.5,
        "options": [
            {"text": "表示理解，安慰对方并一起想办法减压", "score": 0},
            {"text": "接受道歉，但提醒他下次注意方式", "score": 0},
            {"text": "嘴上说没事但心里一直不舒服", "score": 2},
            {"text": "抓住这件事反复翻旧账", "score": 3},
        ]
    },
    {
        "content": "关于双方的社交圈，你认为比较理想的状态是：",
        "dimension": "边界与独立",
        "tags": ["社交边界"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "彼此有自己的社交圈，也融入对方的圈子", "score": 0},
            {"text": "各自保留部分私人朋友，不强行融入", "score": 0},
            {"text": "要求对方的朋友圈必须有自己", "score": 2},
            {"text": "希望对方只和自己玩，不要有太多朋友", "score": 3},
        ]
    },
    {
        "content": "你觉得在亲密关系中，双方应该多久沟通一次「感情状态」？",
        "dimension": "情绪与同理心",
        "tags": ["沟通频率"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "顺其自然，有需要就聊，不刻意", "score": 0},
            {"text": "每周找个固定时间聊一聊近况和感受", "score": 0},
            {"text": "遇到问题才聊，平时不主动提起", "score": 2},
            {"text": "完全不聊，觉得没必要", "score": 3},
        ]
    },
    {
        "content": "看到伴侣和异性同事有说有笑地一起吃饭，你的第一反应是：",
        "dimension": "边界与独立",
        "tags": ["安全感"],
        "time_limit": 10,
        "weight": 1.5,
        "options": [
            {"text": "很正常的工作社交，不会多想", "score": 0},
            {"text": "有一点点在意但选择信任", "score": 0},
            {"text": "回家后旁敲侧击询问对方", "score": 2},
            {"text": "当场走过去打断他们并质问", "score": 3},
        ]
    },
    {
        "content": "你认为什么样的彩礼/嫁妆安排比较合理？",
        "dimension": "权责对等",
        "tags": ["彩礼", "婚姻"],
        "time_limit": 15,
        "weight": 2.0,
        "options": [
            {"text": "双方家庭量力而行，全部给小家庭做启动资金", "score": 0},
            {"text": "走个形式，根据双方条件协商即可", "score": 0},
            {"text": "必须按当地习俗来，不能比别人少", "score": 2},
            {"text": "彩礼是男方诚意的体现，越多越好", "score": 3},
        ]
    },
    {
        "content": "伴侣忘记了你生日，直到当天晚上才想起来，你会：",
        "dimension": "情绪与同理心",
        "tags": ["包容"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "笑着说没关系，但暗示下次要记住", "score": 0},
            {"text": "觉得有点失落但接受道歉", "score": 0},
            {"text": "一整天都闷闷不乐等对方自己发现", "score": 2},
            {"text": "大发脾气，指责对方不在乎自己", "score": 3},
        ]
    },
    {
        "content": "你们的收入差距较大时，你觉得家庭开销应该怎么分担？",
        "dimension": "权责对等",
        "tags": ["经济分担"],
        "time_limit": 15,
        "weight": 1.5,
        "options": [
            {"text": "按收入比例分担，公平合理", "score": 0},
            {"text": "设立共同账户，统一管理收支", "score": 0},
            {"text": "收入高的一方应该多承担", "score": 1},
            {"text": "男人就应该养家，女人赚的钱自己花", "score": 3},
        ]
    },
    {
        "content": "当你情绪低落时，你希望伴侣怎样做？",
        "dimension": "情绪与同理心",
        "tags": ["情感支持"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "安静地陪在身边，等我自己愿意说", "score": 0},
            {"text": "主动关心询问，帮我分析问题", "score": 0},
            {"text": "给我空间暂时不要打扰我", "score": 1},
            {"text": "必须第一时间哄我，不然就是不在乎", "score": 3},
        ]
    },
    {
        "content": "你如何看待婚后的「个人空间」？",
        "dimension": "边界与独立",
        "tags": ["隐私", "空间"],
        "time_limit": 10,
        "weight": 1.0,
        "options": [
            {"text": "即使结婚了也需要各自的兴趣爱好和独处时间", "score": 0},
            {"text": "可以各自保留小秘密，但大事要透明", "score": 0},
            {"text": "结婚了就不分彼此，什么都要共享", "score": 2},
            {"text": "对方的一切我都必须知道，包括聊天记录", "score": 3},
        ]
    },
]

def handle_question_bank_history(headers):
    user, err = require_admin(headers)
    if err:
        return err
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT action, total, approved, pending, rejected, created_at "
            "FROM question_bank_log ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
        return json_response([dict(r) for r in rows])
    finally:
        conn.close()


def handle_question_counts(headers):
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        rows = conn.execute("SELECT dimension, COUNT(*) as c FROM questions WHERE status='approved' GROUP BY dimension").fetchall()
        per_dimension = {}
        for r in rows:
            dim = r["dimension"] or "其他"
            per_dimension[dim] = r["c"]
        return json_response({
            "total": total,
            "per_dimension": per_dimension,
        })
    finally:
        conn.close()

def handle_batch_insert_test_questions(headers, body):
    conn = get_db()
    try:
        # Check conditions
        total = conn.execute("SELECT COUNT(*) as c FROM questions WHERE status='approved'").fetchone()["c"]
        if total >= 200:
            return error_response("题库总数已达 200 道上限", 400)
        rows = conn.execute("SELECT dimension, COUNT(*) as c FROM questions WHERE status='approved' GROUP BY dimension").fetchall()
        per_dimension = {}
        for r in rows:
            per_dimension[r["dimension"] or "其他"] = r["c"]
        # Check each dimension from the pool
        dim_counts = {}
        for q in TEST_QUESTIONS_POOL:
            d = q["dimension"]
            dim_counts[d] = dim_counts.get(d, 0) + 1
        for d, cnt in dim_counts.items():
            existing = per_dimension.get(d, 0)
            if existing + cnt > 50:
                return error_response(f"维度「{d}」已接近上限（{existing} 道），无法批量添加 {cnt} 道", 400)
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        inserted = 0
        for q in TEST_QUESTIONS_POOL:
            qid = uuid.uuid4().hex[:8]
            conn.execute(
                "INSERT INTO questions (id, content, options, dimension, weight, time_limit, status, submitter_id, created_at) VALUES (?, ?, ?, ?, ?, ?, 'approved', 'test_uploader', ?)",
                (qid, q["content"], json.dumps(q["options"], ensure_ascii=False), q["dimension"], q.get("weight", 1.0), q.get("time_limit", 15), now)
            )
            for tag_name in q.get("tags", []):
                tag_name = tag_name.strip()
                if not tag_name:
                    continue
                existing_tag = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
                if existing_tag:
                    tid = existing_tag["id"]
                else:
                    tid = uuid.uuid4().hex[:8]
                    conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
                conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
            inserted += 1
        conn.commit()
        _log_question_bank(conn, "batch_insert")
        return json_response({"inserted": inserted, "message": f"成功添加 {inserted} 道测试题目"})
    finally:
        conn.close()

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self._handle("GET")
    def do_POST(self):
        self._handle("POST")
    def do_PATCH(self):
        self._handle("PATCH")
    def do_DELETE(self):
        self._handle("DELETE")
    def do_OPTIONS(self):
        self._cors_headers()
        self.send_response(200)
        self.end_headers()
    
    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
    
    def _handle(self, method):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        
        # API routes
        if path.startswith("/api/"):
            body = None
            if method in ("POST", "PATCH"):
                try:
                    body = read_body(self)
                except Exception:
                    self._send_error(400, "Invalid JSON")
                    return
            
            status, resp_data, content_type = None, None, None
            for m, pat, handler in API_ROUTES:
                if method != m:
                    continue
                match = pat.match(path)
                if match:
                    # Special handling for GET /api/questions with query params
                    if path == "/api/questions" and method == "GET":
                        qs = parse_qs(parsed.query)
                        status, resp_data, content_type = handle_get_questions(self.headers, qs)
                    elif path == "/api/test/verify-token" and method == "GET":
                        qs = parse_qs(parsed.query)
                        status, resp_data, content_type = handle_verify_by_token(self.headers, qs)
                    elif path == "/api/questions/replacement" and method == "GET":
                        qs = parse_qs(parsed.query)
                        status, resp_data, content_type = handle_get_replacement(self.headers, qs)
                    else:
                        args = match.groups()
                        try:
                            status, resp_data, content_type = handler(self.headers, body, *args)
                        except Exception as e:
                            self._send_error(500, f"Internal error: {e}")
                            return
                    break
            
            if status is None:
                self._send_error(404, "Not Found")
            elif isinstance(resp_data, tuple) and len(resp_data) == 3:
                # Already a (status, body, content_type) tuple
                self._send_response(*resp_data)
            else:
                self._send_response(status, resp_data, content_type)
            return
        
        # Static files
        self._serve_static(path)
    
    def _send_response(self, status, body, content_type="application/json; charset=utf-8"):
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    
    def _send_error(self, status, msg):
        body = json.dumps({"detail": msg}, ensure_ascii=False).encode("utf-8")
        self._send_response(status, body)
    
    def _serve_static(self, path):
        if path == "/":
            path = "/index.html"
        filepath = os.path.join(FRONTEND_DIR, path.lstrip("/"))
        filepath = os.path.normpath(filepath)
        # Security: ensure we're still inside frontend dir
        if not filepath.startswith(os.path.normpath(FRONTEND_DIR)):
            self._send_error(403, "Forbidden")
            return
        if not os.path.isfile(filepath):
            self._send_error(404, "File not found")
            return
        ext = os.path.splitext(filepath)[1].lower()
        content_type = MIME_TYPES.get(ext, "application/octet-stream")
        with open(filepath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self._cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    
    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]} {args[2]}")

def main():
    init_db()
    db_mode = "PostgreSQL" if os.environ.get("DATABASE_URL", "") else "SQLite"
    print(f"数据库: {db_mode}  (表已就绪)")
    # Auto-seed if questions table is empty (uses server.py's get_db, which handles PostgreSQL)
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    if count == 0:
        print("Questions table empty, auto-seeding...")
        try:
            from seed_questions import QUESTIONS_JSON
            questions = json.loads(QUESTIONS_JSON)
            now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            for q in questions:
                qid = uuid.uuid4().hex[:8]
                conn.execute(
                    "INSERT INTO questions (id, content, options, dimension, weight, time_limit, status, submitter_id, created_at) VALUES (?, ?, ?, ?, ?, ?, 'approved', 'test_uploader', ?)",
                    (qid, q["content"], json.dumps(q["options"], ensure_ascii=False), q.get("dimension"), q.get("weight", 1.0), q.get("time_limit", 0), now)
                )
                for tag_name in q.get("tags", []):
                    tag_name = tag_name.strip()
                    if not tag_name:
                        continue
                    existing = conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
                    tid = existing["id"] if existing else uuid.uuid4().hex[:8]
                    if not existing:
                        conn.execute("INSERT INTO tags (id, name) VALUES (?, ?)", (tid, tag_name))
                    conn.execute("INSERT OR IGNORE INTO question_tags (question_id, tag_id) VALUES (?, ?)", (qid, tid))
            conn.commit()
            print(f"Seeded {len(questions)} questions.")
        except ImportError:
            print("Warning: seed_questions.py not found, skipping seed.")
    conn.close()

    host = os.environ.get("HOST", "0.0.0.0")
    server = http.server.ThreadingHTTPServer((host, PORT), RequestHandler)
    print(f"Server running at http://{host}:{PORT}")
    print(f"Frontend: http://{host}:{PORT}/")
    print(f"API: http://{host}:{PORT}/api/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

if __name__ == "__main__":
    main()
