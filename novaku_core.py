"""
NovaEX AI auto-register core (refactored for Telegram bot use).

Differences vs the original CLI script:
- No file-based CSV writes; instead emits each successful account via
  `on_account(row_dict)` callback.
- Supports cooperative cancellation via `cancel_event` (threading.Event).
- Uses `logging` instead of `safe_print` for thread-safe output.
- `run_registration(...)` is a synchronous function meant to be called
  from a worker thread (e.g. `loop.run_in_executor`).

The crypto / network / proxy logic is kept identical to the original.
"""

import base64
import json
import logging
import os
import queue
import random
import re
import secrets
import string
import threading
import time
import urllib.parse
import uuid

import pyotp
import requests
from faker import Faker
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


log = logging.getLogger("novaku_core")

NOVAEX_BASE = "https://m.novaexai.com/prod-api"
MAILTM_BASE = "https://api.mail.tm"
DEFAULT_INVITE = "3tb84z"
TOTP_APP_BASE = "https://totp.app/"
MAX_THREADS = 10

USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
]


def pick_user_agent() -> str:
    return random.choice(USER_AGENT_POOL)


HTTP_TIMEOUT = (8, 25)
HTTP_TIMEOUT_FAST = (8, 15)


# ---------------------------------------------------------------------------
# Proxy classification
# ---------------------------------------------------------------------------
PROXY_EXC = (
    requests.exceptions.ProxyError,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.SSLError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
)
PROXY_BAD_STATUS = {407, 408, 502, 503, 504, 522, 523, 524, 525, 526}


class ProxyDeadError(Exception):
    """Signals that the current proxy is unusable; caller should swap proxy."""


class CancelledError(Exception):
    """Raised when the user cancels the registration run."""


def _maybe_proxy_error_from_status(status_code: int):
    if status_code in PROXY_BAD_STATUS:
        raise ProxyDeadError(f"proxy returned HTTP {status_code}")


def _is_proxy_failure(exc: BaseException) -> bool:
    if isinstance(exc, ProxyDeadError):
        return True
    if isinstance(exc, PROXY_EXC):
        return True
    msg = str(exc)
    if any(s in msg for s in ("unexpected IV length", "AES-GCM", "non-JSON", "InvalidTag")):
        return True
    return False


def decode_jwt_payload(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        seg = parts[1] + "=" * (-len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(seg).decode("utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Pools
# ---------------------------------------------------------------------------
class InvitePool:
    def __init__(self, codes=None):
        self._lock = threading.Lock()
        self._codes = list(codes or [])

    def add(self, code: str):
        if not code:
            return
        with self._lock:
            self._codes.append(code)

    def pick(self):
        with self._lock:
            return random.choice(self._codes) if self._codes else None

    def size(self) -> int:
        with self._lock:
            return len(self._codes)


class ProxyPool:
    def __init__(self, lines):
        self._all = []
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            self._all.append(self._normalize(line))
        if not self._all:
            raise ValueError("proxy file is empty")
        random.shuffle(self._all)
        self._q = queue.Queue()
        for p in self._all:
            self._q.put(p)

    @staticmethod
    def _normalize(line: str) -> str:
        if line.startswith("http://") or line.startswith("https://"):
            return line
        if "@" in line:
            return f"http://{line}"
        parts = line.split(":")
        if len(parts) == 4:
            host, port, user, pwd = parts
            return f"http://{user}:{pwd}@{host}:{port}"
        return f"http://{line}"

    def acquire(self) -> str:
        return self._q.get()

    def release(self, proxy: str):
        self._q.put(proxy)

    def size(self) -> int:
        return len(self._all)


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------
def base_headers(device_id: str, user_agent: str):
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://m.novaexai.com",
        "Referer": "https://m.novaexai.com/register",
        "User-Agent": user_agent,
        "lang": "en",
        "deviceid": device_id,
        "X-App-Version": "1.0",
    }


def make_session(device_id: str, user_agent: str, proxy):
    s = requests.Session()
    s.headers.update(base_headers(device_id, user_agent))
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
def gen_username(fake: Faker) -> str:
    base = re.sub(r"[^A-Za-z]", "", fake.first_name()).lower()
    while len(base) < 5:
        base += secrets.choice(string.ascii_lowercase)
    digits = "".join(secrets.choice(string.digits) for _ in range(3))
    username = base + digits
    while len(username) < 8:
        username += secrets.choice(string.digits)
    return username


def gen_password(length: int = 10) -> str:
    letters = [secrets.choice(string.ascii_lowercase) for _ in range(length - 4)]
    letters += [secrets.choice(string.ascii_uppercase) for _ in range(2)]
    digits = [secrets.choice(string.digits) for _ in range(2)]
    chars = letters + digits
    random.shuffle(chars)
    return "".join(chars)


# ---------------------------------------------------------------------------
# ECIES
# ---------------------------------------------------------------------------
def parse_pub(b64_der: str):
    return serialization.load_der_public_key(base64.b64decode(b64_der))


def encode_pub_der(public_key) -> bytes:
    return public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def derive_aes_key(shared_secret: bytes) -> bytes:
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"",
        info=b"ECIES-AES-KEY",
    ).derive(shared_secret)


def encrypt_request(server_pub_b64: str, payload: dict):
    server_pub = parse_pub(server_pub_b64)
    eph_priv = ec.generate_private_key(ec.SECP256R1())
    aes_key = derive_aes_key(eph_priv.exchange(ec.ECDH(), server_pub))
    iv = os.urandom(12)
    plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ct = AESGCM(aes_key).encrypt(iv, plaintext, None)
    return {
        "ephemeralPublicKey": base64.b64encode(encode_pub_der(eph_priv.public_key())).decode(),
        "encryptedData": base64.b64encode(ct).decode(),
        "iv": base64.b64encode(iv).decode(),
        "authTag": "",
    }, eph_priv


def parse_iv_from_x509(data: bytes) -> bytes:
    n = 0
    if data[n] == 0x04:
        n += 1
        length = data[n]; n += 1
        if length & 0x80:
            cnt = length & 0x7F; length = 0
            for _ in range(cnt):
                length = (length << 8) | data[n]; n += 1
        return data[n:n + length]
    if data[n] == 0x30:
        n += 1
        length = data[n]; n += 1
        if length & 0x80:
            cnt = length & 0x7F; length = 0
            for _ in range(cnt):
                length = (length << 8) | data[n]; n += 1
        if data[n] == 0x04:
            n += 1
            l2 = data[n]; n += 1
            if l2 & 0x80:
                cnt = l2 & 0x7F; l2 = 0
                for _ in range(cnt):
                    l2 = (l2 << 8) | data[n]; n += 1
            return data[n:n + l2]
    return data


def _b64_clean(s: str) -> str:
    s = re.sub(r"\s+", "", s).replace("-", "+").replace("_", "/")
    while len(s) % 4:
        s += "="
    return s


def decrypt_response(eph_priv, server_pub_b64, body: dict) -> dict:
    server_pub = parse_pub(server_pub_b64)
    aes_key = derive_aes_key(eph_priv.exchange(ec.ECDH(), server_pub))
    iv = parse_iv_from_x509(base64.b64decode(_b64_clean(body["iv"])))
    if len(iv) != 12:
        raise ValueError(f"unexpected IV length {len(iv)}")
    ct = base64.b64decode(_b64_clean(body["encryptedData"]))
    if body.get("authTag"):
        ct += base64.b64decode(_b64_clean(body["authTag"]))
    return json.loads(AESGCM(aes_key).decrypt(iv, ct, None).decode("utf-8"))


# ---------------------------------------------------------------------------
# Novaex helpers
# ---------------------------------------------------------------------------
def get_server_public_key(session) -> str:
    try:
        r = session.get(f"{NOVAEX_BASE}/security/ecies-public-key", timeout=HTTP_TIMEOUT_FAST)
    except PROXY_EXC as e:
        raise ProxyDeadError(str(e))
    _maybe_proxy_error_from_status(r.status_code)
    r.raise_for_status()
    body = r.json()
    if body.get("code") != 200:
        raise RuntimeError(f"fetch public key failed: {body}")
    return body["data"]["publicKey"]


def encrypted_post(session, path, payload, server_pub, *, token=None):
    enc, eph = encrypt_request(server_pub, payload)
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = session.post(
            f"{NOVAEX_BASE}{path}",
            data=json.dumps(enc, separators=(",", ":")),
            headers=headers,
            timeout=HTTP_TIMEOUT,
        )
    except PROXY_EXC as e:
        raise ProxyDeadError(str(e))
    _maybe_proxy_error_from_status(r.status_code)
    try:
        body = r.json()
    except ValueError:
        raise RuntimeError(f"non-JSON [{r.status_code}]: {r.text[:200]}")
    if isinstance(body, dict) and "encryptedData" in body and "iv" in body:
        srv_pub = body.get("ephemeralPublicKey") or server_pub
        return decrypt_response(eph, srv_pub, body)
    return body


def plain_get(session, path, *, token=None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = session.get(f"{NOVAEX_BASE}{path}", headers=headers, timeout=HTTP_TIMEOUT_FAST)
    except PROXY_EXC as e:
        raise ProxyDeadError(str(e))
    _maybe_proxy_error_from_status(r.status_code)
    try:
        return r.json()
    except ValueError:
        raise RuntimeError(f"non-JSON [{r.status_code}]: {r.text[:200]}")


# ---------------------------------------------------------------------------
# mail.tm helpers
# ---------------------------------------------------------------------------
def mailtm_pick_domain(session) -> str:
    try:
        r = session.get(
            f"{MAILTM_BASE}/domains?page=1",
            timeout=HTTP_TIMEOUT_FAST,
            headers={"Accept": "application/ld+json"},
        )
    except PROXY_EXC as e:
        raise ProxyDeadError(str(e))
    _maybe_proxy_error_from_status(r.status_code)
    r.raise_for_status()
    members = r.json().get("hydra:member", [])
    active = [d["domain"] for d in members if d.get("isActive")]
    if not active:
        raise RuntimeError("no active mail.tm domains")
    return random.choice(active)


def mailtm_create_account(session, address: str, password: str):
    try:
        r = session.post(
            f"{MAILTM_BASE}/accounts",
            json={"address": address, "password": password},
            headers={"Content-Type": "application/json"},
            timeout=HTTP_TIMEOUT_FAST,
        )
    except PROXY_EXC as e:
        raise ProxyDeadError(str(e))
    _maybe_proxy_error_from_status(r.status_code)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"mail.tm create [{r.status_code}]: {r.text[:120]}")
    return r.json()


# ---------------------------------------------------------------------------
# Per-account workflow
# ---------------------------------------------------------------------------
def register_one(session, fake, server_pub, mail_domain, invite, retries, log_prefix, cancel_event):
    last_err = None
    for attempt in range(1, retries + 1):
        if cancel_event.is_set():
            raise CancelledError()
        username = gen_username(fake)
        password = gen_password()
        email = f"{username}@{mail_domain}"
        log.info(f"{log_prefix} attempt {attempt}/{retries} -> {username} / {email}")

        try:
            avail = encrypted_post(session, "/user/username/exist", {"username": username}, server_pub)
            if avail.get("code") == 200 and avail.get("data") is True:
                continue
        except Exception as e:
            if _is_proxy_failure(e):
                raise
            log.info(f"{log_prefix}   ! exist warn: {e}")

        try:
            mailtm_create_account(session, email, password)
        except Exception as e:
            if _is_proxy_failure(e):
                raise
            last_err = e
            log.info(f"{log_prefix}   ! mail.tm error: {e}")
            time.sleep(1)
            continue

        payload = {
            "username": username, "password": password,
            "passwordConfirm": password, "inviteCode": invite,
            "fbp": "", "fbc": "", "fbclid": "", "route_code": "",
        }
        try:
            res = encrypted_post(session, "/user/registered", payload, server_pub)
        except Exception as e:
            if _is_proxy_failure(e):
                raise
            last_err = e
            log.info(f"{log_prefix}   ! register error: {e}")
            continue

        code = res.get("code")
        if code == 200:
            token = res.get("data") or ""
            log.info(f"{log_prefix}   OK register token={str(token)[:24]}...")
            return {"username": username, "password": password, "email": email, "token": token}
        if code == 1003:
            log.info(f"{log_prefix}   ! username taken (server) - retry")
            continue
        if code == 1001:
            raise RuntimeError(f"invite code {invite!r} rejected by server")
        last_err = res
        log.info(f"{log_prefix}   ! unexpected {res}")
        time.sleep(1)
    raise RuntimeError(f"register failed after {retries} attempts: {last_err}")


def bind_google_auth(session, server_pub, account, log_prefix, cancel_event):
    token = account["token"]
    if cancel_event.is_set():
        raise CancelledError()

    cred = plain_get(session, "/validator/getCredential", token=token)
    if cred.get("code") != 200 or not cred.get("data"):
        raise RuntimeError(f"getCredential failed: {cred}")
    secret = cred["data"]["secret"]
    otpauth = cred["data"].get("otpAuthURL", "")

    totp = pyotp.TOTP(secret)
    remaining = totp.interval - (int(time.time()) % totp.interval)
    if remaining <= 2:
        time.sleep(remaining + 1)
    code = totp.now()

    res = encrypted_post(session, "/validator/bound", {"code": int(code)}, server_pub, token=token)
    if res.get("code") != 200:
        if cancel_event.is_set():
            raise CancelledError()
        time.sleep(31)
        code = pyotp.TOTP(secret).now()
        res = encrypted_post(session, "/validator/bound", {"code": int(code)}, server_pub, token=token)
    if res.get("code") != 200:
        raise RuntimeError(f"validator/bound failed: {res}")
    log.info(f"{log_prefix}   OK Google Authenticator bound")
    return secret, otpauth


def make_totp_app_url(secret: str, label: str) -> str:
    qs = urllib.parse.urlencode({"secret": secret, "issuer": "NovaEX AI", "account": label})
    return f"{TOTP_APP_BASE}?{qs}"


CSV_HEADER = [
    "created_at", "novaex_username", "novaex_password",
    "email", "mailtm_password",
    "ga_secret", "otpauth_url", "totp_app_url",
    "invite_code", "user_code", "token",
    "device_id", "user_agent", "proxy", "cookies",
]


def proxy_label(p: str) -> str:
    try:
        u = urllib.parse.urlparse(p)
        return f"{u.hostname}:{u.port}"
    except Exception:
        return p


def _swap_proxy(pool, current, log_prefix, reason):
    log.info(f"{log_prefix}   ~ proxy dead ({reason}); swapping ...")
    pool.release(current)
    new_proxy = pool.acquire()
    while new_proxy == current and pool.size() > 1:
        pool.release(new_proxy)
        new_proxy = pool.acquire()
    log.info(f"{log_prefix}   ~ new proxy = {proxy_label(new_proxy)}")
    return new_proxy


def worker_task(idx, total, settings, fake, pool, invite_pool, mail_domain,
                server_pub_initial, on_account, cancel_event):
    if cancel_event.is_set():
        return False

    proxy = pool.acquire()
    device_id = str(uuid.uuid4())
    user_agent = pick_user_agent()
    session = make_session(device_id, user_agent, proxy)

    log_prefix_base = f"[{idx:02d}/{total}]"
    log_prefix = f"{log_prefix_base}[{proxy_label(proxy)}]"

    invite = settings["invite"] if settings["invite_mode"] == "fixed" \
        else (invite_pool.pick() or settings["invite"])

    swaps_left = settings["max_proxy_swaps"]
    server_pub = server_pub_initial

    # PHASE 1: register
    account = None
    while account is None:
        if cancel_event.is_set():
            pool.release(proxy)
            return False
        try:
            try:
                server_pub = get_server_public_key(session)
            except Exception as e:
                if _is_proxy_failure(e):
                    raise
            account = register_one(session, fake, server_pub, mail_domain,
                                   invite, settings["retries"], log_prefix, cancel_event)
        except CancelledError:
            pool.release(proxy)
            return False
        except Exception as e:
            if _is_proxy_failure(e) and swaps_left > 0:
                proxy = _swap_proxy(pool, proxy, log_prefix, e)
                session = make_session(device_id, user_agent, proxy)
                log_prefix = f"{log_prefix_base}[{proxy_label(proxy)}]"
                swaps_left -= 1
                continue
            log.info(f"{log_prefix} FAIL register {e}")
            pool.release(proxy)
            return False

    user_code = (decode_jwt_payload(account["token"]).get("userCode") or "").strip()
    if user_code:
        invite_pool.add(user_code)

    # PHASE 2: GA bind
    secret, otpauth = "", ""
    if not settings["no_ga"]:
        ga_done = False
        while not ga_done:
            if cancel_event.is_set():
                break
            try:
                secret, otpauth = bind_google_auth(session, server_pub, account, log_prefix, cancel_event)
                ga_done = True
            except CancelledError:
                break
            except Exception as e:
                if _is_proxy_failure(e) and swaps_left > 0:
                    proxy = _swap_proxy(pool, proxy, log_prefix, e)
                    session = make_session(device_id, user_agent, proxy)
                    log_prefix = f"{log_prefix_base}[{proxy_label(proxy)}]"
                    swaps_left -= 1
                    continue
                log.info(f"{log_prefix}   !! GA bind failed: {e}")
                break

    totp_url = make_totp_app_url(secret, account["email"]) if secret else ""
    try:
        cookies_json = json.dumps(
            {c.name: c.value for c in session.cookies},
            ensure_ascii=False, separators=(",", ":"),
        )
    except Exception:
        cookies_json = "{}"

    row = {
        "created_at": int(time.time()),
        "novaex_username": account["username"],
        "novaex_password": account["password"],
        "email": account["email"],
        "mailtm_password": account["password"],
        "ga_secret": secret,
        "otpauth_url": otpauth,
        "totp_app_url": totp_url,
        "invite_code": invite,
        "user_code": user_code,
        "token": account["token"],
        "device_id": device_id,
        "user_agent": user_agent,
        "proxy": proxy_label(proxy),
        "cookies": cookies_json,
    }
    try:
        on_account(row)
    except Exception as e:
        log.warning(f"on_account callback error: {e}")

    log.info(f"{log_prefix} DONE {account['email']}")
    pool.release(proxy)
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS = {
    "count": 1,
    "threads": 3,
    "invite": DEFAULT_INVITE,
    "invite_mode": "random",
    "no_ga": False,
    "retries": 3,
    "max_proxy_swaps": 5,
}


def run_registration(settings: dict, proxy_file: str,
                     on_account=None, cancel_event=None,
                     pre_invite_codes=None) -> dict:
    """
    Execute the registration pipeline.

    Returns a dict: {"success": int, "total": int, "error": str | None}
    """
    if on_account is None:
        on_account = lambda row: None
    if cancel_event is None:
        cancel_event = threading.Event()

    if not os.path.exists(proxy_file):
        return {"success": 0, "total": settings["count"], "error": f"Proxy file not found: {proxy_file}"}

    try:
        with open(proxy_file, "r", encoding="utf-8") as f:
            pool = ProxyPool(f.readlines())
    except ValueError as e:
        return {"success": 0, "total": settings["count"], "error": str(e)}

    log.info(f"Loaded proxies={pool.size()} target={settings['count']} threads={settings['threads']}")

    fake = Faker()
    invite_pool = InvitePool(pre_invite_codes or [])

    # Bootstrap
    server_pub_initial = None
    mail_domain = None
    swaps_left = settings["max_proxy_swaps"]
    boot_proxy = pool.acquire()
    boot_session = None
    while server_pub_initial is None:
        if cancel_event.is_set():
            pool.release(boot_proxy)
            return {"success": 0, "total": settings["count"], "error": "cancelled"}
        boot_session = make_session(str(uuid.uuid4()), pick_user_agent(), boot_proxy)
        try:
            server_pub_initial = get_server_public_key(boot_session)
        except Exception as e:
            if _is_proxy_failure(e) and swaps_left > 0:
                pool.release(boot_proxy)
                boot_proxy = pool.acquire()
                swaps_left -= 1
                continue
            pool.release(boot_proxy)
            return {"success": 0, "total": settings["count"], "error": f"bootstrap pubkey failed: {e}"}

    while mail_domain is None:
        if cancel_event.is_set():
            pool.release(boot_proxy)
            return {"success": 0, "total": settings["count"], "error": "cancelled"}
        try:
            mail_domain = mailtm_pick_domain(boot_session)
        except Exception as e:
            if _is_proxy_failure(e) and swaps_left > 0:
                pool.release(boot_proxy)
                boot_proxy = pool.acquire()
                boot_session = make_session(str(uuid.uuid4()), pick_user_agent(), boot_proxy)
                swaps_left -= 1
                continue
            pool.release(boot_proxy)
            return {"success": 0, "total": settings["count"], "error": f"pick mail.tm domain failed: {e}"}
    pool.release(boot_proxy)

    success = 0
    success_lock = threading.Lock()
    count = settings["count"]
    threads = max(1, min(settings["threads"], MAX_THREADS))

    work_queue: "queue.Queue[int]" = queue.Queue()
    for i in range(1, count + 1):
        work_queue.put(i)

    def worker_loop():
        nonlocal success
        while True:
            if cancel_event.is_set():
                return
            try:
                idx = work_queue.get_nowait()
            except queue.Empty:
                return
            try:
                ok = worker_task(
                    idx, count, settings, fake, pool, invite_pool,
                    mail_domain, server_pub_initial, on_account, cancel_event,
                )
            except Exception as e:
                log.warning(f"worker raised: {e}")
                ok = False
            if ok:
                with success_lock:
                    success += 1

    workers = [threading.Thread(target=worker_loop, daemon=True) for _ in range(threads)]
    for t in workers:
        t.start()
    for t in workers:
        t.join()

    return {
        "success": success,
        "total": count,
        "error": None if not cancel_event.is_set() else "cancelled",
    }


def rows_to_csv_bytes(rows) -> bytes:
    """Convert a list of row dicts into CSV bytes using the canonical header."""
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(CSV_HEADER)
    for r in rows:
        writer.writerow([r.get(k, "") for k in CSV_HEADER])
    return buf.getvalue().encode("utf-8")
  
