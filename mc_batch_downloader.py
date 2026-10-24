#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minecraft 原版批量下载器 v1.8
= v1.6 全部功能（多进程/三行进度/版本文件量/竞速/多源轮询/熔断/verbose）
+ v1.7 提速（线程本地 HTTPS 连接池 + 全局线程池）
"""

import os, sys, json, time, uuid, zipfile, shutil, argparse, threading
import http.client
import multiprocessing as mp
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError

# ============================================================
# 源配置
# ============================================================
OFFICIAL_MANIFEST = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"
DOWNLOAD_SOURCES = {
    "official": {"name": "Mojang官方", "manifest": OFFICIAL_MANIFEST, "replacements": []},
    "bmclapi": {
        "name": "BMCLAPI",
        "manifest": "https://bmclapi2.bangbang93.com/mc/game/version_manifest_v2.json",
        "replacements": [
            ("https://launchermeta.mojang.com", "https://bmclapi2.bangbang93.com"),
            ("https://launcher.mojang.com", "https://bmclapi2.bangbang93.com"),
            ("https://piston-meta.mojang.com", "https://bmclapi2.bangbang93.com"),
            ("https://resources.download.minecraft.net", "https://bmclapi2.bangbang93.com/assets"),
            ("https://libraries.minecraft.net", "https://bmclapi2.bangbang93.com/maven"),
        ],
    },
    "hkmirror": {
        "name": "HK光速",
        "manifest": "https://mirror.hkmc.online/mc/game/version_manifest_v2.json",
        "replacements": [
            ("https://launchermeta.mojang.com", "https://mirror.hkmc.online"),
            ("https://launcher.mojang.com", "https://mirror.hkmc.online"),
            ("https://piston-meta.mojang.com", "https://mirror.hkmc.online"),
            ("https://resources.download.minecraft.net", "https://mirror.hkmc.online/assets"),
            ("https://libraries.minecraft.net", "https://mirror.hkmc.online/maven"),
        ],
    },
}

FAIL_THRESHOLD = 15
COOLDOWN_SECONDS = 45.0
FAULT_CODES = {403, 429, 502, 503, 504}
RACE_THRESHOLD = 3 * 1024 * 1024

# ============================================================
# 全局
# ============================================================
_print_lock = threading.Lock()
_stop_event = threading.Event()
_PROC_TAG = ""
_tls = threading.local()
_H = {}
_P = {}
_W = {}
_WORKER_EXECUTOR = None


def safe_print(*args, **kwargs):
    with _print_lock:
        prefix = f"[{_PROC_TAG}] " if _PROC_TAG else ""
        print(prefix + " ".join(str(a) for a in args), **kwargs)


def apply_source(url, repls):
    for old, new in repls:
        if url.startswith(old):
            return url.replace(old, new, 1)
    return url


# ============================================================
# 线程本地连接池
# ============================================================
def _get_conn(host, port, is_https, timeout=60):
    pool = getattr(_tls, "conns", None)
    if pool is None:
        pool = {}
        _tls.conns = pool
    key = (host, port)
    conn = pool.get(key)
    if conn is None:
        conn = (http.client.HTTPSConnection if is_https else http.client.HTTPConnection)(
            host, port, timeout=timeout)
        pool[key] = conn
    return conn


def _drop_conn(host, port):
    pool = getattr(_tls, "conns", None)
    if pool:
        conn = pool.pop((host, port), None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _stream_pooled(url, tmp, timeout=60, stop_evt=None):
    """连接池下载，支持外部停止事件"""
    stop = stop_evt or _stop_event
    parsed = urlparse(url)
    host = parsed.hostname
    is_https = parsed.scheme == "https"
    port = parsed.port or (443 if is_https else 80)
    path = parsed.path + ("?" + parsed.query if parsed.query else "")

    for attempt in range(3):
        if stop.is_set():
            raise InterruptedError
        conn = _get_conn(host, port, is_https, timeout)
        try:
            conn.request("GET", path, headers={
                "User-Agent": "MC-Batch-Downloader/1.8",
                "Accept": "*/*",
                "Accept-Encoding": "identity",
            })
            resp = conn.getresponse()
            if resp.status != 200:
                try:
                    resp.read()
                except Exception:
                    pass
                raise HTTPError(url, resp.status, resp.reason, resp.headers, None)
            with open(tmp, "wb") as f:
                while True:
                    if stop.is_set():
                        raise InterruptedError
                    chunk = resp.read(262144)
                    if not chunk:
                        break
                    f.write(chunk)
                    _progress_add_bytes(len(chunk))
            return True
        except (http.client.HTTPException, ConnectionError, OSError, TimeoutError) as e:
            _drop_conn(host, port)
            if isinstance(e, InterruptedError):
                raise
            if attempt == 2:
                raise
            time.sleep(0.2 * (attempt + 1))
    return False


# ============================================================
# 源健康（含 active 计数）
# ============================================================
def _health_init(order, fails, cool, ok, active, lock):
    _H.clear()
    _H.update(fails=fails, cool=cool, ok=ok, active=active, lock=lock,
              order=list(order), index={s: i for i, s in enumerate(order)})


def _health_begin(name):
    i = _H["index"][name]
    with _H["lock"]:
        _H["active"][i] += 1


def _health_end(name, success):
    i = _H["index"][name]
    with _H["lock"]:
        _H["active"][i] = max(0, _H["active"][i] - 1)
        if success:
            _H["ok"][i] += 1
            _H["fails"][i] = 0
        else:
            _H["fails"][i] += 1
            if _H["fails"][i] >= FAIL_THRESHOLD:
                _H["cool"][i] = time.time() + COOLDOWN_SECONDS
                _H["fails"][i] = 0


def _health_cooled(name):
    return _H["cool"][_H["index"][name]] > time.time()


def _next_source():
    with _P["rr"].get_lock():
        idx = _P["rr"].value
        _P["rr"].value = idx + 1
    order = _H["order"]
    n = len(order)
    for off in range(n):
        name = order[(idx + off) % n]
        if not _health_cooled(name):
            return name
    return order[idx % n]


# ============================================================
# 进度
# ============================================================
def _progress_inc(key):
    v = _P[key]
    with v.get_lock():
        v.value += 1


def _progress_add_bytes(n):
    v = _P["bytes"]
    with v.get_lock():
        v.value += n


def _update_version_progress(vid, total, done, phase):
    with _P["vp_lock"]:
        _P["version_progress"][vid] = {"total": total, "done": done, "phase": phase}


def _inc_version_done(vid):
    with _P["vp_lock"]:
        d = _P["version_progress"].get(vid)
        if d is not None:
            d = dict(d)
            d["done"] = d.get("done", 0) + 1
            _P["version_progress"][vid] = d


def _set_proc_version(tag, vid, phase):
    with _P["pc_lock"]:
        if vid is None:
            _P["proc_current"].pop(tag, None)
        else:
            _P["proc_current"][tag] = {"version": vid, "phase": phase}


# ============================================================
# 竞速 & 下载核心
# ============================================================
def _unique_tmp(dest):
    tag = f"{os.getpid()}_{threading.get_ident()}_{uuid.uuid4().hex[:8]}"
    return dest.with_name(f"{dest.name}.{tag}.part")


def _safe_replace(src, dst, retries=5):
    for i in range(retries):
        try:
            os.replace(src, dst)
            return True
        except (PermissionError, OSError):
            if i == retries - 1:
                return False
            time.sleep(0.15 * (i + 1))
    return False


def _race_download(raw_url, dest, tmp, source_names, timeout=60):
    """两源同时下载，先完成的胜出"""
    race_stop = threading.Event()
    winner = [None]
    lock = threading.Lock()
    tmp_files = {}

    def _try(name, idx):
        rt = tmp.with_name(f"{tmp.name}.race{idx}")
        tmp_files[idx] = rt
        try:
            url = apply_source(raw_url, DOWNLOAD_SOURCES[name]["replacements"])
            _health_begin(name)
            try:
                _stream_pooled(url, rt, timeout, stop_evt=race_stop)
                with lock:
                    if winner[0] is None:
                        winner[0] = (name, rt)
                        race_stop.set()
                _health_end(name, True)
            except InterruptedError:
                # 被竞速停止，不算失败
                _health_end(name, True)
            except Exception:
                _health_end(name, False)
        except Exception:
            _health_end(name, False)

    threads = [threading.Thread(target=_try, args=(n, i), daemon=True)
               for i, n in enumerate(source_names)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=timeout + 10)

    if winner[0]:
        name, rt = winner[0]
        if _safe_replace(rt, dest):
            _progress_inc("files_done")
            for i, tf in tmp_files.items():
                if tf != rt:
                    tf.unlink(missing_ok=True)
            return True
    for tf in tmp_files.values():
        tf.unlink(missing_ok=True)
    return False


def download_file(raw_url, dest, size=0, force=False):
    if _stop_event.is_set():
        return False
    if not force and dest.exists() and dest.stat().st_size > 0:
        _progress_inc("files_done")
        return True
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _unique_tmp(dest)

    preferred = _next_source()
    order = [preferred] + [s for s in _H["order"] if s != preferred]

    try:
        # 大文件竞速
        if _W.get("race") and size >= RACE_THRESHOLD:
            healthy = [s for s in order if not _health_cooled(s)][:2]
            if len(healthy) >= 2:
                if _race_download(raw_url, dest, tmp, healthy):
                    return True

        # 普通轮询
        for _round in range(3):
            if _stop_event.is_set():
                return False
            any_tried = False
            for name in order:
                if _health_cooled(name):
                    continue
                any_tried = True
                url = apply_source(raw_url, DOWNLOAD_SOURCES[name]["replacements"])
                _health_begin(name)
                try:
                    _stream_pooled(url, tmp)
                    if _safe_replace(tmp, dest):
                        _health_end(name, True)
                        _progress_inc("files_done")
                        return True
                    _health_end(name, False)
                except InterruptedError:
                    _health_end(name, False)
                    return False
                except HTTPError as e:
                    tmp.unlink(missing_ok=True)
                    if e.code in FAULT_CODES:
                        _health_end(name, False)
                    else:
                        _health_end(name, True)
                except Exception:
                    tmp.unlink(missing_ok=True)
                    _health_end(name, False)
            if not any_tried:
                time.sleep(1.0)
        _progress_inc("files_fail")
        if _W.get("verbose"):
            safe_print(f"  ✗ 失败: {dest.name}")
        return False
    finally:
        tmp.unlink(missing_ok=True)


def fetch_json(url, retries=3, timeout=30):
    last = None
    for i in range(1, retries + 1):
        try:
            parsed = urlparse(url)
            host = parsed.hostname
            is_https = parsed.scheme == "https"
            port = parsed.port or (443 if is_https else 80)
            path = parsed.path + ("?" + parsed.query if parsed.query else "")
            conn = _get_conn(host, port, is_https, timeout)
            try:
                conn.request("GET", path,
                             headers={"User-Agent": "MC-Batch-Downloader/1.8"})
                resp = conn.getresponse()
                data = resp.read()
                if resp.status != 200:
                    raise HTTPError(url, resp.status, resp.reason, resp.headers, None)
                return json.loads(data.decode("utf-8"))
            except Exception:
                _drop_conn(host, port)
                raise
        except Exception as e:
            last = e
            if i < retries:
                time.sleep(1.5 * i)
    raise RuntimeError(f"无法获取 {url}: {last}")


def _parallel(items, force, vid, phase):
    """items: [(url, dest, size)]，提交到全局线程池"""
    if not items:
        return 0, 0
    _update_version_progress(vid, total=len(items), done=0, phase=phase)
    _set_proc_version(_PROC_TAG, vid, phase)
    ok = fail = 0
    futures = [_WORKER_EXECUTOR.submit(download_file, u, d, s, force) for u, d, s in items]
    try:
        for fut in as_completed(futures):
            try:
                if fut.result():
                    ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
            _inc_version_done(vid)
    except KeyboardInterrupt:
        _stop_event.set()
        for f in futures:
            f.cancel()
        raise
    return ok, fail


# ============================================================
# 版本选择 & 库规则
# ============================================================
def select_versions(manifest, args):
    av = manifest.get("versions", [])
    s = []
    if args.latest:
        l = manifest.get("latest", {})
        s += [l["release"], l["snapshot"]]
        s = list(dict.fromkeys(s))
    if args.version:
        s += args.version
    if args.all:
        s += [v["id"] for v in av]
    if args.release:
        s += [v["id"] for v in av if v["type"] == "release"]
    if args.snapshot:
        s += [v["id"] for v in av if v["type"] == "snapshot"]
    if args.type:
        ts = {t.strip() for t in args.type.split(",")}
        s += [v["id"] for v in av if v["type"] in ts]
    if args.from_date:
        s += [v["id"] for v in av if v.get("releaseTime", "")[:10] >= args.from_date]
    seen, out = set(), []
    for x in s:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    if not out:
        raise SystemExit("未选中版本")
    return out


def _lib_parts(name):
    p = name.split(":")
    return (p[0].split("."), p[1], p[2]) if len(p) >= 3 else None


def _rules_allow(rules):
    import platform
    sysname = platform.system().lower()
    os_name = {"windows": "windows", "darwin": "osx", "linux": "linux"}.get(sysname, sysname)
    a = False
    for r in rules:
        act = r.get("action")
        r_os = r.get("os")
        if r_os is None:
            if act == "allow":
                a = True
            elif act == "disallow":
                a = False
            continue
        if r_os.get("name") == os_name:
            if act == "allow":
                a = True
            elif act == "disallow":
                a = False
    return a


ASSETS_LANG = ("minecraft/lang/", "minecraft/font/")
ASSETS_FULL = ("minecraft/lang/", "minecraft/font/", "minecraft/textures/",
               "minecraft/models/", "minecraft/blockstates/", "minecraft/sounds.json",
               "minecraft/sounds/", "minecraft/realms/")


# ============================================================
# 单版本
# ============================================================
def download_version(vid, manifest, game_root, force, assets_mode):
    entry = next((v for v in manifest.get("versions", []) if v["id"] == vid), None)
    if not entry:
        return {"version": vid, "ok": False, "error": "版本不存在"}

    try:
        vj = fetch_json(entry["url"])
    except Exception as e:
        return {"version": vid, "ok": False, "error": f"JSON失败: {e}"}

    vdir = game_root / "versions" / vid
    vdir.mkdir(parents=True, exist_ok=True)
    with open(vdir / f"{vid}.json", "w", encoding="utf-8") as f:
        json.dump(vj, f, ensure_ascii=False, indent=2)

    downloads = {}

    def add(url, dest, size=0):
        k = str(dest).lower()
        if k not in downloads:
            downloads[k] = (url, dest, size)

    c = vj.get("downloads", {}).get("client")
    if c and c.get("url"):
        add(c["url"], vdir / f"{vid}.jar", c.get("size", 0))

    lib_dir = game_root / "libraries"
    import platform
    sysname = platform.system().lower()
    for lib in vj.get("libraries", []):
        ld = lib.get("downloads", {})
        art = ld.get("artifact")
        parsed = _lib_parts(lib.get("name", ""))
        if art and art.get("url"):
            fn = Path(urlparse(art["url"]).path).name
            dest = lib_dir.joinpath(*parsed[0], parsed[1], parsed[2], fn) if parsed else lib_dir / fn
            add(art["url"], dest, art.get("size", 0))
        cl = ld.get("classifiers")
        if cl and parsed:
            gp, a, v = parsed
            if sysname == "windows":
                keys = ["natives-windows", "natives-windows-64", "natives-windows-arm64"]
            elif sysname == "darwin":
                keys = ["natives-macos", "natives-macos-arm64", "natives-osx"]
            else:
                keys = ["natives-linux", "natives-linux-64"]
            rules = lib.get("rules")
            if rules and not _rules_allow(rules):
                keys = []
            for k in keys:
                if k in cl and cl[k].get("url"):
                    nat = cl[k]
                    fn = Path(urlparse(nat["url"]).path).name
                    add(nat["url"], lib_dir.joinpath(*gp, a, v, fn), nat.get("size", 0))
                    break

    items = list(downloads.values())
    safe_print(f"▶ {vid} 核心 {len(items)} 个文件")
    ok, fail = _parallel(items, force, vid, "core")

    if assets_mode != "none" and not _stop_event.is_set():
        ai = vj.get("assetIndex")
        if ai:
            idx_path = game_root / "assets" / "indexes" / f"{ai['id']}.json"
            idx_path.parent.mkdir(parents=True, exist_ok=True)
            if force or not (idx_path.exists() and idx_path.stat().st_size > 0):
                if not download_file(ai["url"], idx_path, force=force):
                    _set_proc_version(_PROC_TAG, None, None)
                    return {"version": vid, "ok": False, "error": "assetIndex失败"}
            try:
                with open(idx_path, "r", encoding="utf-8") as f:
                    idx_data = json.load(f)
            except Exception:
                idx_data = {}
            objects = idx_data.get("objects", {})
            if objects:
                prefixes = ASSETS_LANG if assets_mode == "lang" else ASSETS_FULL
                base = "https://resources.download.minecraft.net"
                adl = {}
                for ap, o in objects.items():
                    if assets_mode != "all" and not any(ap.startswith(p) for p in prefixes):
                        continue
                    h = o["hash"]
                    dest = game_root / "assets" / "objects" / h[:2] / h
                    k = str(dest).lower()
                    if k not in adl:
                        adl[k] = (f"{base}/{h[:2]}/{h}", dest, o.get("size", 0))
                safe_print(f"▶ {vid} assets({assets_mode}) {len(adl)} 个文件")
                a_ok, a_fail = _parallel(list(adl.values()), force, vid,
                                         f"assets-{assets_mode}")
                ok += a_ok
                fail += a_fail

    _set_proc_version(_PROC_TAG, None, None)
    return {"version": vid, "ok": fail == 0, "version_dir": str(vdir),
            "ok_count": ok, "fail_count": fail}


# ============================================================
# Worker（一次建全局线程池）
# ============================================================
def _worker_init(versions, game_root, jobs, force, assets_mode, race, verbose,
                 src_order, fails, cool, ok_a, act_a, h_lock,
                 files_done, files_fail, bytes_v, rr,
                 vp, vp_lock, pc, pc_lock, vd):
    global _PROC_TAG, _WORKER_EXECUTOR
    me = mp.current_process()
    try:
        n = me._identity[0] if me._identity else 0
    except Exception:
        n = 0
    _PROC_TAG = f"P{n}"
    _W.clear()
    _W.update(manifest={"versions": versions}, game_root=Path(game_root),
              jobs=jobs, force=force, assets_mode=assets_mode,
              race=race, verbose=verbose)
    _P.clear()
    _P.update(files_done=files_done, files_fail=files_fail, bytes=bytes_v, rr=rr,
              version_progress=vp, vp_lock=vp_lock,
              proc_current=pc, pc_lock=pc_lock, versions_done=vd)
    _health_init(src_order, fails, cool, ok_a, act_a, h_lock)
    _WORKER_EXECUTOR = ThreadPoolExecutor(max_workers=jobs)


def _worker_task(vid):
    try:
        return download_version(vid, _W["manifest"], _W["game_root"],
                                _W["force"], _W["assets_mode"])
    except Exception as e:
        return {"version": vid, "ok": False, "error": f"worker: {e}"}


# ============================================================
# 三行进度报告
# ============================================================
def _progress_reporter(stop_evt, total_versions, n_proc, src_order, interval=2.0):
    prev_bytes, prev_time = 0, time.time()
    while not stop_evt.wait(interval):
        now = time.time()
        curr_bytes = _P["bytes"].value
        speed = (curr_bytes - prev_bytes) / max(0.001, now - prev_time) / (1024 * 1024)
        prev_bytes, prev_time = curr_bytes, now

        fd = _P["files_done"].value
        ff = _P["files_fail"].value
        vd = _P["versions_done"].value if hasattr(_P["versions_done"], "value") else _P["versions_done"]

        with _P["pc_lock"]:
            pc = dict(_P["proc_current"])
        with _P["vp_lock"]:
            vp = dict(_P["version_progress"])

        parts = []
        for i in range(1, n_proc + 1):
            tag = f"P{i}"
            info = pc.get(tag)
            if info and info.get("version"):
                vid = info["version"]
                phase = info["phase"]
                vi = vp.get(vid, {})
                done = vi.get("done", 0)
                total = vi.get("total", 0)
                parts.append(f"{tag}:{vid}({done}/{total} {phase})")
            else:
                parts.append(f"{tag}:空闲")
        proc_line = "  [进程] " + "  ".join(parts)

        src_parts = []
        for i, name in enumerate(src_order):
            left = _H["cool"][i] - now
            if left > 0:
                src_parts.append(f"{name}:冷却{int(left)}s")
            else:
                src_parts.append(
                    f"{name}:⚡{_H['active'][i]} ✓{_H['ok'][i]} ✗{_H['fails'][i]}")
        src_line = "  [源]   " + "  ".join(src_parts)

        head = (f"  [进度] {vd}/{total_versions}个版本 | "
                f"✓{fd} ✗{ff} | {speed:5.2f} MB/s")
        sys.stderr.write(f"{head}\n{proc_line}\n{src_line}\n")
        sys.stderr.flush()


# ============================================================
# 压缩
# ============================================================
def compress_version(vdir, zip_path, level=6):
    try:
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=level) as zf:
            for f in sorted(vdir.rglob("*")):
                if f.is_file():
                    zf.write(f, f.relative_to(vdir))
        return True
    except Exception as e:
        safe_print(f"✗ 压缩失败 {vdir.name}: {e}")
        return False


# ============================================================
# main
# ============================================================
def main():
    p = argparse.ArgumentParser(
        description="MC 原版批量下载 v1.8（v1.6 全功能 + 连接复用提速）")
    p.add_argument("--source", nargs="+", default=["official"])
    p.add_argument("--version", nargs="+")
    p.add_argument("--latest", action="store_true")
    p.add_argument("--all", action="store_true")
    p.add_argument("--release", action="store_true")
    p.add_argument("--snapshot", action="store_true")
    p.add_argument("--type")
    p.add_argument("--from-date")
    p.add_argument("-o", "--output", default="./.minecraft")
    p.add_argument("-j", "--jobs", type=int, default=8)
    p.add_argument("--proc", type=int, default=1)
    p.add_argument("--race", action="store_true",
                   help="大文件(≥3MB)双源竞速，带宽翻倍换速度")
    p.add_argument("--assets", choices=["all", "lang", "none"], default="all")
    p.add_argument("--no-compress", action="store_true")
    p.add_argument("--keep-dir", action="store_true")
    p.add_argument("--compresslevel", type=int, default=6)
    p.add_argument("--force", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="打印每个失败文件")
    args = p.parse_args()

    src_order = []
    for s in args.source:
        if s not in DOWNLOAD_SOURCES:
            raise SystemExit(f"未知源: {s}")
        if s not in src_order:
            src_order.append(s)
    for s in DOWNLOAD_SOURCES:
        if s not in src_order:
            src_order.append(s)

    game_root = Path(args.output).expanduser().resolve()
    game_root.mkdir(parents=True, exist_ok=True)

    safe_print(f"源优先级: {' > '.join(src_order)}")
    safe_print(f"目录: {game_root}")
    safe_print(f"并发: {args.proc}进程 × {args.jobs}线程 = {args.proc * args.jobs}")
    safe_print(f"竞速: {'开' if args.race else '关'}（≥3MB双源竞速）")
    safe_print(f"连接复用: 开启（每线程每域名一条持久连接）")

    manifest = None
    for s in src_order:
        try:
            manifest = fetch_json(DOWNLOAD_SOURCES[s]["manifest"])
            safe_print(f"清单获取成功: {s}")
            break
        except Exception as e:
            safe_print(f"清单失败 {s}: {e}")
    if not manifest:
        raise SystemExit("所有源清单均失败")

    vids = select_versions(manifest, args)
    safe_print(f"选中 {len(vids)} 个版本: {', '.join(vids[:8])}"
               + (" ..." if len(vids) > 8 else ""))
    safe_print(f"assets: {args.assets}\n")

    n_src = len(src_order)
    manager = mp.Manager()
    fails = mp.Array('i', n_src, lock=False)
    cool = mp.Array('d', n_src, lock=False)
    ok_a = mp.Array('i', n_src, lock=False)
    act_a = mp.Array('i', n_src, lock=False)
    h_lock = mp.Lock()
    files_done = mp.Value('q', 0)
    files_fail = mp.Value('q', 0)
    bytes_v = mp.Value('q', 0)
    rr = mp.Value('i', 0)
    vd = mp.Value('i', 0)
    vp = manager.dict()
    vp_lock = manager.Lock()
    pc = manager.dict()
    pc_lock = manager.Lock()

    _P.clear()
    _P.update(files_done=files_done, files_fail=files_fail, bytes=bytes_v, rr=rr,
              version_progress=vp, vp_lock=vp_lock,
              proc_current=pc, pc_lock=pc_lock, versions_done=vd)
    _health_init(src_order, fails, cool, ok_a, act_a, h_lock)

    prog_stop = threading.Event()
    prog_thread = threading.Thread(
        target=_progress_reporter,
        args=(prog_stop, len(vids), args.proc, src_order), daemon=True)
    prog_thread.start()

    results = []
    try:
        if args.proc == 1:
            global _WORKER_EXECUTOR
            _WORKER_EXECUTOR = ThreadPoolExecutor(max_workers=args.jobs)
            _W.update(manifest={"versions": manifest.get("versions", [])},
                      game_root=game_root, jobs=args.jobs, force=args.force,
                      assets_mode=args.assets, race=args.race, verbose=args.verbose)
            for i, vid in enumerate(vids, 1):
                if _stop_event.is_set():
                    break
                try:
                    r = download_version(vid, manifest, game_root,
                                         args.force, args.assets)
                except KeyboardInterrupt:
                    _stop_event.set()
                    break
                results.append(r)
                with vd.get_lock():
                    vd.value += 1
                safe_print(f"[{i}/{len(vids)}] {'✓' if r.get('ok') else '✗'} {vid}")
            _WORKER_EXECUTOR.shutdown(wait=False)
        else:
            payload = manifest.get("versions", [])
            pool = mp.Pool(
                processes=args.proc,
                initializer=_worker_init,
                initargs=(payload, str(game_root), args.jobs, args.force,
                          args.assets, args.race, args.verbose, src_order,
                          fails, cool, ok_a, act_a, h_lock,
                          files_done, files_fail, bytes_v, rr,
                          vp, vp_lock, pc, pc_lock, vd))
            try:
                for r in pool.imap_unordered(_worker_task, vids):
                    results.append(r)
                    with vd.get_lock():
                        vd.value += 1
                    safe_print(f"[{len(results)}/{len(vids)}] "
                               f"{'✓' if r.get('ok') else '✗'} {r['version']}")
            except KeyboardInterrupt:
                _stop_event.set()
                safe_print(f"\n中断！已完成 {len(results)}/{len(vids)}")
                pool.terminate()
                pool.join()
            else:
                pool.close()
                pool.join()
    except KeyboardInterrupt:
        _stop_event.set()
    finally:
        prog_stop.set()
        prog_thread.join(timeout=5)

    if not args.no_compress:
        zd = game_root / "_archives"
        for r in results:
            if not r.get("ok") or not r.get("version_dir"):
                continue
            if _stop_event.is_set():
                break
            vd_path = Path(r["version_dir"])
            zp = zd / f"{r['version']}.zip"
            if compress_version(vd_path, zp, args.compresslevel):
                safe_print(f"✓ {r['version']}.zip")
                if not args.keep_dir:
                    shutil.rmtree(vd_path, ignore_errors=True)

    print("\n" + "=" * 60)
    ok = [r for r in results if r.get("ok")]
    bad = [r for r in results if not r.get("ok")]
    print(f"完成: {len(ok)}/{len(vids)}"
          + (f"（中断，未处理 {len(vids) - len(results)}）" if _stop_event.is_set() else ""))
    print(f"文件: ✓{files_done.value} ✗{files_fail.value}")
    for r in bad:
        print(f"  ✗ {r['version']}: {r.get('error', '?')}")


if __name__ == "__main__":
    mp.freeze_support()
    try:
        main()
    except KeyboardInterrupt:
        _stop_event.set()
        print("\n已退出。")
        sys.exit(1)