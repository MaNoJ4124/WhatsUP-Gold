# backend_enhanced.py  — GRID-SHIELD Backend (GUI-Compatible Edition)
# -*- coding: utf-8 -*-
"""
Fully compatible with Gui1_enhanced.py.
New features vs original backend.py:
  ✅ Per-node ping_interval override (0 = global)
  ✅ Node tags loaded & propagated
  ✅ custom_color loaded for groups
  ✅ Desktop tray alert signal via shared DB flag / network_meta
  ✅ tags & ping_interval read from DB and respected per-node
  ✅ GUI-new columns (tags, ping_interval, custom_color) handled gracefully
  ✅ reload_from_database picks up tags / ping_interval changes live
  ✅ DBUpdateWorker unchanged (queue-based, retry logic kept)
  ✅ CONSECUTIVE_FAILURES_THRESHOLD configurable via env
  ✅ Per-node ping_interval: each node pinged on its own schedule
"""

from concurrent.futures import ThreadPoolExecutor
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'extra'))

import time
import json
import threading
import queue
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any, Tuple
import icmplib
import pymysql

# ─────────────────────────────────────────────
# MySQL config
# ─────────────────────────────────────────────
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "appuser",
    "password": "app@123",
    "database": "projectdb",
}

def get_connection():
    return pymysql.connect(**MYSQL_CONFIG)

# ─────────────────────────────────────────────
# MySQL load/save helpers  (GUI-compatible)
# ─────────────────────────────────────────────
def load_network_from_mysql() -> Optional[Dict[str, Any]]:
    """Load full network from MySQL — includes tags, ping_interval, custom_color."""
    try:
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            # ── sheets ──────────────────────────────────────────────
            cursor.execute("SELECT name, zoom FROM sheets")
            sheets = [r['name'] for r in cursor.fetchall()] or ["Main"]

            # ── nodes (with new columns, graceful fallback) ──────────
            try:
                cursor.execute("""
                    SELECT id, name, ip1, ip2, shape, sheet_name,
                           base_x, base_y, size, starttime, total_down_time,
                           last_active, color, alive, previous_alive,
                           last_status_change, disabled, last_down_time, notes,
                           COALESCE(tags,'')         AS tags,
                           COALESCE(ping_interval,0) AS ping_interval
                    FROM nodes
                """)
            except Exception:
                # Fallback: old schema without new columns
                cursor.execute("""
                    SELECT id, name, ip1, ip2, shape, sheet_name,
                           base_x, base_y, size, starttime, total_down_time,
                           last_active, color, alive, previous_alive,
                           last_status_change, disabled, last_down_time, notes
                    FROM nodes
                """)

            nodes_by_id:   Dict[int, dict]  = {}
            nodes_by_name: Dict[str, dict]  = {}
            nodes: List[dict] = []

            for row in cursor.fetchall():
                nd = {
                    "name":               row['name'],
                    "ip1":                row.get('ip1')  or "",
                    "ip2":                row.get('ip2')  or "",
                    "shape":              row.get('shape') or "circle",
                    "sheet_name":         row.get('sheet_name') or "Main",
                    "x":                  row.get('base_x') or 100,
                    "y":                  row.get('base_y') or 100,
                    "base_x":             row.get('base_x') or 100,
                    "base_y":             row.get('base_y') or 100,
                    "size":               row.get('size')  or 30,
                    "starttime":          row.get('starttime') or 0,
                    "total_down_time":    row.get('total_down_time') or 0,
                    "last_active":        row.get('last_active') or "",
                    "color":              row.get('color')  or "red",
                    "alive":              row.get('alive'),
                    "previous_alive":     row.get('previous_alive'),
                    "last_status_change": row.get('last_status_change') or 0,
                    "disabled":           row.get('disabled'),
                    "last_down_time":     row.get('last_down_time') or 0,
                    "notes":              row.get('notes') or "",
                    # New GUI columns
                    "tags":               row.get('tags', '') or '',
                    "ping_interval":      int(row.get('ping_interval', 0) or 0),
                    "connections":        [],
                    "group":              None,
                }
                nodes.append(nd)
                nodes_by_id[row['id']] = nd
                nodes_by_name[row['name']] = nd

            # ── connections ──────────────────────────────────────────
            cursor.execute("""
                SELECT n1.name AS src, n2.name AS tgt
                FROM connections c
                JOIN nodes n1 ON c.node_id = n1.id
                JOIN nodes n2 ON c.target_node_id = n2.id
            """)
            for r in cursor.fetchall():
                src = nodes_by_name.get(r['src'])
                if src and r['tgt'] not in src['connections']:
                    src['connections'].append(r['tgt'])

            # ── groups (with custom_color) ───────────────────────────
            try:
                cursor.execute("""
                    SELECT id, name, sheet_name,
                           COALESCE(custom_color,'') AS custom_color
                    FROM node_groups
                """)
            except Exception:
                cursor.execute("SELECT id, name, sheet_name FROM node_groups")

            groups_by_id: Dict[int, dict] = {}
            groups: List[dict] = []
            for gr in cursor.fetchall():
                gd = {
                    "name":         gr['name'],
                    "sheet_name":   gr.get('sheet_name') or "Main",
                    "custom_color": gr.get('custom_color', '') or '',
                    "nodes":        [],
                }
                groups.append(gd)
                groups_by_id[gr['id']] = gd

            cursor.execute("""
                SELECT gm.group_id, n.name
                FROM group_members gm
                JOIN nodes n ON gm.node_id = n.id
            """)
            for gm in cursor.fetchall():
                gd = groups_by_id.get(gm['group_id'])
                if gd:
                    gd['nodes'].append(gm['name'])
                    nd = nodes_by_name.get(gm['name'])
                    if nd:
                        nd['group'] = gd['name']

            return {
                "sheets":        sheets,
                "current_sheet": "Main",
                "sheet_zoom":    {},
                "nodes":         nodes,
                "groups":        groups,
            }
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"[DB] load_network_from_mysql error: {e}")
        return None


def save_network_to_mysql(data: Dict[str, Any]) -> bool:
    """Full network save (used by GUI on structural changes)."""
    try:
        if not data.get("groups"):
            gmap: dict = {}
            for node in data.get("nodes", []):
                gn = node.get("group")
                if gn:
                    sn = node.get("sheet_name", "Main")
                    k = (gn, sn)
                    if k not in gmap:
                        gmap[k] = {"name": gn, "sheet_name": sn, "nodes": []}
                    gmap[k]["nodes"].append(node["name"])
            data["groups"] = list(gmap.values())

        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            cursor.execute("SET SQL_SAFE_UPDATES=0")
            for tbl in ("connections", "group_members", "nodes", "node_groups", "sheets"):
                cursor.execute(f"DELETE FROM {tbl}")
            cursor.execute("SET SQL_SAFE_UPDATES=1")

            for sn in data.get("sheets", ["Main"]):
                cursor.execute("INSERT INTO sheets(name,zoom) VALUES(%s,%s)", (sn, 1.0))

            node_id_map: Dict[str, int] = {}
            for nd in data.get("nodes", []):
                cursor.execute("""
                    INSERT INTO nodes(
                        name,ip1,ip2,shape,sheet_name,base_x,base_y,size,
                        starttime,total_down_time,last_active,color,
                        alive,previous_alive,last_status_change,disabled,
                        last_down_time,notes,tags,ping_interval
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    nd.get("name"), nd.get("ip1",""), nd.get("ip2",""),
                    nd.get("shape","circle"), nd.get("sheet_name","Main"),
                    nd.get("base_x", nd.get("x",100)), nd.get("base_y", nd.get("y",100)),
                    nd.get("size",30), nd.get("starttime",0), nd.get("total_down_time",0),
                    nd.get("last_active",""), nd.get("color","red"),
                    int(nd.get("alive",0)), int(nd.get("previous_alive",0)),
                    nd.get("last_status_change",0), int(nd.get("disabled",0)),
                    nd.get("last_down_time",0), nd.get("notes",""),
                    nd.get("tags",""), int(nd.get("ping_interval",0)),
                ))
                node_id_map[nd.get("name")] = cursor.lastrowid

            gname_to_id: Dict[str, int] = {}
            seen_groups: set = set()
            for gd in data.get("groups", []):
                gn, gsn = gd.get("name"), gd.get("sheet_name","Main")
                cc = gd.get("custom_color","") or ""
                if (gn, gsn) in seen_groups:
                    continue
                seen_groups.add((gn, gsn))
                cursor.execute(
                    "INSERT INTO node_groups(name,sheet_name,custom_color) VALUES(%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE sheet_name=VALUES(sheet_name),custom_color=VALUES(custom_color)",
                    (gn, gsn, cc)
                )
                cursor.execute("SELECT id FROM node_groups WHERE name=%s AND sheet_name=%s", (gn, gsn))
                row = cursor.fetchone()
                if row:
                    gname_to_id[gn] = row[0] if isinstance(row, tuple) else row['id']

            for gd in data.get("groups", []):
                gid = gname_to_id.get(gd.get("name"))
                if gid:
                    for nn in gd.get("nodes", []):
                        nid = node_id_map.get(nn)
                        if nid:
                            cursor.execute(
                                "INSERT INTO group_members(group_id,node_id) VALUES(%s,%s)",
                                (gid, nid)
                            )

            for nd in data.get("nodes", []):
                src_id = node_id_map.get(nd.get("name"))
                if src_id:
                    for tgt_name in nd.get("connections", []):
                        tgt_id = node_id_map.get(tgt_name)
                        if tgt_id:
                            cursor.execute(
                                "INSERT INTO connections(node_id,target_node_id) VALUES(%s,%s)",
                                (src_id, tgt_id)
                            )

            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            print(f"[DB] save_network_to_mysql error: {e}")
            return False
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"[DB] connection error in save: {e}")
        return False


# ─────────────────────────────────────────────
# Ping helpers
# ─────────────────────────────────────────────
def ping_ip(ip: str, timeout: int = 1) -> bool:
    try:
        host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
        return host.is_alive
    except icmplib.NameLookupError:
        return False
    except icmplib.SocketPermissionError:
        try:
            host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
            return host.is_alive
        except Exception:
            return False
    except Exception:
        return False


def ping_wrapped(ip: str, timeout: int = 1) -> Tuple[bool, float]:
    """Returns (success, rtt_seconds)."""
    try:
        host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
        if host.is_alive:
            return True, host.min_rtt / 1000.0
        return False, 0.0
    except icmplib.NameLookupError:
        return False, 0.0
    except icmplib.SocketPermissionError:
        try:
            host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
            if host.is_alive:
                return True, host.min_rtt / 1000.0
            return False, 0.0
        except Exception:
            return False, 0.0
    except Exception:
        return False, 0.0


# ─────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────
class NodeData:
    def __init__(self, data: dict):
        self.name:               str   = data.get("name", "Node")
        self.ip1:                str   = data.get("ip1", "")
        self.ip2:                str   = data.get("ip2", "")
        self.shape:              str   = data.get("shape", "circle")
        self.sheet_name:         str   = data.get("sheet_name", "Main")
        self.x:                  int   = data.get("x", 100)
        self.y:                  int   = data.get("y", 100)
        self.size:               int   = data.get("size", 35)
        self.starttime:          float = data.get("starttime", time.time())
        self.total_down_time:    int   = data.get("total_down_time", 0)
        self.last_active:        str   = data.get("last_active", "Never")
        self.color:              str   = data.get("color", "#e74c3c")
        self.alive:              bool  = bool(data.get("alive", False))
        self.previous_alive:     bool  = bool(data.get("previous_alive", False))
        self.last_status_change: float = data.get("last_status_change", time.time())
        self.disabled:           bool  = bool(data.get("disabled", False))
        self.last_down_time: Optional[float] = data.get("last_down_time") or None
        self.notes:              str   = data.get("notes", "")
        self.connections: List[str]   = data.get("connections", [])
        self.group:   Optional[str]   = data.get("group", None)
        # ── New fields (GUI-enhanced) ──────────────────────────────
        self.tags:           str = (data.get("tags", "") or "")
        self.ping_interval:  int = int(data.get("ping_interval", 0) or 0)  # 0 = global
        # ── Hysteresis counters (runtime only — never persisted to DB) ──
        # consec_fail: kitni baar consecutively sab IPs fail hue
        # consec_success: kitni baar consecutively koi IP succeed hua
        # DB mein yeh save nahi hote — reload par reset NAHI hona chahiye
        self.consec_success: int = data.get("consec_success", 0)
        self.consec_fail:    int = int(data.get("consec_fail",
                                   data.get("consecutive_failures", 0) or 0))
        self.consecutive_failures = self.consec_fail   # legacy alias
        # ── Per-node ping schedule tracker ────────────────────────
        self._next_ping_at: float = 0.0
        # ── Thread safety ─────────────────────────────────────────
        self._lock: threading.Lock = threading.Lock()

    # ── Runtime state fields (jinhe reload par preserve karna hai) ──
    RUNTIME_FIELDS = (
        "alive", "previous_alive", "color",
        "last_status_change", "last_active", "last_down_time",
        "total_down_time", "starttime",
        "consec_fail", "consec_success", "consecutive_failures",
        "_next_ping_at",
    )

    def preserve_runtime_into(self, target: "NodeData") -> None:
        """
        Copy sare runtime state fields self → target.
        Jab bhi nayi NodeData object banni ho lekin purana state
        preserve karna ho (e.g. load_data restarts), yeh call karo.
        """
        for field in self.RUNTIME_FIELDS:
            if hasattr(self, field):
                setattr(target, field, getattr(self, field))
        # _lock share mat karo — target ka apna lock rahega


class GroupData:
    def __init__(self, data: dict):
        self.name:         str        = data.get("name", "Group")
        self.sheet_name:   str        = data.get("sheet_name", "Main")
        self.nodes:  List[str]        = data.get("nodes", [])
        self.custom_color: str        = data.get("custom_color", "") or ""


# ─────────────────────────────────────────────
# File watcher (JSON fallback)
# ─────────────────────────────────────────────
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    class MyHandler(FileSystemEventHandler):
        def __init__(self, backend: "Backend"):
            self.backend = backend
            self._timer: Optional[threading.Timer] = None

        def on_modified(self, event):
            if event.is_directory:
                return
            if os.path.basename(event.src_path) != "network_data.json":
                return
            if getattr(self.backend, "_saving_json", False):
                return
            if time.time() < getattr(self.backend, "_ignore_fs_events_until", 0):
                return
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(0.3, self.backend.load_data)
            self._timer.start()

    _WATCHDOG_OK = True
except ImportError:
    _WATCHDOG_OK = False
    class MyHandler:  # type: ignore
        pass


# ─────────────────────────────────────────────
# DB Update Worker (queue-based, retry)
# ─────────────────────────────────────────────
class DBUpdateWorker(threading.Thread):
    """
    Dedicated background thread for DB status updates.
    Queue-based so no ping thread ever blocks on DB.
    Retry logic: 3 attempts with back-off.
    """
    def __init__(self, mysql_config: dict, log_fn):
        super().__init__(daemon=True, name="DBUpdateWorker")
        self._queue:        queue.Queue = queue.Queue()
        self._mysql_config: dict       = mysql_config
        self._log                      = log_fn
        self._stop_event: threading.Event = threading.Event()

    def enqueue(self, node_name: str, alive: int, color: str, last_active: str,
                last_status_change: float, total_down_time: int,
                last_down_time: Optional[float], previous_alive: int, disabled: int):
        self._queue.put({
            "node_name":          node_name,
            "alive":              alive,
            "color":              color,
            "last_active":        last_active,
            "last_status_change": last_status_change,
            "total_down_time":    total_down_time,
            "last_down_time":     last_down_time,
            "previous_alive":     previous_alive,
            "disabled":           disabled,
            "enqueued_at":        time.time(),
        })

    def stop(self):
        self._stop_event.set()
        self._queue.put(None)  # unblock get()

    def run(self):
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=2)
            except queue.Empty:
                continue
            if item is None:
                self._drain()
                break
            self._process(item)

    def _drain(self):
        while True:
            try:
                item = self._queue.get_nowait()
                if item is not None:
                    self._process(item)
            except queue.Empty:
                break

    def _process(self, item: dict):
        node_name = item["node_name"]
        for attempt in range(3):
            try:
                conn = pymysql.connect(**self._mysql_config)
                cur  = conn.cursor()
                try:
                    cur.execute("""
                        UPDATE nodes
                        SET alive=%s, color=%s, last_active=%s,
                            last_status_change=%s, total_down_time=%s,
                            last_down_time=%s, previous_alive=%s, disabled=%s
                        WHERE name=%s
                    """, (
                        item["alive"], item["color"], item["last_active"],
                        item["last_status_change"], item["total_down_time"],
                        item["last_down_time"], item["previous_alive"],
                        item["disabled"], node_name,
                    ))
                    # Bump network_meta so GUI detects the change
                    try:
                        cur.execute(
                            "INSERT INTO network_meta(id,last_modified,last_modified_by) "
                            "VALUES(1,%s,'backend') "
                            "ON DUPLICATE KEY UPDATE "
                            "last_modified=VALUES(last_modified),"
                            "last_modified_by=VALUES(last_modified_by)",
                            (time.time(),)
                        )
                    except Exception:
                        pass
                    conn.commit()
                    self._log(f"  DBWorker: {node_name} alive={item['alive']} color={item['color']}")
                    return
                finally:
                    cur.close()
                    conn.close()
            except Exception as e:
                if attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                else:
                    self._log(f"  DBWorker FAILED ({node_name}): {e}")


# ─────────────────────────────────────────────
# Per-node Ping Schedule
# ─────────────────────────────────────────────
class PingScheduler:
    """
    Tracks when each node should next be pinged.
    Respects per-node ping_interval (0 = global default).
    """
    def __init__(self, global_interval_sec: float):
        self.global_interval = global_interval_sec

    def interval_for(self, node: NodeData) -> float:
        """Return effective ping interval (seconds) for a node."""
        pi = getattr(node, 'ping_interval', 0) or 0
        return float(pi) if pi > 0 else self.global_interval

    def is_due(self, node: NodeData) -> bool:
        return time.time() >= node._next_ping_at

    def schedule_next(self, node: NodeData):
        node._next_ping_at = time.time() + self.interval_for(node)


# ─────────────────────────────────────────────
# Backend
# ─────────────────────────────────────────────
class Backend:
    def __init__(self):
        self._print = print
        self._shutdown_event = threading.Event()

        # icmplib check
        try:
            import icmplib as _il
        except ImportError:
            raise ImportError("icmplib is required: pip install icmplib")

        # MySQL connect
        self.db_conn   = None
        self.db_cursor = None
        try:
            self.db_conn   = pymysql.connect(**MYSQL_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            self.init_database()
        except Exception as e:
            self._print(f"[DB] MySQL connection failed: {e}")

        # DB worker
        self._db_worker = DBUpdateWorker(MYSQL_CONFIG, self._print)
        self._db_worker.start()

        # State
        self.nodes:       Dict[str, NodeData]  = {}
        self.groups:      Dict[str, GroupData] = {}
        self.sheets:      List[str]            = ["Main"]
        self.current_sheet: str               = "Main"
        self.sheet_zoom:  Dict[str, float]    = {}
        self._saving_json = False
        self._ignore_fs_events_until = 0.0
        self._nodes_lock = threading.Lock()

        # Global ping interval (ms from env, default 1000ms = 1s)
        try:
            self._monitor_interval_ms = max(
                100,
                int(os.environ.get("BACKEND_MONITOR_INTERVAL_MS", "1000"))
            )
        except Exception:
            self._monitor_interval_ms = 1000

        global_interval_sec = self._monitor_interval_ms / 1000.0

        # Per-node scheduler
        self._scheduler = PingScheduler(global_interval_sec)

        # Consecutive-failure threshold
        try:
            self.CONSECUTIVE_FAILURES_THRESHOLD = int(
                os.environ.get("CONSECUTIVE_FAILURES_THRESHOLD", "2")
            )
        except Exception:
            self.CONSECUTIVE_FAILURES_THRESHOLD = 2

        self.load_data()
        self._print(f"[Backend] {len(self.nodes)} nodes loaded. "
                    f"Global interval={global_interval_sec}s, "
                    f"threshold={self.CONSECUTIVE_FAILURES_THRESHOLD}")

        # File watcher
        if _WATCHDOG_OK:
            self.observer = Observer()
            self.observer.schedule(MyHandler(self), path=".", recursive=False)
            self.observer.start()
        else:
            self.observer = None

        # Thread pool
        try:
            cpu = os.cpu_count() or 1
            env_w = os.environ.get("BACKEND_PING_WORKERS")
            self._worker_count = int(env_w) if (env_w and env_w.isdigit()) else max(50, min(500, cpu * 50))
        except Exception:
            self._worker_count = 200
        self._print(f"[Backend] Thread pool size: {self._worker_count}")

        self.executor         = ThreadPoolExecutor(max_workers=self._worker_count)
        self._result_queue    = queue.Queue()
        self._pending_nodes:  Dict[str, int] = {}
        self._pending_lock    = threading.Lock()
        self._node_temp_results: Dict[str, list] = {}
        self._results_lock    = threading.Lock()
        self._cycle_in_progress = False
        self._cycle_changed   = False
        self._total_tasks     = 0

        # Tkinter GUI (kept for backward compat / headless run)
        self.root        = None
        self.tree        = None
        self.log_text    = None
        self._task_rows: Dict[str, tuple] = {}
        self._filter_query = ""

    # ─────────────────────────────────────────
    # DB init
    # ─────────────────────────────────────────
    def init_database(self):
        if not self.db_cursor:
            return
        self.db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS node_events (
                id              INT AUTO_INCREMENT PRIMARY KEY,
                plant_name      VARCHAR(255),
                event_time      DATETIME,
                starttime       DATETIME,
                last_down_time  DATETIME,
                total_down_time INT,
                uptime          INT,
                disabled        BOOLEAN DEFAULT FALSE,
                user_id         INT
            )
        """)
        # Ensure new columns exist (idempotent)
        for ddl in [
            "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS tags VARCHAR(500) DEFAULT ''",
            "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS ping_interval INT DEFAULT 0",
            "ALTER TABLE node_groups ADD COLUMN IF NOT EXISTS custom_color VARCHAR(32) DEFAULT ''",
        ]:
            try:
                self.db_cursor.execute(ddl)
            except Exception:
                pass
        self.db_conn.commit()
        self._print("[DB] Database initialised.")

    # ─────────────────────────────────────────
    # Load data
    # ─────────────────────────────────────────
    def load_data(self):
        data = None
        try:
            data = load_network_from_mysql()
        except Exception as e:
            self._print(f"[DB] load error: {e}")

        if not data:
            self._print("[Backend] No MySQL data — starting empty.")
            data = {"sheets": ["Main"], "current_sheet": "Main",
                    "sheet_zoom": {}, "nodes": [], "groups": []}

        self.sheets        = data.get("sheets", ["Main"])
        self.current_sheet = data.get("current_sheet", getattr(self, "current_sheet", "Main"))
        self.sheet_zoom    = data.get("sheet_zoom", {})

        raw_nodes = [nd for nd in data.get("nodes", []) if "name" in nd]

        # ── Preserve runtime state for existing nodes ───────────────
        # load_data() is called at startup AND sometimes on file-watcher events.
        # Creating brand-new NodeData() objects would wipe consec_fail, alive,
        # color, etc.  Instead, keep existing NodeData objects and only update
        # config fields — same logic as reload_from_database().
        CONFIG_FIELDS = [
            ("ip1", "ip1", None), ("ip2", "ip2", None),
            ("shape", "shape", None), ("sheet_name", "sheet_name", None),
            ("size", "size", None), ("notes", "notes", None),
            ("disabled", "disabled", "bool"),
            ("tags", "tags", None),
            ("ping_interval", "ping_interval", "int"),
        ]
        existing_nodes: Dict[str, "NodeData"] = getattr(self, "nodes", {})
        new_nodes: Dict[str, "NodeData"] = {}
        for nd in raw_nodes:
            name = nd["name"]
            if name in existing_nodes:
                # Node already known — update only config fields, keep ALL runtime state
                # (consec_fail, alive, color, timestamps, ping schedule)
                node = existing_nodes[name]
                for db_field, attr, cast in CONFIG_FIELDS:
                    new_val = nd.get(db_field, getattr(node, attr, None))
                    if cast == "bool":
                        new_val = bool(new_val)
                    elif cast == "int":
                        new_val = int(new_val or 0)
                    setattr(node, attr, new_val)
                new_nodes[name] = node
            else:
                # Brand-new node — full NodeData init (consec_fail=0 correct yahan)
                new_nodes[name] = NodeData(nd)

        self.nodes  = new_nodes
        self.groups = {gd["name"]: GroupData(gd) for gd in data.get("groups", []) if "name" in gd}

        # Resolve base positions
        szoom = data.get("sheet_zoom", {})
        for nd in raw_nodes:
            node = self.nodes[nd["name"]]
            try:
                if "base_x" in nd or "base_y" in nd:
                    node.base_x = float(nd.get("base_x", nd.get("x", 100)))
                    node.base_y = float(nd.get("base_y", nd.get("y", 100)))
                else:
                    z = szoom.get(node.sheet_name)
                    if z:
                        node.base_x = float(nd.get("x", 100)) / float(z)
                        node.base_y = float(nd.get("y", 100)) / float(z)
                    else:
                        node.base_x = float(nd.get("x", 100))
                        node.base_y = float(nd.get("y", 100))
            except Exception:
                node.base_x = float(nd.get("x", 100))
                node.base_y = float(nd.get("y", 100))

        # Clean up stale refs
        for n in self.nodes.values():
            n.connections = [c for c in n.connections if c in self.nodes]
            if n.group:
                g = self.groups.get(n.group)
                if not g or getattr(g, 'sheet_name', 'Main') != n.sheet_name:
                    n.group = None
        for g in self.groups.values():
            g.nodes = [nn for nn in g.nodes
                       if nn in self.nodes and self.nodes[nn].sheet_name == g.sheet_name]

        self._print(f"[Backend] Loaded: {len(self.nodes)} nodes, "
                    f"{len(self.groups)} groups, sheets={self.sheets}")

    # ─────────────────────────────────────────
    # Reload (live config changes from GUI)
    # ─────────────────────────────────────────
    def reload_from_database(self):
        """
        Non-destructive reload — GUI se aaye config changes pick up karta hai.

        Sirf YE fields DB se overwrite hoti hain (UI se change hoti hain):
            ip1, ip2, shape, sheet_name, size, notes, disabled,
            tags, ping_interval

        YE fields KABHI reset NAHI hoti (runtime state — in-memory only):
            consec_fail, consec_success, consecutive_failures   <-- BUG FIX
            alive, previous_alive, color
            last_status_change, last_active, last_down_time, total_down_time
            starttime, _next_ping_at, _lock
        """
        # (db_field, node_attr, cast)  — sirf config fields
        CONFIG_FIELDS = [
            ("ip1",           "ip1",           None),
            ("ip2",           "ip2",           None),
            ("shape",         "shape",         None),
            ("sheet_name",    "sheet_name",    None),
            ("size",          "size",          None),
            ("notes",         "notes",         None),
            ("disabled",      "disabled",      "bool"),
            ("tags",          "tags",          None),
            ("ping_interval", "ping_interval", "int"),
        ]

        try:
            db_data = load_network_from_mysql()
            if not db_data:
                return

            db_nodes = {nd["name"]: nd for nd in db_data.get("nodes", [])}

            # ── 1. Update / remove existing nodes ───────────────────
            to_remove = []
            for name, node in self.nodes.items():
                if name not in db_nodes:
                    to_remove.append(name)
                    continue

                db = db_nodes[name]
                changed = []

                for db_field, attr, cast in CONFIG_FIELDS:
                    new_val = db.get(db_field, getattr(node, attr, None))
                    if cast == "bool":
                        new_val = bool(new_val)
                    elif cast == "int":
                        new_val = int(new_val or 0)
                    if getattr(node, attr, None) != new_val:
                        setattr(node, attr, new_val)
                        changed.append(db_field)

                if changed:
                    # consec_fail is NOT in CONFIG_FIELDS — will never be touched
                    self._print(
                        f"[Reload] '{name}' config updated: {changed} | "
                        f"consec_fail={node.consec_fail} alive={node.alive} "
                        f"(runtime state preserved)"
                    )

            for name in to_remove:
                del self.nodes[name]
                self._print(f"[Reload] '{name}' removed (deleted from DB)")

            # ── 2. Add genuinely new nodes ───────────────────────────
            for name, db in db_nodes.items():
                if name not in self.nodes:
                    # Brand-new node: runtime state starts fresh (consec_fail=0 correct)
                    self.nodes[name] = NodeData(db)
                    self._print(f"[Reload] '{name}' added from DB (new node, consec_fail=0)")

            # ── 3. Refresh groups ────────────────────────────────────
            self.groups = {
                gd["name"]: GroupData(gd)
                for gd in db_data.get("groups", []) if "name" in gd
            }

        except Exception as e:
            self._print(f"[Reload] Warning: {e}")

    # ─────────────────────────────────────────
    # DB status update (queued)
    # ─────────────────────────────────────────
    def update_node_status_in_db(self, node_name: str):
        node = self.nodes.get(node_name)
        if not node:
            return
        try:
            self._db_worker.enqueue(
                node_name=node_name,
                alive=int(node.alive),
                color=node.color,
                last_active=node.last_active,
                last_status_change=node.last_status_change,
                total_down_time=node.total_down_time,
                last_down_time=node.last_down_time,
                previous_alive=int(node.previous_alive),
                disabled=int(node.disabled),
            )
        except Exception as e:
            self._print(f"[DB] enqueue error ({node_name}): {e}")

    # ─────────────────────────────────────────
    # Save / sync
    # ─────────────────────────────────────────
    def save_data(self):
        """Status updates happen via DBUpdateWorker. This only syncs deletions."""
        try:
            self._sync_deleted_nodes_from_db()
        except Exception as e:
            self._print(f"[Sync] Warning: {e}")

    def _sync_deleted_nodes_from_db(self):
        try:
            conn = get_connection()
            cur  = conn.cursor()
            try:
                cur.execute("SELECT name FROM nodes")
                db_names = {r[0] for r in cur.fetchall()}
            finally:
                cur.close()
                conn.close()
            stale = [n for n in list(self.nodes) if n not in db_names]
            for name in stale:
                del self.nodes[name]
                self._print(f"[Sync] Removed stale '{name}'")
        except Exception as e:
            self._print(f"[Sync] _sync_deleted_nodes_from_db error: {e}")

    # ─────────────────────────────────────────
    # Monitoring cycle  (PER-NODE INTERVAL)
    # ─────────────────────────────────────────
    def monitor_all(self):
        """
        Main monitoring entry point.
        Only pings nodes whose _next_ping_at has elapsed (per-node interval support).
        """
        if self._shutdown_event.is_set():
            self._print("[Monitor] Shutdown — stopping.")
            return

        self.reload_from_database()

        if self._cycle_in_progress:
            self._print("[Monitor] Previous cycle still running — skipping.")
            self._reschedule_monitor()
            return

        self._cycle_in_progress = True
        self._cycle_changed     = False

        with self._results_lock:
            self._node_temp_results.clear()
        with self._pending_lock:
            self._pending_nodes.clear()

        self._total_tasks    = 0
        self._last_cycle_start = time.time()
        now = time.time()

        self._print(f"\n[Monitor] Cycle {datetime.now().strftime('%H:%M:%S')} — "
                    f"{len(self.nodes)} nodes total")

        if self.root:
            try:
                self.root.after(0, lambda: [self.tree.delete(i) for i in self.tree.get_children()])
            except Exception:
                pass

        nodes_to_ping = {}
        for node_name, node in self.nodes.items():
            # Per-node interval check
            if not self._scheduler.is_due(node):
                continue  # skip — not its time yet
            nodes_to_ping[node_name] = node

        if not nodes_to_ping:
            self._print("[Monitor] No nodes due this cycle.")
            self._cycle_in_progress = False
            self._reschedule_monitor()
            return

        self._print(f"[Monitor] Pinging {len(nodes_to_ping)} due nodes "
                    f"(skipped {len(self.nodes)-len(nodes_to_ping)} not-due)")

        for node_name, node in nodes_to_ping.items():
            # Schedule next ping immediately (before actual ping completes)
            self._scheduler.schedule_next(node)

            if node.disabled:
                self._print(f"[Monitor] '{node_name}' disabled — forcing DOWN")
                with node._lock:
                    if node.color != "#e74c3c" or node.alive:
                        node.previous_alive = node.alive
                        node.color  = "#e74c3c"
                        node.alive  = False
                        node.consec_fail = 0
                        self._cycle_changed = True
                        self.update_node_status_in_db(node_name)
                continue

            ip_list = [ip for ip in (node.ip1, node.ip2) if ip]
            if not ip_list:
                continue

            with self._pending_lock:
                self._pending_nodes[node_name] = len(ip_list)

            for ip_idx, ip in enumerate(ip_list, 1):
                task_id = f"{node_name}_IP{ip_idx}"
                row_id  = self._update_task_row(task_id, node_name, ip, "Pending",
                                                0, getattr(node, "consec_fail", 0))
                try:
                    fut = self.executor.submit(ping_wrapped, ip)
                    info = (node_name, ip_idx, ip, row_id)
                    fut.add_done_callback(
                        lambda f, _i=info: self._result_queue.put((_i, f))
                    )
                    self._total_tasks += 1
                except Exception as e:
                    self._print(f"[Monitor] submit failed ({ip}): {e}")
                    with self._pending_lock:
                        if node_name in self._pending_nodes:
                            self._pending_nodes[node_name] -= 1

        self._print(f"[Monitor] {self._total_tasks} ping tasks submitted")
        if self.root:
            self.root.after(50, self._process_ping_queue)
        else:
            self._process_ping_queue_sync()

    def _reschedule_monitor(self):
        """Schedule next monitor_all call."""
        if self._shutdown_event.is_set():
            return
        if self.root:
            try:
                self.root.after(self._monitor_interval_ms, self.monitor_all)
            except Exception:
                pass
        else:
            t = threading.Timer(self._monitor_interval_ms / 1000.0, self.monitor_all)
            t.daemon = True
            t.start()

    # ─────────────────────────────────────────
    # Result processing (Tkinter after loop)
    # ─────────────────────────────────────────
    def _process_ping_queue(self):
        """Process completed futures — called from Tkinter after loop."""
        self._drain_result_queue()

        with self._pending_lock:
            remaining = sum(self._pending_nodes.values()) if self._pending_nodes else 0

        if remaining > 0:
            if self.root:
                self.root.after(50, self._process_ping_queue)
            return

        self._finish_cycle()

    def _process_ping_queue_sync(self):
        """Synchronous variant used when no Tkinter root (headless mode)."""
        from concurrent.futures import wait as _fw, ALL_COMPLETED
        # Wait until result_queue has all results
        deadline = time.time() + 30
        while True:
            self._drain_result_queue()
            with self._pending_lock:
                remaining = sum(self._pending_nodes.values()) if self._pending_nodes else 0
            if remaining <= 0:
                break
            if time.time() > deadline:
                self._print("[Monitor] Timeout waiting for pings.")
                break
            time.sleep(0.05)
        self._finish_cycle()

    def _drain_result_queue(self):
        """Consume all available items from result queue."""
        while True:
            try:
                info, fut = self._result_queue.get_nowait()
            except queue.Empty:
                break

            node_name, ip_idx, ip, row_id = info
            try:
                success, ping_time = fut.result()
            except Exception as e:
                success, ping_time = False, 0.0
                self._print(f"[Ping] {node_name}/{ip} exception: {e}")

            node = self.nodes.get(node_name)

            # Immediate UP path
            if node and success and not getattr(node, "disabled", False):
                with node._lock:
                    node.consec_fail = 0
                    node.consecutive_failures = 0   # legacy alias sync
                    node.color = "#01fd6a"
                    if not node.alive:
                        node.previous_alive = node.alive
                        node.alive = True
                        node.last_active = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                        node.last_status_change = time.time()
                        node.last_down_time = None
                        self._cycle_changed = True
                        self._print(f"[Ping] '{node_name}' RECOVERED (IP{ip_idx} reachable)")
                        self.update_node_status_in_db(node_name)

            # Update GUI row
            status = "SUCCESS" if success else "FAILED"
            rtt_ms = ping_time * 1000
            self._update_task_row(row_id, node_name, ip, status, rtt_ms,
                                  getattr(node, "consec_fail", 0) if node else 0)

            # Store result
            with self._results_lock:
                self._node_temp_results.setdefault(node_name, []).append(
                    (success, ping_time, ip_idx, ip, row_id)
                )

            # Decrement pending
            finalize = False
            with self._pending_lock:
                if node_name in self._pending_nodes:
                    self._pending_nodes[node_name] -= 1
                    if self._pending_nodes[node_name] <= 0:
                        del self._pending_nodes[node_name]
                        finalize = True

            if finalize:
                self._finalize_node(node_name)

    def _finish_cycle(self):
        elapsed = time.time() - getattr(self, '_last_cycle_start', time.time())
        self._print(f"[Monitor] Cycle done in {elapsed:.3f}s")
        if self._cycle_changed:
            self.save_data()
        self._cycle_in_progress = False
        self._reschedule_monitor()

    # ─────────────────────────────────────────
    # Finalize node (called when all IPs done)
    # ─────────────────────────────────────────
    def _finalize_node(self, node_name: str):
        node = self.nodes.get(node_name)
        if not node:
            return

        with self._results_lock:
            results = list(self._node_temp_results.get(node_name, []))

        any_success = any(r[0] for r in results)

        with node._lock:
            if not hasattr(node, "consec_fail"):
                node.consec_fail = 0

            if any_success:
                node.consec_fail = 0
                node.consecutive_failures = 0   # legacy alias sync
                if not node.alive:
                    if node.last_down_time:
                        node.total_down_time += time.time() - node.last_down_time
                        node.last_down_time   = None
                    node.previous_alive     = node.alive
                    node.color              = "#01fd6a"
                    node.alive              = True
                    node.last_active        = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    node.last_status_change = time.time()
                    self._cycle_changed     = True
                    self._print(f"[Finalize] '{node_name}' UP")
                    self.update_node_status_in_db(node_name)
                elif node.color != "#01fd6a":
                    node.color = "#01fd6a"
                    self.update_node_status_in_db(node_name)
            else:
                node.consec_fail += 1
                node.consecutive_failures = node.consec_fail   # legacy alias sync
                self._print(
                    f"[Finalize] '{node_name}' fail "
                    f"{node.consec_fail}/{self.CONSECUTIVE_FAILURES_THRESHOLD} "
                    f"alive={node.alive}"
                )
                if node.consec_fail >= self.CONSECUTIVE_FAILURES_THRESHOLD:
                    if node.alive:
                        node.previous_alive     = node.alive
                        node.alive              = False
                        node.color              = "#f22009"
                        node.last_down_time     = time.time()
                        node.last_status_change = time.time()
                        self._cycle_changed     = True
                        self._print(
                            f"[Finalize] '{node_name}' DOWN after "
                            f"{node.consec_fail} consecutive failures"
                        )
                        self.update_node_status_in_db(node_name)
                    else:
                        if node.color != "#f22009":
                            node.color = "#f22009"
                            self.update_node_status_in_db(node_name)

        # DB event log
        if self._cycle_changed:
            try:
                self.log_node_event(
                    node.name, node.starttime, node.last_down_time,
                    node.total_down_time, node.alive, node.disabled
                )
            except Exception:
                pass

    # ─────────────────────────────────────────
    # Tkinter GUI helpers (optional)
    # ─────────────────────────────────────────
    def setup_gui(self):
        try:
            import tkinter as tk
            from tkinter import ttk
        except ImportError:
            self._print("[GUI] tkinter not available — headless mode")
            return

        self.root = tk.Tk()
        self.root.title("GRID-SHIELD Backend Monitor")
        self.root.geometry("900x600")

        log_frame = tk.Frame(self.root)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text = tk.Text(log_frame, height=10, wrap=tk.WORD)
        sb = tk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=sb.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        tree_frame = tk.Frame(self.root)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        cols = ("ID", "Node", "IP", "Status", "RTT (ms)", "Fail Count",
                "Tags", "Interval(s)")     # ← new columns shown
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=10)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=100 if c not in ("Tags","ID") else 140)
        sb2 = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb2.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb2.pack(side=tk.RIGHT, fill=tk.Y)

        # Search bar
        top = tk.Frame(self.root)
        top.pack(fill=tk.X, padx=6, pady=(4, 0))
        self.search_var = tk.StringVar()
        se = tk.Entry(top, textvariable=self.search_var, width=30)
        se.pack(side=tk.RIGHT, padx=2)
        se.bind('<Return>', lambda _: self.do_search())
        tk.Button(top, text="Search", command=self.do_search).pack(side=tk.RIGHT, padx=2)
        tk.Button(top, text="Clear",  command=self.clear_search).pack(side=tk.RIGHT, padx=2)
        self._filter_label = tk.Label(top, text="All tasks")
        self._filter_label.pack(side=tk.RIGHT, padx=8)
        self._search_entry = se

        def _log(msg):
            def _upd():
                ts = datetime.now().strftime("%H:%M:%S")
                self.log_text.insert(tk.END, f"[{ts}] {msg}\n")
                self.log_text.see(tk.END)
            self.root.after(0, _upd)

        self._print = _log
        self._db_worker._log = _log
        self._print("[GUI] Backend monitor GUI ready.")

    def _update_task_row(self, row_id: str, node_name: str, ip: str,
                         status: str, rtt_ms: float, consec_fail: int) -> str:
        rtt_disp = f"{rtt_ms:.2f}" if rtt_ms > 0 else "0.00"
        node = self.nodes.get(node_name)
        tags_str = getattr(node, 'tags', '') or ''
        interval_str = str(self._scheduler.interval_for(node)) if node else '-'
        row_vals = (row_id, node_name, ip, status, rtt_disp, consec_fail,
                    tags_str, interval_str)
        self._task_rows[row_id] = row_vals

        if not self.tree:
            return row_id

        q = (self._filter_query or "").strip().lower()
        matches = not q or (q in node_name.lower()) or (q in ip.lower()) or (q in tags_str.lower())

        try:
            if matches:
                if self.tree.exists(row_id):
                    self.tree.item(row_id, values=row_vals)
                else:
                    try:
                        self.tree.insert("", "end", iid=row_id, values=row_vals)
                    except Exception:
                        alt = f"{row_id}_{int(time.time()*1000)}"
                        self.tree.insert("", "end", iid=alt, values=row_vals)
                        self._task_rows[alt] = row_vals
                        return alt
            else:
                if self.tree.exists(row_id):
                    self.tree.delete(row_id)
        except Exception:
            pass
        return row_id

    def _apply_filter(self):
        if not self.tree:
            return
        q = (self._filter_query or "").strip().lower()
        try:
            for it in self.tree.get_children():
                self.tree.delete(it)
        except Exception:
            pass
        count = 0
        for rid, vals in self._task_rows.items():
            node_name = vals[1]; ip = vals[2]; tags = vals[6] if len(vals) > 6 else ''
            if not q or q in node_name.lower() or q in ip.lower() or q in tags.lower():
                try:
                    self.tree.insert("", "end", iid=rid, values=vals)
                    count += 1
                except Exception:
                    pass
        try:
            lbl = f"Filter: '{self._filter_query}' ({count})" if self._filter_query else "All tasks"
            self._filter_label.config(text=lbl)
        except Exception:
            pass

    def do_search(self, event=None):
        self._filter_query = (self.search_var.get() or "").strip()
        self._apply_filter()

    def clear_search(self):
        self.search_var.set("")
        self._filter_query = ""
        self._apply_filter()

    # ─────────────────────────────────────────
    # node_events log
    # ─────────────────────────────────────────
    def log_node_event(self, plant_name: str, starttime: float,
                       last_down_time: Optional[float], total_down_time: int,
                       alive: bool, disabled: bool):
        if not (self.db_cursor and self.db_conn):
            return
        try:
            event_time     = datetime.now()
            starttime_dt   = datetime.fromtimestamp(starttime)   if starttime      else None
            last_down_dt   = datetime.fromtimestamp(last_down_time) if last_down_time else None
            uptime = int(time.time() - starttime - total_down_time) if alive and not disabled else 0
            self.db_cursor.execute("""
                INSERT INTO node_events
                    (plant_name,event_time,starttime,last_down_time,
                     total_down_time,uptime,disabled,user_id)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            """, (plant_name, event_time, starttime_dt, last_down_dt,
                  total_down_time, uptime, disabled, None))
            self.db_conn.commit()
        except Exception as e:
            self._print(f"[DB] log_node_event error: {e}")

    # ─────────────────────────────────────────
    # Headless run (no Tkinter)
    # ─────────────────────────────────────────
    def run_headless(self):
        """Run backend without any GUI — useful when GUI is the PyQt5 app."""
        self._print("[Backend] Running in headless mode (no Tkinter GUI).")
        self._print(f"[Backend] Monitor interval: {self._monitor_interval_ms}ms | "
                    f"Threshold: {self.CONSECUTIVE_FAILURES_THRESHOLD}")
        try:
            while not self._shutdown_event.is_set():
                self.monitor_all_sync()
                time.sleep(self._monitor_interval_ms / 1000.0)
        except KeyboardInterrupt:
            self._print("\n[Backend] Stopped by user.")
        finally:
            self.shutdown()

    def monitor_all_sync(self):
        """Synchronous single cycle — for headless mode."""
        if self._shutdown_event.is_set():
            return
        if self._cycle_in_progress:
            return
        self._cycle_in_progress = True
        self._cycle_changed     = False
        with self._results_lock:
            self._node_temp_results.clear()
        with self._pending_lock:
            self._pending_nodes.clear()
        self._total_tasks      = 0
        self._last_cycle_start = time.time()

        self.reload_from_database()

        nodes_to_ping = {n: nd for n, nd in self.nodes.items()
                         if self._scheduler.is_due(nd)}

        if not nodes_to_ping:
            self._cycle_in_progress = False
            return

        self._print(f"[Monitor] Pinging {len(nodes_to_ping)} nodes "
                    f"(interval={self._monitor_interval_ms}ms)")

        for node_name, node in nodes_to_ping.items():
            self._scheduler.schedule_next(node)
            if node.disabled:
                with node._lock:
                    if node.color != "#e74c3c" or node.alive:
                        node.previous_alive = node.alive
                        node.color = "#e74c3c"; node.alive = False
                        node.consec_fail = 0; self._cycle_changed = True
                        self.update_node_status_in_db(node_name)
                continue
            ip_list = [ip for ip in (node.ip1, node.ip2) if ip]
            if not ip_list:
                continue
            with self._pending_lock:
                self._pending_nodes[node_name] = len(ip_list)
            for ip_idx, ip in enumerate(ip_list, 1):
                task_id = f"{node_name}_IP{ip_idx}"
                try:
                    fut = self.executor.submit(ping_wrapped, ip)
                    info = (node_name, ip_idx, ip, task_id)
                    fut.add_done_callback(
                        lambda f, _i=info: self._result_queue.put((_i, f))
                    )
                    self._total_tasks += 1
                except Exception as e:
                    self._print(f"[Monitor] submit error ({ip}): {e}")
                    with self._pending_lock:
                        if node_name in self._pending_nodes:
                            self._pending_nodes[node_name] -= 1

        self._process_ping_queue_sync()

    # ─────────────────────────────────────────
    # Tkinter run
    # ─────────────────────────────────────────
    def run(self):
        self.setup_gui()
        def _start():
            if not self._shutdown_event.is_set():
                try:
                    self.monitor_all()
                except Exception as e:
                    self._print(f"[Monitor] start error: {e}")
        self.root.after(1000, _start)
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        try:
            self.root.mainloop()
        except Exception as e:
            self._print(f"[GUI] error: {e}")

    # ─────────────────────────────────────────
    # Shutdown
    # ─────────────────────────────────────────
    def shutdown(self):
        self._print("[Backend] Shutting down…")
        self._shutdown_event.set()
        try:
            self._db_worker.stop()
            self._db_worker.join(timeout=5)
        except Exception:
            pass
        try:
            if self.observer:
                self.observer.stop()
                self.observer.join(timeout=2)
        except Exception:
            pass
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass
        try:
            if self.db_cursor: self.db_cursor.close()
            if self.db_conn:   self.db_conn.close()
        except Exception:
            pass
        self._print("[Backend] Shutdown complete.")

    def on_closing(self):
        self.shutdown()
        try:
            self.root.destroy()
        except Exception:
            pass

    def __del__(self):
        try:
            if hasattr(self, 'observer') and self.observer:
                self.observer.stop()
        except Exception:
            pass
        try:
            if hasattr(self, '_db_worker'):
                self._db_worker.stop()
        except Exception:
            pass
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except Exception:
            pass


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="GRID-SHIELD Backend")
    parser.add_argument("--headless", action="store_true",
                        help="Run without Tkinter GUI (use with PyQt5 frontend)")
    args = parser.parse_args()

    try:
        import icmplib
    except ImportError:
        print("ERROR: pip install icmplib")
        sys.exit(1)

    be = Backend()
    try:
        if args.headless:
            be.run_headless()
        else:
            be.run()
    except KeyboardInterrupt:
        be._print("\n[Backend] Stopped by user.")
        be.shutdown()