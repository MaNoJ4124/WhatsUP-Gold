import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'extra'))
print('sys.path:', sys.path)
# backend.py
# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'extra'))
import time
import json
import os
import threading
import queue
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import icmplib  # Changed from ping3 to icmplib
import pymysql

# --- MySQL driver functions (inlined from extra/mysql_driver.py) ---
from typing import Any

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "appuser",
    "password": "app@123",
    "database": "projectdb",
}

def get_connection():
    """Create and return a MySQL connection."""
    return pymysql.connect(**MYSQL_CONFIG)

def load_network_from_mysql() -> Optional[Dict[str, Any]]:
    """Load the entire network structure from MySQL normalized tables."""
    try:
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute("SELECT name, zoom FROM sheets")
            sheets_rows = cursor.fetchall()
            sheets = [row['name'] for row in sheets_rows]
            if not sheets:
                sheets = ["Main"]

            cursor.execute("""
                SELECT id, name, ip1, ip2, shape, sheet_name, 
                       base_x, base_y, size, starttime, total_down_time,
                       last_active, color, alive, previous_alive,
                       last_status_change, disabled, last_down_time, notes
                FROM nodes
            """)
            nodes_rows = cursor.fetchall()
            nodes_by_id = {}
            nodes_by_name = {}
            nodes = []
            for row in nodes_rows:
                node_dict = {
                    "name": row['name'],
                    "ip1": row['ip1'] or "",
                    "ip2": row['ip2'] or "",
                    "shape": row['shape'] or "circle",
                    "sheet_name": row['sheet_name'] or "Main",
                    "x": row['base_x'] or 100,
                    "y": row['base_y'] or 100,
                    "base_x": row['base_x'] or 100,
                    "base_y": row['base_y'] or 100,
                    "size": row['size'] or 30,
                    "starttime": row['starttime'] or 0,
                    "total_down_time": row['total_down_time'] or 0,
                    "last_active": row['last_active'] or "",
                    "color": row['color'] or "red",
                    "alive": row['alive'],
                    "previous_alive": row['previous_alive'],
                    "last_status_change": row['last_status_change'] or 0,
                    "disabled": row['disabled'],
                    "last_down_time": row['last_down_time'] or 0,
                    "notes": row['notes'] or "",
                    "connections": [],
                    "group": None,
                }
                nodes.append(node_dict)
                nodes_by_id[row['id']] = node_dict
                nodes_by_name[row['name']] = node_dict

            cursor.execute("""
                SELECT n1.name as source_name, n2.name as target_name
                FROM connections c
                JOIN nodes n1 ON c.node_id = n1.id
                JOIN nodes n2 ON c.target_node_id = n2.id
            """)
            connections_rows = cursor.fetchall()
            for conn_row in connections_rows:
                source_name = conn_row['source_name']
                target_name = conn_row['target_name']
                if source_name in nodes_by_name:
                    if target_name not in nodes_by_name[source_name]['connections']:
                        nodes_by_name[source_name]['connections'].append(target_name)

            cursor.execute("SELECT id, name, sheet_name FROM node_groups")
            groups_rows = cursor.fetchall()
            groups_by_id = {}
            groups = []
            for group_row in groups_rows:
                group_dict = {
                    "name": group_row['name'],
                    "sheet_name": group_row['sheet_name'] or "Main",
                    "nodes": [],
                }
                groups.append(group_dict)
                groups_by_id[group_row['id']] = group_dict

            cursor.execute("""
                SELECT gm.group_id, n.name
                FROM group_members gm
                JOIN nodes n ON gm.node_id = n.id
            """)
            group_member_rows = cursor.fetchall()
            for gm_row in group_member_rows:
                group_id = gm_row['group_id']
                node_name = gm_row['name']
                if group_id in groups_by_id:
                    groups_by_id[group_id]['nodes'].append(node_name)
                    if node_name in nodes_by_name:
                        nodes_by_name[node_name]['group'] = groups_by_id[group_id]['name']

            network = {
                "sheets": sheets,
                "current_sheet": "Main",
                "sheet_zoom": {},
                "nodes": nodes,
                "groups": groups,
            }
            return network
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error loading network from MySQL: {e}")
        return None

def save_network_to_mysql(data: Dict[str, Any]) -> bool:
    """Save the network structure to MySQL normalized tables."""
    try:
        # --- Patch: reconstruct groups from nodes if missing or empty ---
        if not data.get("groups"):
            group_map = {}
            for node in data.get("nodes", []):
                group_name = node.get("group")
                if group_name:
                    sheet_name = node.get("sheet_name", "Main")
                    key = (group_name, sheet_name)
                    if key not in group_map:
                        group_map[key] = {"name": group_name, "sheet_name": sheet_name, "nodes": []}
                    group_map[key]["nodes"].append(node["name"])
            data["groups"] = list(group_map.values())
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            # disable safe updates so the following blanket deletes will run
            cursor.execute("SET SQL_SAFE_UPDATES = 0")
            cursor.execute("DELETE FROM connections")
            cursor.execute("DELETE FROM group_members")
            cursor.execute("DELETE FROM nodes")
            cursor.execute("DELETE FROM node_groups")
            cursor.execute("DELETE FROM sheets")
            # restore safe updates immediately after the deletes
            cursor.execute("SET SQL_SAFE_UPDATES = 1")
            sheets = data.get("sheets", ["Main"])
            for sheet_name in sheets:
                cursor.execute(
                    "INSERT INTO sheets (name, zoom) VALUES (%s, %s)",
                    (sheet_name, 1.0)
                )
            nodes_data = data.get("nodes", [])
            node_name_to_id = {}
            for node in nodes_data:
                node_name = node.get("name")
                cursor.execute("""
                    INSERT INTO nodes (
                        name, ip1, ip2, shape, sheet_name, base_x, base_y,
                        size, starttime, total_down_time, last_active, color,
                        alive, previous_alive, last_status_change, disabled,
                        last_down_time, notes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    node.get("name"),
                    node.get("ip1", ""),
                    node.get("ip2", ""),
                    node.get("shape", "circle"),
                    node.get("sheet_name", "Main"),
                    node.get("base_x", node.get("x", 100)),
                    node.get("base_y", node.get("y", 100)),
                    node.get("size", 30),
                    node.get("starttime", 0),
                    node.get("total_down_time", 0),
                    node.get("last_active", ""),
                    node.get("color", "red"),
                    node.get("alive", 0),
                    node.get("previous_alive", 0),
                    node.get("last_status_change", 0),
                    node.get("disabled", 0),
                    node.get("last_down_time", 0),
                    node.get("notes", ""),
                ))
                node_name_to_id[node_name] = cursor.lastrowid
            groups_data = data.get("groups", [])
            group_name_to_id = {}
            unique_groups = {}
            for group in groups_data:
                group_name = group.get("name")
                sheet_name = group.get("sheet_name", "Main")
                composite_key = (group_name, sheet_name)
                if composite_key not in unique_groups:
                    unique_groups[composite_key] = group
            for (group_name, sheet_name), group in unique_groups.items():
                cursor.execute(
                    "INSERT INTO node_groups (name, sheet_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE sheet_name=VALUES(sheet_name)",
                    (group_name, sheet_name)
                )
                cursor.execute("SELECT id FROM node_groups WHERE name=%s AND sheet_name=%s", (group_name, sheet_name))
                result = cursor.fetchone()
                if result:
                    group_id_tuple = result if isinstance(result, tuple) else (result['id'],)
                    group_name_to_id[group_name] = group_id_tuple[0]
            for group in groups_data:
                group_name = group.get("name")
                group_id = group_name_to_id.get(group_name)
                if group_id:
                    for node_name in group.get("nodes", []):
                        node_id = node_name_to_id.get(node_name)
                        if node_id:
                            cursor.execute(
                                "INSERT INTO group_members (group_id, node_id) VALUES (%s, %s)",
                                (group_id, node_id)
                            )
            for node in nodes_data:
                source_node_name = node.get("name")
                source_node_id = node_name_to_id.get(source_node_name)
                if source_node_id:
                    for target_node_name in node.get("connections", []):
                        target_node_id = node_name_to_id.get(target_node_name)
                        if target_node_id:
                            cursor.execute(
                                "INSERT INTO connections (node_id, target_node_id) VALUES (%s, %s)",
                                (source_node_id, target_node_id)
                            )
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            print(f"Error saving network to MySQL: {e}")
            return False
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
        return False
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools
import tkinter as tk
from tkinter import ttk
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'extra'))

# When building a frozen executable with PyInstaller, cryptography's
# native OpenSSL binding is sometimes omitted which causes SHA-2
# authentication to fail at runtime. Touch the binding here so
# PyInstaller's static analysis is more likely to include it.
try:
    import cryptography.hazmat.bindings.openssl as _openssl_binding  # type: ignore
    _ = _openssl_binding.Binding()
except Exception:
    _openssl_binding = None

# -----------------------------
# Utility helpers
# -----------------------------
def atomic_write_json(filename: str, data: dict):
    """
    Write JSON atomically using temp file + rename to prevent partial reads.
    Critical for preventing race conditions with GUI reading while backend writes.
    """
    import shutil
    temp_filename = f"{filename}.tmp"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Write to temporary file first
            with open(temp_filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            
            # Atomic replace (rename is atomic on Windows and Unix)
            if os.path.exists(filename):
                # On Windows, need to remove target first
                if sys.platform == 'win32':
                    try:
                        os.remove(filename)
                    except FileNotFoundError:
                        pass
            
            os.rename(temp_filename, filename)
            return  # Success
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.1 * (attempt + 1))  # Back off
            else:
                # Final attempt failed, clean up temp file
                try:
                    os.remove(temp_filename)
                except Exception:
                    pass
                raise

def ping_ip(ip: str, timeout: int = 1) -> bool:
    """
    Ping a single IP with timeout using icmplib.
    Returns True if successful, False otherwise.
    """
    try:
        # Use icmplib's ping function with single packet
        host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
        return host.is_alive
    except icmplib.NameLookupError:
        # Hostname resolution failed
        return False
    except icmplib.SocketPermissionError:
        # No permission to create raw socket, try with privileged=False
        try:
            host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
            return host.is_alive
        except Exception:
            return False
    except Exception:
        return False

from typing import Tuple
def ping_wrapped(ip: str, timeout: int = 1) -> Tuple[bool, float]:
    """
    Wrapped ping function using icmplib that returns success and RTT.
    Returns (success: bool, rtt_seconds: float)
    """
    try:
        # Ping with icmplib
        host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
        
        if host.is_alive:
            # Convert RTT from milliseconds to seconds
            # Use min_rtt for most accurate measurement
            rtt_seconds = host.min_rtt / 1000.0
            return True, rtt_seconds
        else:
            return False, 0.0
            
    except icmplib.NameLookupError:
        # DNS resolution failed
        return False, 0.0
    except icmplib.SocketPermissionError:
        # Try again without raw socket privileges
        try:
            host = icmplib.ping(ip, count=1, timeout=timeout, privileged=False)
            if host.is_alive:
                rtt_seconds = host.min_rtt / 1000.0
                return True, rtt_seconds
            return False, 0.0
        except Exception:
            return False, 0.0
    except icmplib.SocketAddressError:
        # Invalid IP address
        return False, 0.0
    except icmplib.ICMPError as e:
        # ICMP-specific error
        return False, 0.0
    except Exception:
        return False, 0.0

# -----------------------------
# Data classes
# -----------------------------
class NodeData:
    def __init__(self, data: dict):
        self.name: str = data.get("name", "Node")
        self.ip1: str = data.get("ip1", "")
        self.ip2: str = data.get("ip2", "")
        self.shape: str = data.get("shape", "circle")
        self.sheet_name: str = data.get("sheet_name", "Main")
        self.x: int = data.get("x", 100)
        self.y: int = data.get("y", 100)
        self.size: int = data.get("size", 35)
        self.starttime: float = data.get("starttime", time.time())
        self.total_down_time: int = data.get("total_down_time", 0)
        self.last_active: str = data.get("last_active", "Never")
        self.color: str = data.get("color", "#e74c3c")
        self.alive: bool = data.get("alive", False)
        self.previous_alive: bool = data.get("previous_alive", False)
        self.last_status_change: float = data.get("last_status_change", time.time())
        self.disabled: bool = data.get("disabled", False)
        self.last_down_time: Optional[float] = data.get("last_down_time", None)
        self.notes: str = data.get("notes", "")
        self.connections: List[str] = data.get("connections", [])
        self.group: Optional[str] = data.get("group", None)
        # Hysteresis counters to reduce false positives
        # `consec_success` counts consecutive cycles with any successful ping
        # `consec_fail` counts consecutive cycles with all pings failed
        # For compatibility, accept either `consec_fail` or legacy `consecutive_failures`.
        self.consec_success: int = data.get("consec_success", 0)
        self.consec_fail: int = int(data.get("consec_fail", data.get("consecutive_failures", 0) or 0))
        # keep legacy attribute for any external code expecting it
        self.consecutive_failures = self.consec_fail
        # FIX: Per-node lock to prevent race conditions during status updates
        self._lock: threading.Lock = threading.Lock()

class GroupData:
    def __init__(self, data: dict):
        self.name: str = data.get("name", "Group")
        self.sheet_name: str = data.get("sheet_name", "Main")
        self.nodes: List[str] = data.get("nodes", [])

# -----------------------------
# File watcher with debounce
# -----------------------------
class MyHandler(FileSystemEventHandler):
    def __init__(self, backend: "Backend"):
        self.backend = backend
        self._debounce_timer = None

    def on_modified(self, event):
        if event.is_directory:
            return
        if os.path.basename(event.src_path) != "network_data.json":
            return
        if getattr(self.backend, "_saving_json", False) or time.time() < getattr(self.backend, "_ignore_fs_events_until", 0):
            return
        if self._debounce_timer:
            self._debounce_timer.cancel()
        self._debounce_timer = threading.Timer(0.3, self.backend.load_data)
        self._debounce_timer.start()

# -----------------------------
# DB update queue worker
# -----------------------------
class DBUpdateWorker(threading.Thread):
    """
    FIX: Dedicated background thread that processes DB update requests from a queue.
    This ensures DB updates are never dropped and don't block the ping threads.
    Each item in the queue is a dict with node snapshot data to write.
    Uses retry logic for transient connection failures.
    """
    def __init__(self, mysql_config: dict, log_fn):
        super().__init__(daemon=True, name="DBUpdateWorker")
        self._queue: queue.Queue = queue.Queue()
        self._mysql_config = mysql_config
        self._log = log_fn
        self._stop_event = threading.Event()

    def enqueue(self, node_name: str, alive: int, color: str, last_active: str,
                last_status_change: float, total_down_time: int,
                last_down_time: Optional[float], previous_alive: int, disabled: int):
        """Add a node status update to the queue (non-blocking)."""
        self._queue.put({
            "node_name": node_name,
            "alive": alive,
            "color": color,
            "last_active": last_active,
            "last_status_change": last_status_change,
            "total_down_time": total_down_time,
            "last_down_time": last_down_time,
            "previous_alive": previous_alive,
            "disabled": disabled,
            "enqueued_at": time.time(),
        })

    def stop(self):
        self._stop_event.set()
        # Unblock the get() call
        self._queue.put(None)

    def run(self):
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=2)
            except queue.Empty:
                continue
            if item is None:
                # Sentinel — drain remaining items then exit
                self._drain()
                break
            self._process(item)

    def _drain(self):
        """Process any remaining items after stop signal."""
        while True:
            try:
                item = self._queue.get_nowait()
                if item is not None:
                    self._process(item)
            except queue.Empty:
                break

    def _process(self, item: dict):
        """Execute DB update with retry."""
        node_name = item["node_name"]
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = pymysql.connect(**self._mysql_config)
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        UPDATE nodes
                        SET alive = %s, color = %s, last_active = %s,
                            last_status_change = %s, total_down_time = %s,
                            last_down_time = %s, previous_alive = %s,
                            disabled = %s
                        WHERE name = %s
                    """, (
                        item["alive"],
                        item["color"],
                        item["last_active"],
                        item["last_status_change"],
                        item["total_down_time"],
                        item["last_down_time"],
                        item["previous_alive"],
                        item["disabled"],
                        node_name,
                    ))
                    # Bump network_meta so GUI detects the change
                    try:
                        cursor.execute(
                            "INSERT INTO network_meta (id, last_modified, last_modified_by) "
                            "VALUES (1, %s, 'backend') "
                            "ON DUPLICATE KEY UPDATE "
                            "last_modified = VALUES(last_modified), "
                            "last_modified_by = VALUES(last_modified_by)",
                            (time.time(),)
                        )
                    except Exception:
                        pass  # network_meta may not exist on older DBs
                    conn.commit()
                    self._log(f"  DBWorker: updated {node_name} (alive={item['alive']}, color={item['color']})")
                    return  # Success
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 0.2 * (attempt + 1)
                    self._log(f"  DBWorker: retry {attempt+1}/{max_retries} for {node_name}: {e}")
                    time.sleep(wait)
                else:
                    self._log(f"  DBWorker: FAILED all retries for {node_name}: {e}")

# -----------------------------
# Backend
# -----------------------------
class Backend:
    def __init__(self):
        self._print = print  # Initial print function
        self._shutdown_event = threading.Event()  # Signal to gracefully stop monitoring

        # Check if icmplib is available
        try:
            import icmplib
            icmplib_available = True
        except ImportError:
            icmplib_available = False
            self._print("ERROR: 'icmplib' package is required but not installed.")
            self._print("Please install it with: pip install icmplib")
            raise ImportError("icmplib package is required. Install with: pip install icmplib")

        # Attempt to connect to MySQL if possible
        self.db_conn = None
        self.db_cursor = None
        try:
            import cryptography  # type: ignore
            crypto_available = True
        except Exception:
            crypto_available = False

        if not crypto_available:
            # Running without 'cryptography' (common in minimal frozen builds). Do not raise.
            self._print("Warning: 'cryptography' package not found. Skipping MySQL connection.")
            self._print("To enable DB features, install 'cryptography' in your build/runtime environment (e.g. 'pip install cryptography') or change the MySQL user's authentication plugin to 'mysql_native_password'.")
        else:
            try:
                self.db_conn = pymysql.connect(
                    host="localhost",
                    user="appuser",
                    password="app@123",
                    database="projectdb",
                )
                self.db_cursor = self.db_conn.cursor()
                self.init_database()
            except RuntimeError as e:
                msg = str(e).lower()
                if "cryptography" in msg and ("sha256" in msg or "caching_sha2_password" in msg or "sha256_password" in msg):
                    self._print("MySQL authentication requires the 'cryptography' package for SHA-2 auth methods.")
                    self._print("Fix: install it in your environment (e.g. 'pip install cryptography') or change the MySQL user's authentication plugin to 'mysql_native_password'.")
                    self.db_conn = None
                    self.db_cursor = None
                else:
                    self._print(f"Failed to connect to MySQL: {e}")
                    raise
            except pymysql.Error as e:
                self._print(f"Failed to connect to MySQL: {str(e)}")
                raise

        # FIX: Start dedicated DB update worker thread
        self._db_worker = DBUpdateWorker(MYSQL_CONFIG, self._print)
        self._db_worker.start()

        self.nodes: Dict[str, NodeData] = {}
        self.groups: Dict[str, GroupData] = {}
        self.sheets: List[str] = ["Main"]
        self.current_sheet: str = "Main"
        self.sheet_zoom: Dict[str, float] = {}
        self._saving_json = False
        self._ignore_fs_events_until = 0.0

        # FIX: Global lock for self.nodes dict access from multiple threads
        self._nodes_lock = threading.Lock()

        self.load_data()
        self._print("Backend initialized. Using icmplib for accurate ICMP pings. Starting file watcher and monitor loop.")
        self.observer = Observer()
        handler = MyHandler(self)
        self.observer.schedule(handler, path=".", recursive=False)
        self.observer.start()

        # Configure worker pool for pinging (persistent across cycles to avoid thread churn)
        try:
            cpu = os.cpu_count() or 1
            env_workers = os.environ.get("BACKEND_PING_WORKERS")
            if env_workers and env_workers.isdigit():
                self._worker_count = int(env_workers)
            else:
                self._worker_count = max(50, min(500, cpu * 50))
        except Exception:
            self._worker_count = 1200
        self._print(f"Using {self._worker_count} worker threads for pinging (env BACKEND_PING_WORKERS to override)")
        self.executor = ThreadPoolExecutor(max_workers=self._worker_count)
        self._result_queue = queue.Queue()
        self._cycle_in_progress = False
        self._pending_nodes = {}     # node_name -> remaining pending IP count
        self._pending_lock = threading.Lock()  # FIX: lock for _pending_nodes counter
        self._node_temp_results = {} # node_name -> list of (success, ping_time, ip_idx, ip, row_id)
        self._results_lock = threading.Lock()  # FIX: lock for _node_temp_results
        self._cycle_changed = False
        self._total_tasks = 0
        # Monitoring interval (milliseconds) - default 5000ms (5 seconds)
        try:
            self._monitor_interval_ms = int(os.environ.get("BACKEND_MONITOR_INTERVAL_MS", "1000"))
            if self._monitor_interval_ms < 100:
                self._monitor_interval_ms = 2000
        except Exception:
            self._monitor_interval_ms = 1000 
        # Threshold for consecutive failures to mark DOWN
        try:
            self.CONSECUTIVE_FAILURES_THRESHOLD = int(os.environ.get("CONSECUTIVE_FAILURES_THRESHOLD", 3))
        except Exception:
            self.CONSECUTIVE_FAILURES_THRESHOLD = 3

    def init_database(self):
        self.db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS node_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                plant_name VARCHAR(255),
                event_time DATETIME,
                starttime DATETIME,
                last_down_time DATETIME,
                total_down_time INT,
                uptime INT,
                disabled BOOLEAN DEFAULT FALSE,
                user_id INT
            )
        ''')
        self.db_conn.commit()
        self._print("Database initialized.")

    def setup_gui(self):
        self.root = tk.Tk()
        self.root.title("Network Node Monitor (ICMP)")
        self.root.geometry("900x600")

        # Log text widget
        log_frame = tk.Frame(self.root)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = tk.Text(log_frame, height=10, wrap=tk.WORD)
        scrollbar_log = tk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar_log.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_log.pack(side=tk.RIGHT, fill=tk.Y)

        # Treeview for current tasks
        tree_frame = tk.Frame(self.root)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        columns = ("ID", "Node", "IP", "Status", "RTT (ms)", "Consecutive Failures")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=10)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120)
        scrollbar_tree = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar_tree.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_tree.pack(side=tk.RIGHT, fill=tk.Y)

        # Override _print to use GUI log
        def _log(msg):
            def update_log():
                timestamp = datetime.now().strftime("%H:%M:%S")
                self.log_text.insert(tk.END, f"[{timestamp}] {msg}\n")
                self.log_text.see(tk.END)
            self.root.after(0, update_log)

        self._print = _log
        # FIX: Update DB worker's log function to match GUI logger
        self._db_worker._log = _log
        self._print("GUI setup complete. Using icmplib for accurate ICMP ping measurements.")
        self._print("Monitoring tasks will be shown in table below with RTT times.")

        # --- Search UI (top-right) ---
        top_bar = tk.Frame(self.root)
        top_bar.pack(fill=tk.X, padx=6, pady=(4, 0))
        tk.Frame(top_bar).pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.search_var = tk.StringVar()
        search_entry = tk.Entry(top_bar, textvariable=self.search_var, width=30)
        search_entry.pack(side=tk.RIGHT, padx=(4, 2))
        search_entry.bind('<Return>', lambda _e: self.do_search())
        search_btn = tk.Button(top_bar, text="Search", command=self.do_search)
        search_btn.pack(side=tk.RIGHT, padx=(2, 2))
        clear_btn = tk.Button(top_bar, text="Clear", command=self.clear_search)
        clear_btn.pack(side=tk.RIGHT, padx=(2, 8))
        self._filter_label = tk.Label(top_bar, text="Showing all tasks", anchor="e")
        self._filter_label.pack(side=tk.RIGHT, padx=(4, 8))

        self._task_rows: Dict[str, tuple] = {}
        self._filter_query: str = ""
        self._search_entry = search_entry

    def load_data(self):
        try:
            data = load_network_from_mysql()
        except Exception as e:
            self._print(f"Error loading from MySQL: {e}")
            data = None

        if not data:
            self._print("No data found in MySQL. Starting with empty in-memory data.")
            data = {
                "sheets": ["Main"],
                "current_sheet": "Main",
                "sheet_zoom": {},
                "nodes": [],
                "groups": [],
            }

        try:
            self.sheets = data.get("sheets", ["Main"])
            self.current_sheet = data.get("current_sheet", getattr(self, "current_sheet", "Main"))
            self.sheet_zoom = data.get("sheet_zoom", getattr(self, "sheet_zoom", {}))

            raw_nodes = [nd for nd in data.get("nodes", []) if "name" in nd]
            self.nodes = {nd["name"]: NodeData(nd) for nd in raw_nodes}
            self.groups = {gd["name"]: GroupData(gd) for gd in data.get("groups", []) if "name" in gd}
            saved_sheet_zoom = data.get("sheet_zoom", {})
            for nd in raw_nodes:
                node = self.nodes[nd["name"]]
                try:
                    if ("base_x" in nd) or ("base_y" in nd):
                        node.base_x = float(nd.get("base_x", nd.get("x", 100)))
                        node.base_y = float(nd.get("base_y", nd.get("y", 100)))
                    else:
                        z = saved_sheet_zoom.get(node.sheet_name)
                        if z:
                            node.base_x = float(nd.get("x", 100)) / float(z)
                            node.base_y = float(nd.get("y", 100)) / float(z)
                        else:
                            node.base_x = float(nd.get("x", 100))
                            node.base_y = float(nd.get("y", 100))
                except Exception:
                    node.base_x = float(nd.get("x", 100))
                    node.base_y = float(nd.get("y", 100))

            for n in self.nodes.values():
                n.connections = [c for c in n.connections if c in self.nodes]
                if n.group:
                    grp = self.groups.get(n.group)
                    if not grp or getattr(grp, 'sheet_name', 'Main') != n.sheet_name:
                        n.group = None
            for g in self.groups.values():
                g.nodes = [n for n in g.nodes if n in self.nodes and self.nodes[n].sheet_name == getattr(g, 'sheet_name', 'Main')]
            self._print(f"Data loaded: {len(self.nodes)} nodes, {len(self.groups)} groups, sheets: {self.sheets}")
        except json.JSONDecodeError as e:
            self._print(f"JSON decode error: {e}")
        except Exception as e:
            self._print(f"Error loading data: {str(e)}")

    def update_node_status_in_db(self, node_name: str):
        """
        FIX: Instead of a direct synchronous DB call (which could block/fail silently),
        enqueue the update to the dedicated DB worker thread which handles retries.
        This ensures no update is ever dropped even under DB connection pressure.
        """
        try:
            node = self.nodes.get(node_name)
            if not node:
                return
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
            self._print(f"  DB update enqueued: {node_name} (alive={int(node.alive)}, color={node.color})")
        except Exception as e:
            self._print(f"  Warning: update_node_status_in_db enqueue error for {node_name}: {e}")

    def save_data(self):
        """
        BUG FIX: Do NOT use save_network_to_mysql() (DELETE ALL + re-INSERT).
        Status updates are done in real-time via update_node_status_in_db().
        This function only syncs self.nodes with MySQL to drop stale in-memory entries.
        """
        try:
            self._sync_deleted_nodes_from_db()
        except Exception as e:
            self._print(f"Warning: sync after save failed: {e}")

    def _sync_deleted_nodes_from_db(self):
        """
        Remove from self.nodes any node that no longer exists in MySQL.
        Called after each monitoring cycle to catch deletions mid-cycle.
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT name FROM nodes")
                db_names = {row[0] for row in cursor.fetchall()}
            finally:
                cursor.close()
                conn.close()

            stale = [n for n in list(self.nodes.keys()) if n not in db_names]
            for name in stale:
                del self.nodes[name]
                self._print(f"  Sync: removed stale node '{name}' (deleted from DB mid-cycle)")
        except Exception as e:
            self._print(f"  Warning: _sync_deleted_nodes_from_db error: {e}")

    def reload_from_database(self):
        """Reload node configuration from MySQL database to pick up GUI changes."""
        try:
            db_data = load_network_from_mysql()
            if not db_data:
                return

            db_nodes = {nd["name"]: nd for nd in db_data.get("nodes", [])}
            db_groups = {gd["name"]: gd for gd in db_data.get("groups", [])}

            nodes_to_remove = []
            for node_name, node_obj in self.nodes.items():
                if node_name in db_nodes:
                    db_node = db_nodes[node_name]
                    changed_fields = []
                    for field, attr in [("ip1", "ip1"), ("ip2", "ip2"),
                                        ("shape", "shape"), ("sheet_name", "sheet_name"),
                                        ("size", "size"), ("notes", "notes")]:
                        new_val = db_node.get(field, getattr(node_obj, attr, None))
                        if getattr(node_obj, attr, None) != new_val:
                            setattr(node_obj, attr, new_val)
                            changed_fields.append(field)
                    if changed_fields:
                        self._print(f"Updated node '{node_name}' fields: {changed_fields}")
                else:
                    nodes_to_remove.append(node_name)

            for node_name in nodes_to_remove:
                del self.nodes[node_name]
                self._print(f"Removed node '{node_name}' (deleted from database)")

            for node_name, db_node in db_nodes.items():
                if node_name not in self.nodes:
                    self.nodes[node_name] = NodeData(db_node)
                    self._print(f"Added node '{node_name}' from database")

            self.groups = {
                gd["name"]: GroupData(gd)
                for gd in db_data.get("groups", [])
                if "name" in gd
            }

        except Exception as e:
            self._print(f"Warning: Failed to reload from database: {e}")

    def monitor_all(self):
        if self._shutdown_event.is_set():
            self._print("Shutdown signal received, stopping monitor loop.")
            return

        self.reload_from_database()

        if self._cycle_in_progress:
            self._print("Previous monitoring cycle still in progress; skipping this run.")
            return

        self._cycle_in_progress = True
        self._cycle_changed = False

        # FIX: Clear shared state under locks before starting new cycle
        with self._results_lock:
            self._node_temp_results.clear()
        with self._pending_lock:
            self._pending_nodes.clear()

        self._total_tasks = 0
        self._last_cycle_start = time.time()

        self._print(f"\n--- Monitoring Cycle Start: {datetime.now().strftime('%H:%M:%S')} ---")
        def clear_tree():
            for item in self.tree.get_children():
                self.tree.delete(item)
        self.root.after(0, clear_tree)

        for node_name, node in self.nodes.items():
            self._print(f"\nPreparing concurrent pings for node: {node_name} (disabled: {node.disabled})")
            if node.disabled:
                self._print(f"  Node is disabled - forcing down state (no ping).")
                # FIX: Use per-node lock when mutating node state
                with node._lock:
                    if node.color != "#e74c3c" or node.alive:
                        old_color = node.color
                        old_alive = node.alive
                        node.previous_alive = old_alive
                        node.color = "#e74c3c"
                        node.alive = False
                        node.consec_fail = 0
                        self._cycle_changed = True
                        self._print(f"  Status change: Alive {old_alive} -> False, Color {old_color} -> #e74c3c")
                        self.update_node_status_in_db(node_name)
                continue

            ip_list = [ip for ip in (node.ip1, node.ip2) if ip]
            if not ip_list:
                continue

            # FIX: Set pending count atomically
            with self._pending_lock:
                self._pending_nodes[node_name] = len(ip_list)

            for ip_idx, ip in enumerate(ip_list, start=1):
                task_id = f"{node_name}_IP{ip_idx}"
                row_id = self._update_task_row(task_id, node_name, ip, "Pending", 0, getattr(node, "consec_fail", 0))
                self._print(f"  Submitting concurrent ping for IP{ip_idx} '{ip}'")
                try:
                    future = self.executor.submit(ping_wrapped, ip)
                    info = (node_name, ip_idx, ip, row_id)
                    future.add_done_callback(lambda fut, info=info: self._result_queue.put((info, fut)))
                    self._total_tasks += 1
                except Exception as e:
                    self._print(f"  Failed to submit ping task for {ip}: {e}")
                    # FIX: Decrement pending count if submit failed so node is not stuck
                    with self._pending_lock:
                        if node_name in self._pending_nodes:
                            self._pending_nodes[node_name] -= 1

        self._print(f"\nAll ping tasks submitted ({self._total_tasks} tasks). Processing results asynchronously.")
        self.root.after(50, self._process_ping_queue)

    def _process_ping_queue(self):
        """
        Process completed ping futures from the queue.
        FIX: All node state mutations are now done under per-node locks.
        FIX: Duplicate UP transitions are prevented by checking alive state under lock.
        FIX: _pending_nodes counter is decremented under lock to prevent race.
        """
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
                self._print(f"  Ping for {node_name} {ip} raised exception: {e}")

            # FIX: Use per-node lock for ALL node state changes to prevent race conditions
            node = self.nodes.get(node_name)
            if node and success and not getattr(node, "disabled", False):
                with node._lock:
                    # FIX: Only trigger UP transition once (check alive under lock)
                    node.consec_fail = 0
                    node.color = "#01fd6a"
                    if not node.alive:
                        old_alive = node.alive
                        node.previous_alive = old_alive
                        node.alive = True
                        node.last_active = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                        node.last_status_change = time.time()
                        node.last_down_time = None
                        self._cycle_changed = True
                        self._print(f"  Node {node_name} immediate recovery (IP{ip_idx} reachable).")
                        self.update_node_status_in_db(node_name)

            # Update tree row
            try:
                status = "SUCCESS" if success else "FAILED"
                rtt_ms = ping_time * 1000
                try:
                    self._update_task_row(row_id, node_name, ip, status, rtt_ms, getattr(node, "consec_fail", 0) if node else 0)
                except Exception:
                    pass
            except Exception:
                pass

            # FIX: Store result under results lock
            with self._results_lock:
                lst = self._node_temp_results.setdefault(node_name, [])
                lst.append((success, ping_time, ip_idx, ip, row_id))

            # FIX: Decrement pending count and finalize under lock
            finalize_node = False
            with self._pending_lock:
                if node_name in self._pending_nodes:
                    self._pending_nodes[node_name] -= 1
                    if self._pending_nodes[node_name] <= 0:
                        del self._pending_nodes[node_name]
                        finalize_node = True

            if finalize_node:
                self._finalize_node(node_name)

        # Check if any pending nodes remain
        with self._pending_lock:
            remaining = sum(self._pending_nodes.values()) if self._pending_nodes else 0

        if remaining > 0:
            self.root.after(50, self._process_ping_queue)
            return

        # All tasks done — finalize cycle
        total_ping_time = time.time() - getattr(self, '_last_cycle_start', time.time())
        self._print(f"\nAll concurrent pings processed (cycle time: {total_ping_time:.3f}s)")

        if self._cycle_changed:
            self._print("  Changes detected - saving data.")
            try:
                self.save_data()
            except Exception as e:
                self._print(f"  Save data failed: {e}")
        else:
            self._print("  No changes - skipping save.")

        self._cycle_in_progress = False
        if not self._shutdown_event.is_set():
            try:
                self.root.after(self._monitor_interval_ms, self.monitor_all)
            except Exception:
                self.root.after(1000, self.monitor_all)
        else:
            self._print("Shutdown requested; not scheduling further monitoring cycles.")

    def _finalize_node(self, node_name: str):
        """
        FIX: Extracted finalization logic into its own method.
        Called exactly once per node per cycle (when pending count hits zero).
        All state mutations happen under the per-node lock.
        """
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
                if not node.alive:
                    # Node recovered (finalize path — immediate path may have already set this)
                    if node.last_down_time:
                        downtime = time.time() - node.last_down_time
                        node.total_down_time += downtime
                        node.last_down_time = None
                    old_alive = node.alive
                    node.previous_alive = old_alive
                    node.color = "#01fd6a"
                    node.alive = True
                    node.last_active = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    node.last_status_change = time.time()
                    self._cycle_changed = True
                    self._print(f"  Node {node_name} recovered (finalize: at least 1 IP reachable).")
                    self.update_node_status_in_db(node_name)
                # FIX: Even if already alive, ensure color is correct (covers partial recovery)
                elif node.color != "#01fd6a":
                    node.color = "#01fd6a"
                    self.update_node_status_in_db(node_name)
            else:
                node.consec_fail += 1
                self._print(f"  Node {node_name}: consec_fail={node.consec_fail}/{self.CONSECUTIVE_FAILURES_THRESHOLD}")
                if node.consec_fail >= self.CONSECUTIVE_FAILURES_THRESHOLD:
                    if node.alive:
                        old_alive = node.alive
                        node.previous_alive = old_alive
                        node.alive = False
                        node.color = "#f22009"
                        node.last_down_time = time.time()
                        node.last_status_change = time.time()
                        self._cycle_changed = True
                        self._print(f"  Node {node_name} marked DOWN after {node.consec_fail} consecutive failures.")
                        self.update_node_status_in_db(node_name)
                    else:
                        # FIX: Node was already down — still update color/timestamp if needed
                        if node.color != "#f22009":
                            node.color = "#f22009"
                            self.update_node_status_in_db(node_name)
                else:
                    self._print(f"  Node {node_name}: not marking down yet ({node.consec_fail}/{self.CONSECUTIVE_FAILURES_THRESHOLD})")

        # Log DB event if state changed
        if self._cycle_changed:
            try:
                self.log_node_event(
                    node.name, node.starttime, node.last_down_time,
                    node.total_down_time, node.alive, node.disabled
                )
            except Exception:
                pass

    # ---- Task row cache / filtering helpers ----
    def _update_task_row(self, row_id: str, node_name: str, ip: str, status: str, rtt_ms: float, consec_fail: int) -> str:
        if rtt_ms > 0:
            rtt_display = f"{rtt_ms:.2f}"
        else:
            rtt_display = "0.00"
        
        self._task_rows[row_id] = (row_id, node_name, ip, status, rtt_display, consec_fail)
        q = (self._filter_query or "").strip().lower()
        matches = False
        if not q:
            matches = True
        else:
            if (node_name and q in node_name.lower()) or (ip and q in ip.lower()):
                matches = True
        try:
            if matches:
                if self.tree.exists(row_id):
                    self.tree.item(row_id, values=(row_id, node_name, ip, status, rtt_display, consec_fail))
                    return row_id
                else:
                    try:
                        self.tree.insert("", "end", iid=row_id, values=(row_id, node_name, ip, status, rtt_display, consec_fail))
                        return row_id
                    except tk.TclError:
                        alt_id = f"{row_id}_{int(time.time()*1000)}"
                        try:
                            self.tree.insert("", "end", iid=alt_id, values=(alt_id, node_name, ip, status, rtt_display, consec_fail))
                            self._task_rows[alt_id] = (alt_id, node_name, ip, status, rtt_display, consec_fail)
                            return alt_id
                        except Exception:
                            return row_id
            else:
                try:
                    if self.tree.exists(row_id):
                        self.tree.delete(row_id)
                except Exception:
                    pass
                return row_id
        except Exception:
            return row_id

    def _apply_filter(self):
        q = (self._filter_query or "").strip().lower()
        try:
            for it in self.tree.get_children():
                self.tree.delete(it)
        except Exception:
            pass
        count = 0
        for _rid, (rid, node_name, ip, status, rtt_display, consec) in self._task_rows.items():
            if not q or (node_name and q in node_name.lower()) or (ip and q in ip.lower()):
                try:
                    self.tree.insert("", "end", iid=rid, values=(rid, node_name, ip, status, rtt_display, consec))
                    count += 1
                except Exception:
                    try:
                        alt_id = f"{rid}_{int(time.time()*1000)}"
                        self.tree.insert("", "end", iid=alt_id, values=(alt_id, node_name, ip, status, rtt_display, consec))
                        count += 1
                    except Exception:
                        pass
        try:
            if self._filter_query:
                self._filter_label.config(text=f"Filter: '{self._filter_query}' ({count})")
            else:
                self._filter_label.config(text="Showing all tasks")
        except Exception:
            pass

    def do_search(self, event=None):
        q = (self.search_var.get() or "").strip()
        self._filter_query = q
        self._apply_filter()

    def clear_search(self):
        self.search_var.set("")
        self._filter_query = ""
        self._apply_filter()

    def log_node_event(self, plant_name: str, starttime: float, last_down_time: Optional[float],
                       total_down_time: int, alive: bool, disabled: bool):
        event_time = datetime.now()
        starttime_dt = datetime.fromtimestamp(starttime) if starttime else None
        last_down_time_dt = datetime.fromtimestamp(last_down_time) if last_down_time else None
        uptime = int(time.time() - starttime - total_down_time) if alive and not disabled else 0
        self._print(f"  Logging DB event for {plant_name}: uptime={uptime}s, total_down={total_down_time}s, alive={alive}, disabled={disabled}")
        if self.db_cursor and self.db_conn:
            try:
                self.db_cursor.execute(
                    """
                    INSERT INTO node_events (plant_name, event_time, starttime, last_down_time, total_down_time, uptime, disabled, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (plant_name, event_time, starttime_dt, last_down_time_dt, total_down_time, uptime, disabled, None)
                )
                self.db_conn.commit()
            except Exception as e:
                self._print(f"  DB log error: {e}")

    def run(self):
        self.setup_gui()
        def _start_once():
            if not self._shutdown_event.is_set():
                try:
                    self.monitor_all()
                except Exception as e:
                    self._print(f"Failed to start monitoring: {e}")
            else:
                self.root.quit()
        self.root.after(1000, _start_once)
        self._print("Starting GUI-based monitoring loop. Using icmplib for accurate ICMP pings.")
        self._print(f"Monitoring cycle will run every {self._monitor_interval_ms/1000:.1f} seconds (scheduled after each cycle completes).")
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        try:
            self.root.mainloop()
        except Exception as e:
            self._print(f"GUI error: {str(e)}")

    def on_closing(self):
        self._print("Shutting down gracefully...")
        self._shutdown_event.set()
        # Stop DB worker and wait for queue to flush
        try:
            self._db_worker.stop()
            self._db_worker.join(timeout=5)
        except Exception as e:
            self._print(f"DB worker shutdown error: {e}")
        try:
            self.observer.stop()
            self.observer.join(timeout=2)
        except Exception as e:
            self._print(f"Observer shutdown error: {str(e)}")
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except Exception as e:
            self._print(f"Executor shutdown error: {e}")
        try:
            if self.db_cursor:
                self.db_cursor.close()
            if self.db_conn:
                self.db_conn.close()
        except Exception as e:
            self._print(f"Database close error: {str(e)}")
        try:
            self.root.destroy()
        except Exception:
            pass
        self._print("Shutdown complete.")

    def __del__(self):
        try:
            if hasattr(self, 'observer'):
                self.observer.stop()
                self.observer.join(timeout=1)
        except Exception:
            pass
        try:
            if hasattr(self, '_db_worker'):
                self._db_worker.stop()
        except Exception:
            pass
        try:
            if hasattr(self, 'db_cursor') and self.db_cursor:
                self.db_cursor.close()
            if hasattr(self, 'db_conn') and self.db_conn:
                self.db_conn.close()
        except Exception:
            pass
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except Exception:
            pass

if __name__ == "__main__":
    try:
        import icmplib
        be = Backend()
        try:
            be.run()
        except KeyboardInterrupt:
            be._print("\nMonitoring stopped by user.")
    except ImportError:
        print("ERROR: 'icmplib' package is required but not installed.")
        print("Please install it with: pip install icmplib")
        print("For better ICMP accuracy, you may also need to run with appropriate permissions:")
        print("  - On Linux: sudo setcap cap_net_raw+ep $(which python3) or run with sudo")
        print("  - On Windows: Run as Administrator")
        print("  - On macOS: Run with sudo")