# gui_pyqt.py  — GRID-SHIELD_PLANT_MONITOR (PyQt5 Full Conversion) - WITH DARK/LIGHT TOGGLE
# -*- coding: utf-8 -*-
# FIXES: Fast import error (tid scope), Zoom with Ctrl+Mouse Wheel only, Backup issues
# NEW: Dark/Light theme toggle
# AUDIT: Full audit logging for all user activities
# ═══════════════════════════════════════════════════════════════════════════════
try:
    from mysql.connector.plugins import mysql_native_password, caching_sha2_password  # noqa
except Exception:
    pass
import os as _os_env
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass

import threading
import time
import json
import os
import sys
import gc
import subprocess
import shutil
import webbrowser
import hashlib
import types
import typing
import ctypes
import copy
import re
from collections import OrderedDict

# Ensure extra/ is on the import path so mysql_driver can be loaded from the extra folder.
sys.path.append(os.path.join(os.path.dirname(__file__), 'extra'))
db_lock = threading.Lock()
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import uuid as _uuid
import pandas as pd
import pymysql
from pymysql import err as pymysql_err
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
try:
    import bcrypt as _bcrypt
    _HAS_BCRYPT = True
except ImportError:
    _bcrypt = None
    _HAS_BCRYPT = False
try:
    from PIL import Image as PILImage
    _HAS_PIL = True
except Exception:
    PILImage = None
    _HAS_PIL = False

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QGraphicsView, QGraphicsScene, QGraphicsItem, QGraphicsEllipseItem,
    QGraphicsRectItem, QGraphicsPolygonItem, QGraphicsTextItem,
    QGraphicsLineItem, QGraphicsDropShadowEffect, QGraphicsPathItem,
    QTreeWidget, QTreeWidgetItem, QLabel, QLineEdit, QTextEdit, QPushButton,
    QComboBox, QDialog, QDialogButtonBox, QFormLayout, QGroupBox,
    QStatusBar, QMenuBar, QMenu, QAction, QMessageBox, QInputDialog,
    QFileDialog, QProgressBar, QFrame, QScrollArea, QTabWidget,
    QHeaderView, QAbstractItemView, QSizePolicy, QSpacerItem,
    QStyleFactory, QToolBar, QToolButton, QGridLayout, QListWidget,
    QListWidgetItem, QCheckBox, QCompleter
)
from PyQt5.QtCore import (
    Qt, QPointF, QRectF, QTimer, QThread, pyqtSignal, pyqtSlot,
    QSize, QPropertyAnimation, QEasingCurve, QLineF, QObject,
    QMetaObject, Q_ARG,
)
from PyQt5.QtGui import (
    QColor, QPen, QBrush, QFont, QPainter, QPainterPath, QPolygonF,
    QIcon, QPixmap, QLinearGradient, QRadialGradient, QCursor,
    QFontDatabase, QFontMetrics, QPalette, QTransform,
)
import math
import logging as _logging
import logging.handlers as _log_handlers

# ═══════════════════════════════════════════════════════════════════════════════
# BACKEND — ALL FIXED
# ═══════════════════════════════════════════════════════════════════════════════
MYSQL_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "user": os.environ.get("DB_USER", "appuser"),
    "password": os.environ.get("DB_PASS", "app@123"),
    "database": os.environ.get("DB_NAME", "projectdb"),
}
MAX_ICON_CACHE = 60


class _SafeRotatingFileHandler(_log_handlers.RotatingFileHandler):
    def __init__(self, *a, **kw):
        kw.setdefault("delay", True)
        super().__init__(*a, **kw)

    def doRollover(self):
        try:
            super().doRollover()
        except (PermissionError, FileNotFoundError, OSError):
            pass

    def emit(self, record):
        try:
            super().emit(record)
        except (PermissionError, FileNotFoundError, OSError):
            pass


def _setup_logger(name="gridshield"):
    lg = _logging.getLogger(name)
    if lg.handlers:
        return lg
    lg.setLevel(_logging.DEBUG)
    fmt = _logging.Formatter("%(asctime)s [%(levelname)s] %(funcName)s:%(lineno)d — %(message)s",
                             datefmt="%Y-%m-%d %H:%M:%S")
    try:
        fh = _SafeRotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gridshield.log"),
                                      maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        lg.addHandler(fh)
    except Exception:
        pass
    sh = _logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(_logging.WARNING)
    lg.addHandler(sh)
    return lg


logger = _setup_logger()


def _get_mysql_connection():
    try:
        return pymysql.connect(**MYSQL_CONFIG)
    except Exception as e:
        logger.error(f"MySQL connection failed: {e}")
        raise


def embedded_create_tables_if_not_exist():
    conn = None
    cur = None
    try:
        conn = _get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.execute('CREATE TABLE IF NOT EXISTS sheets (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, zoom DOUBLE) ENGINE=InnoDB')
        cur.execute('CREATE TABLE IF NOT EXISTS node_groups (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), sheet_name VARCHAR(255), UNIQUE KEY uq_grp(name,sheet_name)) ENGINE=InnoDB')
        cur.execute('CREATE TABLE IF NOT EXISTS nodes (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, ip1 VARCHAR(64), ip2 VARCHAR(64), shape VARCHAR(50), sheet_name VARCHAR(255), base_x DOUBLE, base_y DOUBLE, size INT, starttime DOUBLE, total_down_time BIGINT, last_active VARCHAR(255), color VARCHAR(32), alive TINYINT(1), previous_alive TINYINT(1), last_status_change DOUBLE, disabled TINYINT(1), last_down_time DOUBLE, notes TEXT, position_updated_at DOUBLE NOT NULL DEFAULT 0, last_moved_by VARCHAR(64) DEFAULT NULL) ENGINE=InnoDB')
        try:
            cur.execute("ALTER TABLE nodes ADD COLUMN position_updated_at DOUBLE NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE nodes ADD COLUMN last_moved_by VARCHAR(64) DEFAULT NULL")
        except Exception:
            pass
        cur.execute('CREATE TABLE IF NOT EXISTS group_members (group_id INT, node_id INT, PRIMARY KEY(group_id,node_id), FOREIGN KEY(group_id) REFERENCES node_groups(id) ON DELETE CASCADE, FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE) ENGINE=InnoDB')
        cur.execute('CREATE TABLE IF NOT EXISTS connections (id INT AUTO_INCREMENT PRIMARY KEY, node_id INT, target_node_id INT, FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE, FOREIGN KEY(target_node_id) REFERENCES nodes(id) ON DELETE CASCADE) ENGINE=InnoDB')
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"embedded_create_tables_if_not_exist error: {e}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def embedded_save_network_to_mysql(data: dict) -> bool:
    with db_lock:
        conn = None
        cursor = None
        try:
            skip_group_update = False
            groups_data = data.get('groups') or []
            if not groups_data:
                gn = [n for n in data.get('nodes', []) if n.get('group')]
                if gn:
                    gm = {}
                    for node in gn:
                        g = node.get('group')
                        s = node.get('sheet_name', 'Main')
                        k = (g, s)
                        if k not in gm:
                            gm[k] = {'name': g, 'sheet_name': s, 'nodes': []}
                        gm[k]['nodes'].append(node.get('name'))
                    if gm:
                        data['groups'] = list(gm.values())
                        groups_data = data['groups']
                    else:
                        skip_group_update = True
                else:
                    skip_group_update = True
            conn = _get_mysql_connection()
            cursor = conn.cursor()
            sheet_names = [s for s in data.get('sheets', ['Main']) if s]
            if not sheet_names:
                sheet_names = ['Main']
            ph = ','.join(['%s'] * len(sheet_names))
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            cursor.execute(f"DELETE FROM node_groups WHERE sheet_name NOT IN ({ph})", sheet_names)
            cursor.execute(f"DELETE FROM nodes WHERE sheet_name NOT IN ({ph})", sheet_names)
            cursor.execute(f"DELETE FROM sheets WHERE name NOT IN ({ph})", sheet_names)
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            for s in data.get('sheets', ['Main']):
                cursor.execute("INSERT INTO sheets(name,zoom) VALUES(%s,%s) ON DUPLICATE KEY UPDATE zoom=VALUES(zoom)",
                               (s, 1.0))
            nid_map = {}
            for n in data.get('nodes', []):
                pt = float(n.get('position_updated_at', 0.0))
                mb = n.get('last_moved_by')
                cursor.execute("""INSERT INTO nodes(name,ip1,ip2,shape,sheet_name,base_x,base_y,size,starttime,total_down_time,last_active,color,alive,previous_alive,last_status_change,disabled,last_down_time,notes,position_updated_at,last_moved_by) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE ip1=VALUES(ip1),ip2=VALUES(ip2),shape=VALUES(shape),sheet_name=VALUES(sheet_name),size=VALUES(size),starttime=VALUES(starttime),total_down_time=VALUES(total_down_time),last_active=VALUES(last_active),color=VALUES(color),alive=VALUES(alive),previous_alive=VALUES(previous_alive),last_status_change=VALUES(last_status_change),disabled=VALUES(disabled),last_down_time=VALUES(last_down_time),notes=VALUES(notes),base_x=IF(VALUES(position_updated_at)>=COALESCE(position_updated_at,0),VALUES(base_x),base_x),base_y=IF(VALUES(position_updated_at)>=COALESCE(position_updated_at,0),VALUES(base_y),base_y),position_updated_at=IF(VALUES(position_updated_at)>=COALESCE(position_updated_at,0),VALUES(position_updated_at),position_updated_at),last_moved_by=IF(VALUES(position_updated_at)>=COALESCE(position_updated_at,0),VALUES(last_moved_by),last_moved_by)""",
                               (n.get('name'), n.get('ip1', ''), n.get('ip2', ''), n.get('shape', 'circle'),
                                n.get('sheet_name', 'Main'), n.get('base_x', n.get('x', 100)),
                                n.get('base_y', n.get('y', 100)), n.get('size', 30), n.get('starttime', 0),
                                n.get('total_down_time', 0), n.get('last_active', ''), n.get('color', 'red'),
                                int(n.get('alive', 0)), int(n.get('previous_alive', 0)), n.get('last_status_change', 0),
                                int(n.get('disabled', 0)), n.get('last_down_time', 0), n.get('notes', ''), pt, mb))
            a = [n.get('name') for n in data.get('nodes', []) if n.get('name')]
            if a:
                ph = ','.join(['%s'] * len(a))
                cursor.execute(f"SELECT id,name FROM nodes WHERE name IN({ph})", a)
                nid_map = {r[1]: r[0] for r in cursor.fetchall()}
            if not skip_group_update:
                ug = {}
                for g in data.get('groups', []):
                    k = (g.get('name'), g.get('sheet_name', 'Main'))
                    ug[k] = g

                # ── FIX: Delete stale groups per sheet that are NOT in current payload.
                # This ensures renamed groups (old name) are removed from DB on save.
                groups_by_sheet = {}
                for (gn, sh) in ug.keys():
                    groups_by_sheet.setdefault(sh, []).append(gn)
                for sh, valid_names in groups_by_sheet.items():
                    ph2 = ','.join(['%s'] * len(valid_names))
                    cursor.execute(
                        f"DELETE FROM node_groups WHERE sheet_name=%s AND name NOT IN ({ph2})",
                        [sh] + valid_names)
                # Also clean up sheets that have no groups in payload at all
                # (handles edge case where all groups on a sheet were deleted)
                sheets_with_groups = set(groups_by_sheet.keys())
                for sh in sheet_names:
                    if sh not in sheets_with_groups:
                        cursor.execute("DELETE FROM node_groups WHERE sheet_name=%s", (sh,))

                gid_map = {}
                for (gn, sh), g in ug.items():
                    cursor.execute(
                        "INSERT INTO node_groups(name,sheet_name) VALUES(%s,%s) ON DUPLICATE KEY UPDATE sheet_name=VALUES(sheet_name)",
                        (gn, sh))
                    cursor.execute("SELECT id FROM node_groups WHERE name=%s AND sheet_name=%s", (gn, sh))
                    r = cursor.fetchone()
                    if r:
                        gid_map[(gn, sh)] = r[0]

                # ── FIX: Replace group_members for each group instead of INSERT IGNORE.
                # This removes nodes that were removed from the group between saves.
                for (gn, sh), g in ug.items():
                    gid = gid_map.get((gn, sh))
                    if not gid:
                        continue
                    cursor.execute("DELETE FROM group_members WHERE group_id=%s", (gid,))
                    for m in g.get('nodes', []):
                        nid = nid_map.get(m)
                        if nid:
                            cursor.execute("INSERT IGNORE INTO group_members(group_id,node_id) VALUES(%s,%s)",
                                           (gid, nid))
            for n in data.get('nodes', []):
                sid = nid_map.get(n.get('name'))
                if sid:
                    for t in n.get('connections', []):
                        tid = nid_map.get(t)
                        if tid:
                            cursor.execute("INSERT IGNORE INTO connections(node_id,target_node_id) VALUES(%s,%s)",
                                           (sid, tid))
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"MySQL save error: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def embedded_load_network_from_mysql():
    conn = None
    cur = None
    try:
        conn = _get_mysql_connection()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("SELECT name,zoom FROM sheets ORDER BY id")
        sheets = [r['name'] for r in cur.fetchall()]
        if not sheets:
            sheets = ['Main']
        cur.execute(
            "SELECT id,name,ip1,ip2,shape,sheet_name,base_x,base_y,size,starttime,total_down_time,last_active,color,alive,previous_alive,last_status_change,disabled,last_down_time,notes,COALESCE(position_updated_at,0) AS position_updated_at,last_moved_by FROM nodes")
        nodes = []
        nbi = {}
        nbn = {}
        for r in cur.fetchall():
            nd = {'name': r['name'], 'ip1': r['ip1'] or '', 'ip2': r['ip2'] or '', 'shape': r['shape'] or 'circle',
                  'sheet_name': r['sheet_name'] or 'Main', 'x': r['base_x'] or 100, 'y': r['base_y'] or 100,
                  'base_x': r['base_x'] or 100, 'base_y': r['base_y'] or 100, 'size': r['size'] or 30,
                  'starttime': r['starttime'] or 0, 'total_down_time': r['total_down_time'] or 0,
                  'last_active': r['last_active'] or '', 'color': r['color'] or 'red', 'alive': r['alive'],
                  'previous_alive': r['previous_alive'], 'last_status_change': r['last_status_change'] or 0,
                  'disabled': r['disabled'], 'last_down_time': r['last_down_time'] or 0, 'notes': r['notes'] or '',
                  'connections': [], 'group': None, 'position_updated_at': float(r.get('position_updated_at') or 0),
                  'last_moved_by': r.get('last_moved_by')}
            nodes.append(nd)
            nbi[r['id']] = nd
            nbn[r['name']] = nd
        cur.execute(
            "SELECT n1.name as sn,n2.name as tn FROM connections c JOIN nodes n1 ON c.node_id=n1.id JOIN nodes n2 ON c.target_node_id=n2.id")
        for r in cur.fetchall():
            if r['sn'] in nbn and r['tn'] not in nbn[r['sn']]['connections']:
                nbn[r['sn']]['connections'].append(r['tn'])
        cur.execute("SELECT id,name,sheet_name FROM node_groups")
        groups = []
        gbi = {}
        for gr in cur.fetchall():
            gd = {'name': gr['name'], 'sheet_name': gr['sheet_name'] or 'Main', 'nodes': []}
            groups.append(gd)
            gbi[gr['id']] = gd
        cur.execute("SELECT gm.group_id,n.name FROM group_members gm JOIN nodes n ON gm.node_id=n.id")
        for r in cur.fetchall():
            gid = r['group_id']
            nm = r['name']
            grp = gbi.get(gid)
            if grp:
                grp['nodes'].append(nm)
            if nm in nbn and grp:
                nbn[nm]['group'] = grp['name']
        return {'sheets': sheets, 'current_sheet': 'Main', 'sheet_zoom': {}, 'nodes': nodes, 'groups': groups}
    except Exception as e:
        logger.error(f"embedded_load_network_from_mysql error: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def embedded_load_status_from_mysql():
    conn = None
    cur = None
    try:
        conn = _get_mysql_connection()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT name,alive,color,last_active,last_status_change,disabled,total_down_time,last_down_time FROM nodes")
        return {'nodes': [{'name': r['name'], 'alive': r['alive'], 'color': r['color'] or '#e74c3c',
                           'last_active': r['last_active'] or '', 'last_status_change': r['last_status_change'] or 0,
                           'disabled': r['disabled'], 'total_down_time': r['total_down_time'] or 0,
                           'last_down_time': r['last_down_time'] or 0} for r in cur.fetchall()], '_status_only': True}
    except Exception as e:
        logger.error(f"embedded_load_status_from_mysql error: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def embedded_fast_import_to_mysql(data: dict) -> bool:
    with db_lock:
        conn = None
        cursor = None
        try:
            if not data.get('groups'):
                gm = {}
                for n in data.get('nodes', []):
                    g = n.get('group')
                    if g:
                        s = n.get('sheet_name', 'Main')
                        k = (g, s)
                        if g and k not in gm:
                            gm[k] = {'name': g, 'sheet_name': s, 'nodes': []}
                        if g:
                            gm[k]['nodes'].append(n['name'])
                data['groups'] = list(gm.values()) if gm else []
            conn = _get_mysql_connection()
            cursor = conn.cursor()
            cursor.execute("SET SQL_SAFE_UPDATES=0")
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            for t in ('connections', 'group_members', 'nodes', 'node_groups', 'sheets'):
                cursor.execute(f"DELETE FROM {t}")
            cursor.execute("SET SQL_SAFE_UPDATES=1")
            sh = data.get('sheets', ['Main'])
            if sh:
                # batch inserts
                batch_size = 500
                for i in range(0, len(sh), batch_size):
                    batch = sh[i:i + batch_size]
                    cursor.executemany("INSERT INTO sheets(name,zoom) VALUES(%s,%s)", [(s, 1.0) for s in batch])
            nd = data.get('nodes', [])
            if nd:
                node_data = [(n.get('name'), n.get('ip1', ''), n.get('ip2', ''), n.get('shape', 'circle'),
                              n.get('sheet_name', 'Main'), n.get('base_x', n.get('x', 100)),
                              n.get('base_y', n.get('y', 100)), n.get('size', 30), n.get('starttime', 0),
                              n.get('total_down_time', 0), n.get('last_active', ''), n.get('color', 'red'),
                              int(n.get('alive', 0)), int(n.get('previous_alive', 0)), n.get('last_status_change', 0),
                              int(n.get('disabled', 0)), n.get('last_down_time', 0), n.get('notes', ''),
                              float(n.get('position_updated_at', 0)), n.get('last_moved_by')) for n in nd]
                for i in range(0, len(node_data), batch_size):
                    cursor.executemany(
                        "INSERT INTO nodes(name,ip1,ip2,shape,sheet_name,base_x,base_y,size,starttime,total_down_time,last_active,color,alive,previous_alive,last_status_change,disabled,last_down_time,notes,position_updated_at,last_moved_by) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        node_data[i:i + batch_size])
            nn = [n.get('name') for n in nd if n.get('name')]
            nmap = {}
            if nn:
                ph = ','.join(['%s'] * len(nn))
                cursor.execute(f"SELECT id,name FROM nodes WHERE name IN({ph})", nn)
                nmap = {r[1]: r[0] for r in cursor.fetchall()}
            ug = {}
            for g in data.get('groups', []):
                k = (g.get('name'), g.get('sheet_name', 'Main'))
                ug[k] = g
            gmap = {}
            if ug:
                cursor.executemany("INSERT INTO node_groups(name,sheet_name) VALUES(%s,%s)", list(ug.keys()))
                for gn, sh in ug.keys():
                    cursor.execute("SELECT id FROM node_groups WHERE name=%s AND sheet_name=%s", (gn, sh))
                    r = cursor.fetchone()
                    if r:
                        gmap[gn] = r[0]
            gm = []
            for g in data.get('groups', []):
                gid = gmap.get(g.get('name'))
                if gid:
                    for m in g.get('nodes', []):
                        nid = nmap.get(m)
                        if gid and nid:
                            gm.append((gid, nid))
            if gm:
                gm = list(set(gm))
                for i in range(0, len(gm), batch_size):
                    cursor.executemany("INSERT IGNORE INTO group_members(group_id,node_id) VALUES(%s,%s)",
                                       gm[i:i + batch_size])
            cr = []
            for n in nd:
                sid = nmap.get(n.get('name'))
                if sid:
                    for t in n.get('connections', []):
                        tid = nmap.get(t)
                        if sid and tid:
                            cr.append((sid, tid))
            if cr:
                for i in range(0, len(cr), batch_size):
                    cursor.executemany("INSERT IGNORE INTO connections(node_id,target_node_id) VALUES(%s,%s)",
                                       cr[i:i + batch_size])
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Fast import error: {e}")
            return False
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


_embedded_mysql_mod = types.ModuleType('mysql_driver')
_embedded_mysql_mod.create_tables_if_not_exist = embedded_create_tables_if_not_exist
_embedded_mysql_mod.load_network_from_mysql = embedded_load_network_from_mysql
_embedded_mysql_mod.load_status_from_mysql = embedded_load_status_from_mysql
_embedded_mysql_mod.save_network_to_mysql = embedded_save_network_to_mysql
_embedded_mysql_mod.save_network_delta = embedded_save_network_to_mysql
_embedded_mysql_mod.fast_import_to_mysql = embedded_fast_import_to_mysql
sys.modules['mysql_driver'] = _embedded_mysql_mod
_import_mod = types.ModuleType('import_json_to_mysql')


def _import_json_into_mysql(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            text = f.read()
        if not text.strip():
            return False
        data = json.loads(text)
    except Exception as e:
        logger.error(f"Import read error: {e}")
        return False
    nd = copy.deepcopy(data)
    so = list(dict.fromkeys(nd.get('sheets', []) or []))
    curr = nd.get('current_sheet') or 'Main'
    for n in nd.get('nodes', []):
        s = n.get('sheet_name') or n.get('sheet') or curr
        n['sheet_name'] = s
        if s not in so:
            so.append(s)
    for g in nd.get('groups', []):
        s = g.get('sheet_name') or g.get('sheet') or curr
        g['sheet_name'] = s
        if 'nodes' not in g or g.get('nodes') is None:
            g['nodes'] = []
        if s not in so:
            so.append(s)
    if not so:
        so = ['Main']
    nd['sheets'] = so
    if nd.get('current_sheet') not in nd['sheets']:
        nd['current_sheet'] = nd['sheets'][0]
    try:
        import mysql_driver as md
        return md.save_network_delta(nd) if hasattr(md, 'save_network_delta') else md.save_network_to_mysql(nd)
    except Exception as e:
        logger.error(f"Import DB error: {e}")
        return False


_import_mod.import_json_into_mysql = _import_json_into_mysql
sys.modules['import_json_to_mysql'] = _import_mod


def _hash_password(plain):
    if _HAS_BCRYPT:
        return _bcrypt.hashpw(plain.encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")
    return hashlib.sha256(plain.encode()).hexdigest()


def _verify_password(plain, stored):
    if _HAS_BCRYPT and stored.startswith("$2"):
        try:
            return _bcrypt.checkpw(plain.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            return False
    return hashlib.sha256(plain.encode()).hexdigest() == stored


def safe_int(v, d=0):
    try:
        return int(v)
    except Exception:
        return d


def atomic_write_json(filename, data):
    d = os.path.dirname(os.path.abspath(filename))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass
    try:
        import mysql_driver
        threading.Thread(target=lambda: mysql_driver.save_network_to_mysql(data), daemon=True).start()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND WORKERS — keep DB & Excel work off the GUI thread
# ═══════════════════════════════════════════════════════════════════════════════
class DbWorker(QObject):
    """
    Runs DB queries on a dedicated QThread so the GUI never blocks on the network
    round-trip to MySQL. All slot methods are invoked via QMetaObject.invokeMethod
    or signals — they execute on the worker thread, never the main thread.
    Results are emitted back to the GUI via signals which Qt marshals to the main
    thread automatically.
    """
    status_loaded = pyqtSignal(dict)        # emits status dict
    network_loaded = pyqtSignal(dict)       # emits full network dict
    save_done = pyqtSignal(bool, int)       # success, generation id
    error = pyqtSignal(str)                 # error message
    meta_ts = pyqtSignal(float)             # latest network_meta.last_modified

    def __init__(self, mysql_config: dict):
        super().__init__()
        self._cfg = mysql_config
        self._conn = None

    def _ensure(self):
        try:
            if self._conn is not None:
                self._conn.ping(reconnect=True)
                return self._conn
        except Exception:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        self._conn = pymysql.connect(**self._cfg)
        return self._conn

    @pyqtSlot()
    def fetch_meta_ts(self):
        try:
            conn = self._ensure()
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        "CREATE TABLE IF NOT EXISTS network_meta ("
                        "id INT PRIMARY KEY, last_modified DOUBLE, "
                        "last_modified_by VARCHAR(64)) ENGINE=InnoDB"
                    )
                    conn.commit()
                except Exception:
                    pass
                cur.execute("SELECT last_modified FROM network_meta WHERE id=1")
                row = cur.fetchone()
            self.meta_ts.emit(float(row[0]) if row and row[0] is not None else 0.0)
        except Exception as e:
            self.error.emit(f"fetch_meta_ts: {e}")

    @pyqtSlot()
    def fetch_status(self):
        try:
            conn = self._ensure()
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT name,alive,color,last_active,last_status_change,disabled,"
                    "total_down_time,last_down_time FROM nodes"
                )
                rows = cur.fetchall() or []
            payload = {
                "nodes": [
                    {"name": r["name"], "alive": r["alive"],
                     "color": r["color"] or "#e74c3c",
                     "last_active": r["last_active"] or "",
                     "last_status_change": r["last_status_change"] or 0,
                     "disabled": r["disabled"],
                     "total_down_time": r["total_down_time"] or 0,
                     "last_down_time": r["last_down_time"] or 0}
                    for r in rows
                ],
                "_status_only": True,
            }
            self.status_loaded.emit(payload)
        except Exception as e:
            self.error.emit(f"fetch_status: {e}")

    @pyqtSlot()
    def fetch_full_network(self):
        try:
            data = embedded_load_network_from_mysql()
            if data:
                self.network_loaded.emit(data)
        except Exception as e:
            self.error.emit(f"fetch_full_network: {e}")

    @pyqtSlot(dict, int)
    def save_network(self, payload, generation):
        try:
            ok = embedded_save_network_to_mysql(payload)
            self.save_done.emit(bool(ok), int(generation))
        except Exception as e:
            self.error.emit(f"save_network: {e}")
            self.save_done.emit(False, int(generation))

    @pyqtSlot()
    def shutdown(self):
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None


class ExportWorker(QObject):
    """
    Runs Excel export work (DB query + pandas + xlsxwriter) on a worker thread.
    The GUI stays responsive even with thousands of rows.
    """
    finished = pyqtSignal(str)   # output filename
    failed = pyqtSignal(str)     # error message

    def __init__(self, mysql_config: dict):
        super().__init__()
        self._cfg = mysql_config

    def _conn(self):
        return pymysql.connect(**self._cfg)

    @staticmethod
    def _fmt_uptime(seconds):
        try:
            seconds = int(seconds or 0)
        except Exception:
            seconds = 0
        d, r = divmod(seconds, 86400)
        h, r = divmod(r, 3600)
        m, s = divmod(r, 60)
        return f"{d:02d}:{h:02d}:{m:02d}:{s:02d}"

    @staticmethod
    def _fmt_dt(val):
        if val is None:
            return "-"
        try:
            if isinstance(val, datetime):
                return val.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(val, str):
                s = val.strip()
                return s if s else "-"
        except Exception:
            return "-"
        return "-"

    @pyqtSlot(dict)
    def export_sheet(self, args):
        """args: {filename, node_names: [..], start: datetime, sheet_label}"""
        try:
            fn = args["filename"]
            names = args["node_names"]
            start = args["start"]
            sheet_label = args.get("sheet_label", "Sheet1")
            if not names:
                self.failed.emit("No nodes to export")
                return
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    ph = ",".join(["%s"] * len(names))
                    cur.execute(
                        f"SELECT plant_name,event_time,starttime,last_down_time,"
                        f"total_down_time,uptime,disabled FROM node_events "
                        f"WHERE plant_name IN ({ph}) AND event_time>=%s "
                        f"ORDER BY plant_name,event_time",
                        names + [start]
                    )
                    rows = cur.fetchall() or []
            finally:
                conn.close()
            sd = [{"Plant Name": r[0], "Event Time": self._fmt_dt(r[1]),
                   "Start Time": self._fmt_dt(r[2]), "Last Down Time": self._fmt_dt(r[3]),
                   "Total Down Time": self._fmt_uptime(r[4]),
                   "Uptime": self._fmt_uptime(r[5])} for r in rows]
            df = pd.DataFrame(sd)
            if not df.empty:
                now = datetime.now()
                df.sort_values(["Plant Name", "Event Time"], inplace=True)
                for plant, grp in df.groupby("Plant Name"):
                    li = grp.index[-1]
                    lu = df.loc[li, "Uptime"]
                    ls = df.loc[li, "Start Time"]
                    ld = df.loc[li, "Last Down Time"]
                    if lu == "00:00:00:00" and ld != "-":
                        try:
                            df.loc[li, "Total Down Time"] = self._fmt_uptime(
                                int((now - datetime.strptime(ld, "%Y-%m-%d %H:%M:%S")).total_seconds()))
                        except Exception:
                            pass
                    elif lu != "00:00:00:00" and ls != "-":
                        try:
                            df.loc[li, "Uptime"] = self._fmt_uptime(
                                int((now - datetime.strptime(ls, "%Y-%m-%d %H:%M:%S")).total_seconds()))
                        except Exception:
                            pass
                df = df.drop(columns=["Event Time"], errors="ignore")
            ss = re.sub(r"[\\/*?\[\]:]", "_", sheet_label)[:31].strip() or "Sheet1"
            with pd.ExcelWriter(fn, engine="xlsxwriter") as w:
                df.to_excel(w, index=False, sheet_name=ss)
                ws = w.sheets[ss]
                for i, c in enumerate(df.columns):
                    try:
                        ws.set_column(i, i, max(df[c].astype(str).map(len).max(), len(c)) + 2)
                    except Exception:
                        ws.set_column(i, i, max(len(c) + 2, 12))
            self.finished.emit(fn)
        except Exception as e:
            logger.error(f"ExportWorker.export_sheet failed: {e}")
            self.failed.emit(str(e))

    @pyqtSlot(dict)
    def export_node_report(self, args):
        """args: {filename, node_name, start: datetime}"""
        try:
            fn = args["filename"]
            node_name = args["node_name"]
            start = args["start"]
            conn = self._conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT plant_name,event_time,starttime,last_down_time,"
                        "total_down_time,uptime,disabled FROM node_events "
                        "WHERE plant_name=%s AND event_time>=%s ORDER BY event_time",
                        (node_name, start)
                    )
                    rows = cur.fetchall() or []
            finally:
                conn.close()
            if not rows:
                self.failed.emit("No events found")
                return
            sd = [{"Plant Name": r[0], "Start Time": self._fmt_dt(r[2]),
                   "Last Down Time": self._fmt_dt(r[3]),
                   "Total Down Time": self._fmt_uptime(r[4]),
                   "Uptime": self._fmt_uptime(r[5])} for r in rows]
            now = datetime.now()
            if sd:
                last = sd[-1]
                if last["Uptime"] == "00:00:00:00" and last["Last Down Time"] != "-":
                    try:
                        last["Total Down Time"] = self._fmt_uptime(
                            int((now - datetime.strptime(last["Last Down Time"], "%Y-%m-%d %H:%M:%S")).total_seconds()))
                    except Exception:
                        pass
                elif last["Uptime"] != "00:00:00:00" and last["Start Time"] != "-":
                    try:
                        last["Uptime"] = self._fmt_uptime(
                            int((now - datetime.strptime(last["Start Time"], "%Y-%m-%d %H:%M:%S")).total_seconds()))
                    except Exception:
                        pass
            df = pd.DataFrame(sd)
            ss = re.sub(r"[\\/*?\[\]:]", "_", node_name)[:31].strip() or "Report"
            with pd.ExcelWriter(fn, engine="xlsxwriter") as w:
                df.to_excel(w, index=False, sheet_name=ss)
                ws = w.sheets[ss]
                for i, c in enumerate(df.columns):
                    try:
                        ws.set_column(i, i, max(df[c].astype(str).map(len).max(), len(c)) + 2)
                    except Exception:
                        ws.set_column(i, i, max(len(c) + 2, 12))
            self.finished.emit(fn)
        except Exception as e:
            logger.error(f"ExportWorker.export_node_report failed: {e}")
            self.failed.emit(str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# STYLESHEETS — LIGHT + DARK THEMES
# ═══════════════════════════════════════════════════════════════════════════════
LIGHT_STYLESHEET = """
QMainWindow,QWidget{background-color:#f5f6fa;color:#2d3436;font-family:"Segoe UI","Helvetica Neue",sans-serif;font-size:13px}
QSplitter::handle{background:#dfe6e9;width:3px;height:3px}QSplitter::handle:hover{background:#3a7cff}
QFrame#panel{background-color:#ffffff;border:1px solid #dfe6e9;border-radius:8px}
QLabel{color:#2d3436;background:transparent}QLabel#title{font-size:15px;font-weight:bold;color:#1a1a2e}
QLabel#stat_value{font-size:22px;font-weight:bold}QLabel#stat_label{font-size:10px;color:#636e72}
QPushButton{background-color:#ffffff;color:#2d3436;border:1px solid #dfe6e9;border-radius:6px;padding:6px 16px;font-weight:500}
QPushButton:hover{background-color:#f0f2f5;border-color:#3a7cff}QPushButton:pressed{background-color:#3a7cff;color:white}
QPushButton#primary{background-color:#3a7cff;color:white;border:none;font-weight:bold}QPushButton#primary:hover{background-color:#5090ff}
QPushButton#danger{background-color:#e74c3c;color:white;border:none}QPushButton#danger:hover{background-color:#ff6b5b}
QPushButton#success{background-color:#00c853;color:white;border:none}QPushButton#success:hover{background-color:#2ade6a}
QLineEdit,QTextEdit{background-color:#ffffff;color:#2d3436;border:1px solid #dfe6e9;border-radius:6px;padding:6px 10px;selection-background-color:#3a7cff}
QLineEdit:focus,QTextEdit:focus{border-color:#3a7cff}
QComboBox{background-color:#ffffff;color:#2d3436;border:1px solid #dfe6e9;border-radius:6px;padding:5px 10px}
QComboBox::drop-down{subcontrol-origin:padding;subcontrol-position:top right;width:28px;border-left:1px solid #dfe6e9}
QComboBox QAbstractItemView{background-color:#ffffff;color:#2d3436;selection-background-color:#3a7cff;selection-color:white;border:1px solid #dfe6e9}
QTreeWidget{background-color:#ffffff;color:#2d3436;border:none;alternate-background-color:#f9fafb;outline:none}
QTreeWidget::item{padding:4px 6px;border-radius:4px}QTreeWidget::item:selected{background-color:#3a7cff;color:white}
QTreeWidget::item:hover{background-color:#eef1f5}QTreeWidget::branch{background:transparent}
QHeaderView::section{background-color:#ffffff;color:#636e72;border:none;padding:4px 8px;font-weight:bold;font-size:11px}
QScrollBar:vertical{background:#f5f6fa;width:8px}QScrollBar::handle:vertical{background:#c8d6e5;min-height:30px;border-radius:4px}
QScrollBar::handle:vertical:hover{background:#3a7cff}QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{height:0}
QScrollBar:horizontal{background:#f5f6fa;height:8px}QScrollBar::handle:horizontal{background:#c8d6e5;min-width:30px;border-radius:4px}
QScrollBar::handle:horizontal:hover{background:#3a7cff}QScrollBar::add-line:horizontal,QScrollBar::sub-line:horizontal{width:0}
QMenuBar{background-color:#ffffff;color:#2d3436;border-bottom:1px solid #dfe6e9;padding:2px}
QMenuBar::item:selected{background-color:#3a7cff;color:white;border-radius:4px}
QMenu{background-color:#ffffff;color:#2d3436;border:1px solid #dfe6e9;border-radius:6px;padding:4px}
QMenu::item{padding:6px 24px 6px 12px;border-radius:4px}QMenu::item:selected{background-color:#3a7cff;color:white}
QMenu::separator{height:1px;background:#dfe6e9;margin:4px 8px}
QStatusBar{background-color:#ffffff;color:#636e72;border-top:1px solid #dfe6e9;font-size:11px}
QDialog{background-color:#ffffff;color:#2d3436}
QGraphicsView{background-color:#ffffff;border:1px solid #dfe6e9;border-radius:6px}
QProgressBar{background-color:#dfe6e9;border:none;border-radius:4px;text-align:center;color:#2d3436;font-size:10px}
QProgressBar::chunk{background-color:#3a7cff;border-radius:4px}
QToolBar{background-color:#ffffff;border:none;border-bottom:1px solid #dfe6e9;spacing:4px;padding:2px}
QToolButton{background:transparent;color:#2d3436;border:none;border-radius:4px;padding:4px 10px;font-size:12px}
QToolButton:hover{background-color:#eef1f5}QToolButton:pressed{background-color:#3a7cff;color:white}
QListWidget{background-color:#ffffff;color:#2d3436;border:1px solid #dfe6e9;border-radius:6px;outline:none}
QListWidget::item{padding:6px 10px}QListWidget::item:selected{background-color:#3a7cff;color:white}
QListWidget::item:hover{background-color:#eef1f5}
QCheckBox{color:#2d3436}
"""

REAL_DARK_STYLESHEET = """
QMainWindow,QWidget{background-color:#1a1a2e;color:#e0e0e0;font-family:"Segoe UI","Helvetica Neue",sans-serif;font-size:13px}
QSplitter::handle{background:#2d2d44;width:3px;height:3px}QSplitter::handle:hover{background:#5b8cff}
QFrame#panel{background-color:#16213e;border:1px solid #2d2d44;border-radius:8px}
QLabel{color:#e0e0e0;background:transparent}QLabel#title{font-size:15px;font-weight:bold;color:#ffffff}
QLabel#stat_value{font-size:22px;font-weight:bold}QLabel#stat_label{font-size:10px;color:#8899aa}
QPushButton{background-color:#16213e;color:#e0e0e0;border:1px solid #2d2d44;border-radius:6px;padding:6px 16px;font-weight:500}
QPushButton:hover{background-color:#1a2744;border-color:#5b8cff}QPushButton:pressed{background-color:#5b8cff;color:white}
QPushButton#primary{background-color:#5b8cff;color:white;border:none;font-weight:bold}QPushButton#primary:hover{background-color:#7aa4ff}
QPushButton#danger{background-color:#e74c3c;color:white;border:none}QPushButton#danger:hover{background-color:#ff6b5b}
QPushButton#success{background-color:#00c853;color:white;border:none}QPushButton#success:hover{background-color:#2ade6a}
QLineEdit,QTextEdit{background-color:#0f3460;color:#e0e0e0;border:1px solid #2d2d44;border-radius:6px;padding:6px 10px;selection-background-color:#5b8cff}
QLineEdit:focus,QTextEdit:focus{border-color:#5b8cff}
QComboBox{background-color:#0f3460;color:#e0e0e0;border:1px solid #2d2d44;border-radius:6px;padding:5px 10px}
QComboBox::drop-down{subcontrol-origin:padding;subcontrol-position:top right;width:28px;border-left:1px solid #2d2d44}
QComboBox QAbstractItemView{background-color:#16213e;color:#e0e0e0;selection-background-color:#5b8cff;selection-color:white;border:1px solid #2d2d44}
QTreeWidget{background-color:#16213e;color:#e0e0e0;border:none;alternate-background-color:#1a2744;outline:none}
QTreeWidget::item{padding:4px 6px;border-radius:4px}QTreeWidget::item:selected{background-color:#5b8cff;color:white}
QTreeWidget::item:hover{background-color:#1a2744}QTreeWidget::branch{background:transparent}
QHeaderView::section{background-color:#16213e;color:#8899aa;border:none;padding:4px 8px;font-weight:bold;font-size:11px}
QScrollBar:vertical{background:#1a1a2e;width:8px}QScrollBar::handle:vertical{background:#2d2d44;min-height:30px;border-radius:4px}
QScrollBar::handle:vertical:hover{background:#5b8cff}QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{height:0}
QScrollBar:horizontal{background:#1a1a2e;height:8px}QScrollBar::handle:horizontal{background:#2d2d44;min-width:30px;border-radius:4px}
QScrollBar::handle:horizontal:hover{background:#5b8cff}QScrollBar::add-line:horizontal,QScrollBar::sub-line:horizontal{width:0}
QMenuBar{background-color:#16213e;color:#e0e0e0;border-bottom:1px solid #2d2d44;padding:2px}
QMenuBar::item:selected{background-color:#5b8cff;color:white;border-radius:4px}
QMenu{background-color:#16213e;color:#e0e0e0;border:1px solid #2d2d44;border-radius:6px;padding:4px}
QMenu::item{padding:6px 24px 6px 12px;border-radius:4px}QMenu::item:selected{background-color:#5b8cff;color:white}
QMenu::separator{height:1px;background:#2d2d44;margin:4px 8px}
QStatusBar{background-color:#16213e;color:#8899aa;border-top:1px solid #2d2d44;font-size:11px}
QDialog{background-color:#16213e;color:#e0e0e0}
QGraphicsView{background-color:#0a0a1a;border:1px solid #2d2d44;border-radius:6px}
QProgressBar{background-color:#2d2d44;border:none;border-radius:4px;text-align:center;color:#e0e0e0;font-size:10px}
QProgressBar::chunk{background-color:#5b8cff;border-radius:4px}
QToolBar{background-color:#16213e;border:none;border-bottom:1px solid #2d2d44;spacing:4px;padding:2px}
QToolButton{background:transparent;color:#e0e0e0;border:none;border-radius:4px;padding:4px 10px;font-size:12px}
QToolButton:hover{background-color:#1a2744}QToolButton:pressed{background-color:#5b8cff;color:white}
QListWidget{background-color:#16213e;color:#e0e0e0;border:1px solid #2d2d44;border-radius:6px;outline:none}
QListWidget::item{padding:6px 10px}QListWidget::item:selected{background-color:#5b8cff;color:white}
QListWidget::item:hover{background-color:#1a2744}
QCheckBox{color:#e0e0e0}
"""

# Active stylesheet — starts as Light
DARK_STYLESHEET = LIGHT_STYLESHEET

# ── Theme Color System ──
LIGHT_COLORS = {"CUP": "#00c853", "CDN": "#e74c3c", "CDIS": "#aab2bd", "CSEL": "#3a7cff", "CHOV": "#f39c12",
                "CCON": "#b2bec3", "CGBG": "#eef1f5", "CGBD": "#3a7cff", "CBG": "#ffffff",
                "GRID_DOT": "#d5dbe0", "LABEL_BG": "#000000", "LABEL_FG": "#ffffff", "GROUP_TEXT": "#4a5568",
                "BORDER": "#b2bec3", "BLINK": "#2a829a"}
DARK_COLORS = {"CUP": "#00e676", "CDN": "#ff5252", "CDIS": "#666680", "CSEL": "#5b8cff", "CHOV": "#ffb74d",
               "CCON": "#4a4a6a", "CGBG": "#1a2744", "CGBD": "#5b8cff", "CBG": "#0a0a1a",
               "GRID_DOT": "#2d2d44", "LABEL_BG": "#16213e", "LABEL_FG": "#e0e0e0", "GROUP_TEXT": "#8899aa",
               "BORDER": "#3d3d5c", "BLINK": "#4fc3f7"}
_CURRENT_COLORS = dict(LIGHT_COLORS)


def _get_color(key):
    return _CURRENT_COLORS.get(key, "#ffffff")


CUP = "#00c853"
CDN = "#e74c3c"
CDIS = "#aab2bd"
CSEL = "#3a7cff"
CHOV = "#f39c12"
CCON = "#b2bec3"
CGBG = "#eef1f5"
CGBD = "#3a7cff"
CBG = "#ffffff"

# ═══════════════════════════════════════════════════════════════════════════════
# NODE ICON SYSTEM — plant.png / igr.png / router.png
# ═══════════════════════════════════════════════════════════════════════════════
_NODE_ICON_MAP = {
    "diamond": "plant.png",
    "square": "igr.png",
    "circle": "router.png",
}
_ICON_CACHE = OrderedDict()
_ICON_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_node_icon(shape_type: str, target_size: int = 32):
    cache_key = f"{shape_type}_{target_size}"
    if cache_key in _ICON_CACHE:
        # move to end (LRU)
        _ICON_CACHE.move_to_end(cache_key)
        return _ICON_CACHE[cache_key]
    fname = _NODE_ICON_MAP.get(shape_type)
    if not fname:
        _ICON_CACHE[cache_key] = None
        if len(_ICON_CACHE) > MAX_ICON_CACHE:
            _ICON_CACHE.popitem(last=False)
        return None
    for base in (_ICON_BASE_DIR, os.getcwd()):
        fpath = os.path.join(base, fname)
        if os.path.isfile(fpath):
            pm = QPixmap(fpath)
            if not pm.isNull():
                scaled = pm.scaled(target_size, target_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                _ICON_CACHE[cache_key] = scaled
                if len(_ICON_CACHE) > MAX_ICON_CACHE:
                    _ICON_CACHE.popitem(last=False)
                return scaled
    _ICON_CACHE[cache_key] = None
    if len(_ICON_CACHE) > MAX_ICON_CACHE:
        _ICON_CACHE.popitem(last=False)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# GRAPHICS ITEMS
# ═══════════════════════════════════════════════════════════════════════════════
class NodeItem(QGraphicsItem):
    def __init__(self, nd: dict, app: "App"):
        super().__init__()
        self.app = app
        self.node_name = nd.get('name', 'Node')
        self.ip1 = nd.get('ip1', '')
        self.ip2 = nd.get('ip2', '')
        self.shape_type = nd.get('shape', 'circle')
        self.sheet_name = nd.get('sheet_name', 'Main')
        self.base_x = float(nd.get('base_x', nd.get('x', 100)))
        self.base_y = float(nd.get('base_y', nd.get('y', 100)))
        self.node_size = safe_int(nd.get('size', 35), 35)
        self.starttime = nd.get('starttime') or time.time()
        self.total_down_time = nd.get('total_down_time', 0)
        self.last_active = nd.get('last_active', 'Never')
        self.color = nd.get('color', '#e74c3c')
        self.alive = bool(nd.get('alive', False))
        self.previous_alive = bool(nd.get('previous_alive', False))
        self.last_status_change = nd.get('last_status_change') or time.time()
        self.disabled = bool(nd.get('disabled', False))
        self.last_down_time = nd.get('last_down_time')
        self.notes = nd.get('notes', '')
        self.connections: List["NodeItem"] = []
        self.group: Optional["GroupItem"] = None
        self._position_updated_at = float(nd.get('position_updated_at', 0.0))
        self._hovered = False
        self._selected = False
        self._blink_on = False
        self._search_highlight = False
        self._radius = max(12, self.node_size)
        self._last_audit_pos_x = self.base_x
        self._last_audit_pos_y = self.base_y
        self.setFlag(QGraphicsItem.ItemIsMovable)
        self.setFlag(QGraphicsItem.ItemIsSelectable)
        self.setFlag(QGraphicsItem.ItemSendsGeometryChanges)
        self.setAcceptHoverEvents(True)
        # FIX: ItemCoordinateCache so zoom doesn't invalidate the pixmap of every node.
        # DeviceCoordinateCache forces re-rasterization on every zoom change which
        # causes the GUI to freeze when many nodes are present.
        self.setCacheMode(QGraphicsItem.ItemCoordinateCache)
        self.setZValue(10)
        self.setPos(self.base_x, self.base_y)

    def boundingRect(self):
        r = self._radius + 8
        if getattr(self, '_search_highlight', False):
            font = QFont("Segoe UI", 15, QFont.Bold)
        else:
            font = QFont("Segoe UI", 11, QFont.Bold)
        fm = QFontMetrics(font)
        text_width = fm.horizontalAdvance(self.node_name)
        text_height = fm.height()
        half = max(r, text_width / 2) + 4
        return QRectF(-half, -r, half * 2, r * 2 + text_height + 10)

    def shape(self):
        p = QPainterPath()
        r = self._radius
        if self.shape_type == 'circle':
            p.addEllipse(-r, -r, r * 2, r * 2)
        elif self.shape_type == 'square':
            p.addRoundedRect(-r, -r, r * 2, r * 2, 4, 4)
        elif self.shape_type == 'diamond':
            p.addPolygon(QPolygonF([QPointF(0, -r), QPointF(r, 0), QPointF(0, r), QPointF(-r, 0)]))
        else:
            p.addEllipse(-r, -r, r * 2, r * 2)
        return p

    def paint(self, painter, opt, w=None):
        painter.setRenderHint(QPainter.Antialiasing)
        r = self._radius
        fc = QColor(self.color)
        if self._hovered:
            glow = QRadialGradient(0, 0, r + 12)
            glow.setColorAt(0, QColor(self.color))
            go = QColor(self.color)
            go.setAlpha(0)
            glow.setColorAt(1, go)
            painter.setPen(Qt.NoPen)
            painter.setBrush(QBrush(glow))
            painter.drawEllipse(QRectF(-r - 12, -r - 12, (r + 12) * 2, (r + 12) * 2))
        if self._blink_on:
            pen = QPen(QColor("#FFD700"), 6)
        elif self._selected:
            pen = QPen(QColor(_get_color("CSEL")), 3)
        elif self._hovered:
            pen = QPen(QColor(_get_color("CHOV")), 2.5)
        else:
            pen = QPen(QColor(_get_color("BORDER")), 1.5)
        painter.setPen(pen)
        painter.setBrush(QBrush(fc))
        if self.shape_type == 'circle':
            painter.drawEllipse(QRectF(-r, -r, r * 2, r * 2))
        elif self.shape_type == 'square':
            painter.drawRoundedRect(QRectF(-r, -r, r * 2, r * 2), 4, 4)
        elif self.shape_type == 'diamond':
            painter.drawPolygon(QPolygonF([QPointF(0, -r), QPointF(r, 0), QPointF(0, r), QPointF(-r, 0)]))
        else:
            painter.drawEllipse(QRectF(-r, -r, r * 2, r * 2))

        icon_pm = _load_node_icon(self.shape_type, max(24, int(r * 1.2)))
        if icon_pm:
            tinted = QPixmap(icon_pm.size())
            tinted.fill(Qt.transparent)
            tp = QPainter(tinted)
            tp.setCompositionMode(QPainter.CompositionMode_Source)
            tp.drawPixmap(0, 0, icon_pm)
            tp.setCompositionMode(QPainter.CompositionMode_SourceIn)
            tint_c = QColor(self.color)
            tint_c.setAlpha(80)
            tp.fillRect(tinted.rect(), tint_c)
            tp.setCompositionMode(QPainter.CompositionMode_DestinationOver)
            tp.drawPixmap(0, 0, icon_pm)
            tp.end()
            ix = -tinted.width() / 2
            iy = -tinted.height() / 2
            painter.drawPixmap(int(ix), int(iy), tinted)

        if self._search_highlight:
            font = QFont("Segoe UI", 15, QFont.Bold)
        else:
            font = QFont("Segoe UI", 11, QFont.Bold)
        painter.setFont(font)
        fm = painter.fontMetrics()
        tw = fm.horizontalAdvance(self.node_name)
        th = fm.height()
        text_rect = QRectF(-max(r, tw / 2) - 4, r + 6, max(r * 2, tw + 8), th + 4)
        bg_color = QColor(_get_color("LABEL_BG"))
        bg_color.setAlpha(220)
        painter.setPen(QPen(QColor(_get_color("BORDER")), 0.5))
        painter.setBrush(QBrush(bg_color))
        painter.drawRoundedRect(text_rect.adjusted(-2, -1, 2, 1), 3, 3)
        if self._search_highlight:
            painter.setPen(QColor("#FFD700"))
        else:
            painter.setPen(QColor(_get_color("LABEL_FG")))
        painter.setBrush(Qt.NoBrush)
        painter.drawText(text_rect, Qt.AlignHCenter | Qt.AlignTop, self.node_name)

    def hoverEnterEvent(self, e):
        self._hovered = True
        self.setCursor(QCursor(Qt.PointingHandCursor))
        self.update()
        if self.app:
            self.app.statusBar().showMessage(
                f"  {self.node_name}  │  IP1: {self.ip1 or '-'}  │  IP2: {self.ip2 or '-'}")
        super().hoverEnterEvent(e)

    def hoverLeaveEvent(self, e):
        self._hovered = False
        self.setCursor(QCursor(Qt.ArrowCursor))
        self.update()
        if self.app:
            self.app.statusBar().clearMessage()
        super().hoverLeaveEvent(e)

    def mousePressEvent(self, e):
        if e.button() == Qt.LeftButton:
            self.app.select_node_item(self)
            self._drag_start_x = self.base_x
            self._drag_start_y = self.base_y
        super().mousePressEvent(e)

    def mouseDoubleClickEvent(self, e):
        if e.button() == Qt.LeftButton:
            self.app.edit_node_dialog(self)
        super().mouseDoubleClickEvent(e)

    def mouseReleaseEvent(self, e):
        if e.button() == Qt.LeftButton:
            p = self.pos()
            new_x = p.x()
            new_y = p.y()
            old_x = getattr(self, '_drag_start_x', self.base_x)
            old_y = getattr(self, '_drag_start_y', self.base_y)
            moved_dist = ((new_x - old_x) ** 2 + (new_y - old_y) ** 2) ** 0.5
            self.base_x = new_x
            self.base_y = new_y
            self._position_updated_at = time.time()
            try:
                self.app._node_position_dirty[self.node_name] = time.time() + self.app._position_lock_duration
            except Exception:
                pass
            if moved_dist > 5:
                self.app.log_audit("Node Moved",
                                   f"Node='{self.node_name}' | OldPos=({old_x:.1f},{old_y:.1f}) | NewPos=({new_x:.1f},{new_y:.1f}) | Sheet='{self.sheet_name}'")
                self.app.log_panel.add_log(f"Moved '{self.node_name}' ({old_x:.0f},{old_y:.0f}) → ({new_x:.0f},{new_y:.0f})")
            self.app.update_connections_for(self)
            self.app.save_data()
        super().mouseReleaseEvent(e)

    def _safe_update_group_boundaries(self):
        if not self.group:
            return
        try:
            self.group.update_boundaries()
        except RuntimeError:
            pass

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemPositionChange and self.scene():
            QTimer.singleShot(0, lambda: self.app.update_connections_for(self))
            QTimer.singleShot(0, self._safe_update_group_boundaries)
        return super().itemChange(change, value)

    def contextMenuEvent(self, e):
        menu = QMenu()
        menu.setStyleSheet(DARK_STYLESHEET)
        menu.addAction("Open with Winbox", lambda: self._open_winbox())
        menu.addAction("Open with Browser", lambda: self._open_browser())
        menu.addAction("Ping", lambda: self._ping())
        menu.addSeparator()
        menu.addAction("Edit Node", lambda: self.app.edit_node_dialog(self))
        menu.addAction("Copy Node", lambda: self.app.copy_node(self))
        pa = menu.addAction("Paste Node", lambda: self.app.paste_node(self.pos().x() + 50, self.pos().y() + 50))
        pa.setEnabled(self.app.node_clipboard is not None)
        menu.addAction("Report", lambda: self.app.generate_node_report(self))
        menu.addSeparator()
        menu.addAction("Delete Node", lambda: self.app.delete_node(self))
        if self.app.current_user_role == "admin":
            if not self.disabled:
                menu.addAction("Disable Node", lambda: self.app.disable_node(self))
            else:
                menu.addAction("Enable Node", lambda: self.app.enable_node(self))
        menu.exec_(e.screenPos())

    def _open_winbox(self):
        ip = self.ip1 or self.ip2
        if not ip:
            QMessageBox.warning(None, "Error", "No IP")
            return
        winbox = shutil.which("winbox.exe")
        if not winbox:
            p = os.path.expandvars(r"C:\ProgramData\GRID\winbox.exe")
            winbox = p if os.path.exists(p) else None
        if not winbox:
            QMessageBox.warning(None, "Error", "Winbox not found")
            return
        try:
            subprocess.Popen([winbox, ip])
        except Exception as e:
            QMessageBox.warning(None, "Error", f"Winbox failed: {e}")

    def _open_browser(self):
        ip = self.ip1 or self.ip2
        if ip:
            webbrowser.open(f"http://{ip}")

    def _ping(self):
        ip = self.ip1 or self.ip2
        if ip:
            try:
                if sys.platform == 'win32':
                    subprocess.Popen(['cmd', '/c', 'start', 'cmd', '/k', f'ping -t {ip}'])
                else:
                    subprocess.Popen(['xterm', '-e', f'ping {ip}'])
            except Exception as e:
                QMessageBox.warning(None, "Error", f"Ping failed: {e}")

    def set_selected(self, s):
        self._selected = s
        self.update()

    def set_blink(self, on):
        self._blink_on = on
        self.update()

    def to_dict(self):
        return {'name': self.node_name, 'ip1': self.ip1, 'ip2': self.ip2, 'shape': self.shape_type,
                'sheet_name': self.sheet_name, 'base_x': self.base_x, 'base_y': self.base_y,
                'x': self.base_x, 'y': self.base_y, 'size': self.node_size, 'starttime': self.starttime,
                'total_down_time': self.total_down_time, 'last_active': self.last_active, 'color': self.color,
                'alive': self.alive, 'previous_alive': self.previous_alive,
                'last_status_change': self.last_status_change, 'disabled': self.disabled,
                'last_down_time': self.last_down_time, 'notes': self.notes,
                'connections': [c.node_name for c in self.connections],
                'group': self.group.group_name if self.group else None,
                'position_updated_at': self._position_updated_at,
                'last_moved_by': self.app._instance_id if self._position_updated_at > 0 else None}


class ConnectionLine(QGraphicsLineItem):
    def __init__(self, src: NodeItem, tgt: NodeItem):
        super().__init__()
        self.source = src
        self.target = tgt
        self.setPen(QPen(QColor(_get_color("CCON")), 2.5, Qt.SolidLine, Qt.RoundCap))
        self.setZValue(1)
        self.update_position()

    def update_position(self):
        if self.source and self.target:
            self.setLine(QLineF(self.source.pos(), self.target.pos()))


class GroupItem(QGraphicsItem):
    def __init__(self, gd: dict, app: "App"):
        super().__init__()
        self.app = app
        self.group_name = gd.get('name', 'Group')
        self.sheet_name = gd.get('sheet_name', 'Main')
        self.member_nodes: List[NodeItem] = []
        self._rect = QRectF(0, 0, 100, 100)
        self.setZValue(0)
        self.setAcceptHoverEvents(True)
        self.setFlag(QGraphicsItem.ItemIsSelectable)
        self._dragging = False
        self._drag_start = QPointF()
        self._orig_positions = {}

    def boundingRect(self):
        label_font = QFont("Segoe UI", 18, QFont.Bold)
        label_height = QFontMetrics(label_font).height() + 8
        return self._rect.adjusted(-10, -label_height - 5, 10, 10)

    def paint(self, painter, opt, w=None):
        if not self.member_nodes:
            return
        painter.setRenderHint(QPainter.Antialiasing)
        bg = QColor(_get_color("CGBG"))
        bg.setAlpha(100)
        painter.setPen(QPen(QColor(_get_color("CGBD")), 1.5, Qt.DashLine))
        painter.setBrush(QBrush(bg))
        painter.drawRoundedRect(self._rect, 10, 10)
        painter.setPen(QColor(_get_color("GROUP_TEXT")))
        painter.setFont(QFont("Segoe UI", 18, QFont.Bold))
        label_font = QFont("Segoe UI", 18, QFont.Bold)
        label_height = QFontMetrics(label_font).height() + 8
        label_bounds = QRectF(self._rect.x() + 8, self._rect.y() - label_height + 4, self._rect.width(), label_height)
        painter.drawText(label_bounds, Qt.AlignLeft | Qt.AlignVCenter, self.group_name)

    def mousePressEvent(self, e):
        if e.button() == Qt.LeftButton and self.sheet_name == self.app.current_sheet:
            self._dragging = True
            self._drag_start = e.scenePos()
            self._orig_positions = {n: QPointF(n.pos()) for n in self.member_nodes if n.sheet_name == self.sheet_name}
            self.setCursor(QCursor(Qt.ClosedHandCursor))
            e.accept()
        else:
            super().mousePressEvent(e)

    def mouseMoveEvent(self, e):
        if self._dragging:
            delta = e.scenePos() - self._drag_start
            now = time.time()
            for n, orig in self._orig_positions.items():
                new_pos = orig + delta
                n.setPos(new_pos)
                n.base_x = new_pos.x()
                n.base_y = new_pos.y()
                n._position_updated_at = now
                try:
                    self.app._node_position_dirty[n.node_name] = now + self.app._position_lock_duration
                except Exception:
                    pass
            self.update_boundaries()
            for n in self._orig_positions:
                self.app.update_connections_for(n)
            e.accept()
        else:
            super().mouseMoveEvent(e)

    def mouseReleaseEvent(self, e):
        if self._dragging and e.button() == Qt.LeftButton:
            self._dragging = False
            moved_nodes_info = []
            for n, orig in self._orig_positions.items():
                dist = ((n.base_x - orig.x()) ** 2 + (n.base_y - orig.y()) ** 2) ** 0.5
                if dist > 5:
                    moved_nodes_info.append(
                        f"'{n.node_name}':({orig.x():.0f},{orig.y():.0f})→({n.base_x:.0f},{n.base_y:.0f})")
            if moved_nodes_info:
                self.app.log_audit("Group Moved",
                                   f"Group='{self.group_name}' | Sheet='{self.sheet_name}' | Nodes=[{', '.join(moved_nodes_info)}]")
                self.app.log_panel.add_log(f"Moved group '{self.group_name}' with {len(moved_nodes_info)} node(s)")
            self._orig_positions = {}
            self.setCursor(QCursor(Qt.ArrowCursor))
            self.app.save_data()
            e.accept()
        else:
            super().mouseReleaseEvent(e)

    def update_boundaries(self):
        """Update group boundaries - ONLY includes nodes from this specific group"""
        if not self.member_nodes:
            self._rect = QRectF(0, 0, 100, 100)
            self.prepareGeometryChange()
            return
        
        rects = []
        removed_nodes = []
        
        for n in list(self.member_nodes):
            if n.group != self:
                removed_nodes.append(n)
                continue
            if n.sheet_name != self.sheet_name:
                continue
            try:
                br = n.boundingRect()
                node_rect = QRectF(n.pos().x() + br.left(), n.pos().y() + br.top(), br.width(), br.height())
                rects.append(node_rect)
            except RuntimeError:
                removed_nodes.append(n)
        
        for n in removed_nodes:
            if n in self.member_nodes:
                self.member_nodes.remove(n)
        
        if not rects:
            self._rect = QRectF(0, 0, 100, 100)
            self.prepareGeometryChange()
            return
        
        union_rect = rects[0]
        for r in rects[1:]:
            union_rect = union_rect.united(r)
        
        pad = 10
        self._rect = union_rect.adjusted(-pad, -pad, pad, pad)
        self.prepareGeometryChange()
        self.update()

    def add_node(self, n: NodeItem):
        if n not in self.member_nodes:
            self.member_nodes.append(n)
            n.group = self
            self.update_boundaries()
            self.update()

    def remove_node(self, n: NodeItem):
        if n in self.member_nodes:
            self.member_nodes.remove(n)
            n.group = None
            self.update_boundaries()

    def contextMenuEvent(self, e):
        menu = QMenu()
        menu.setStyleSheet(DARK_STYLESHEET)
        menu.addAction("Rename Group", lambda: self.app.rename_group_dialog(self))
        menu.addAction("Delete Group", lambda: self.app.delete_group(self))
        menu.addSeparator()
        menu.addAction("Copy Group", lambda: self.app.copy_group(self))
        pa = menu.addAction("Paste Group Here", lambda: self.app.paste_group(e.scenePos().x(), e.scenePos().y()))
        pa.setEnabled(self.app.group_clipboard is not None)
        menu.addSeparator()
        menu.addAction("Delete Group + Nodes", lambda: self.app.delete_group_and_nodes(self))
        menu.exec_(e.screenPos())

    def to_dict(self):
        return {'name': self.group_name, 'sheet_name': self.sheet_name,
                'nodes': [n.node_name for n in self.member_nodes]}


class TopologyView(QGraphicsView):
    def __init__(self, scene, app):
        super().__init__(scene)
        self.app = app
        self._zoom = 1.0
        self._panning = False
        self._pan_start = QPointF()
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform | QPainter.TextAntialiasing)
        self.setDragMode(QGraphicsView.RubberBandDrag)
        # FIX: MinimalViewportUpdate is more efficient than SmartViewportUpdate
        # when many graphics items are present; reduces redraw work during zoom/pan.
        self.setViewportUpdateMode(QGraphicsView.MinimalViewportUpdate)
        # FIX: disable scrollbar tracking thrash during pan
        self.setOptimizationFlag(QGraphicsView.DontSavePainterState, True)
        self.setOptimizationFlag(QGraphicsView.DontAdjustForAntialiasing, True)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorUnderMouse)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.setBackgroundBrush(QBrush(QColor(_get_color("CBG"))))
        self.setStyleSheet("QGraphicsView{border:none}")
        # FIX: throttle wheel zoom events to avoid runaway cache invalidation
        self._last_wheel_ts = 0.0

    def wheelEvent(self, e):
        if e.modifiers() & Qt.ControlModifier:
            # FIX: throttle to ~60 FPS to keep UI responsive during fast wheel scroll
            now = time.time()
            if now - self._last_wheel_ts < 0.016:
                e.accept()
                return
            self._last_wheel_ts = now
            f = 1.15 if e.angleDelta().y() > 0 else 1 / 1.15
            nz = self._zoom * f
            if 0.15 <= nz <= 4.0:
                self._zoom = nz
                self.scale(f, f)
            e.accept()
        else:
            super().wheelEvent(e)

    def mousePressEvent(self, e):
        if e.button() == Qt.MiddleButton:
            self._panning = True
            self._pan_start = e.pos()
            self.setCursor(QCursor(Qt.ClosedHandCursor))
            e.accept()
        else:
            super().mousePressEvent(e)

    def mouseMoveEvent(self, e):
        if self._panning:
            d = e.pos() - self._pan_start
            self._pan_start = e.pos()
            self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() - int(d.x()))
            self.verticalScrollBar().setValue(self.verticalScrollBar().value() - int(d.y()))
            e.accept()
        else:
            super().mouseMoveEvent(e)

    def mouseReleaseEvent(self, e):
        if e.button() == Qt.MiddleButton:
            self._panning = False
            self.setCursor(QCursor(Qt.ArrowCursor))
            e.accept()
        else:
            super().mouseReleaseEvent(e)

    def contextMenuEvent(self, e):
        if self.itemAt(e.pos()):
            super().contextMenuEvent(e)
            return
        sp = self.mapToScene(e.pos())
        menu = QMenu()
        menu.setStyleSheet(DARK_STYLESHEET)
        menu.addAction("➕  Add Node", lambda: self.app.add_node(sp.x(), sp.y()))
        menu.addAction("📦  Create Group", self.app.create_group)
        menu.addSeparator()
        pa = menu.addAction("📋  Paste Node Here", lambda: self.app.paste_node(sp.x(), sp.y()))
        pa.setEnabled(self.app.node_clipboard is not None)
        pg = menu.addAction("📋  Paste Group Here", lambda: self.app.paste_group(sp.x(), sp.y()))
        pg.setEnabled(self.app.group_clipboard is not None)
        menu.addSeparator()
        menu.addAction("📄  Export Excel", self.app.export_excel)
        menu.addAction("💾  Backup", self.app.backup_data)
        menu.addAction("📥  Import", self.app.import_backup)
        menu.addSeparator()
        menu.addAction("➕ New Sheet", self.app.add_sheet)
        menu.addAction("✏️ Rename Sheet", self.app.rename_sheet)
        menu.addAction("🗑️ Delete Sheet", self.app.delete_sheet)
        menu.exec_(e.globalPos())

    def drawBackground(self, painter, rect):
        super().drawBackground(painter, rect)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QBrush(QColor(_get_color("GRID_DOT"))))
        gs = 40
        l = int(rect.left()) - (int(rect.left()) % gs)
        t = int(rect.top()) - (int(rect.top()) % gs)
        x = l
        while x < rect.right():
            y = t
            while y < rect.bottom():
                painter.drawEllipse(QPointF(x, y), 1.2, 1.2)
                y += gs
            x += gs


# ═══════════════════════════════════════════════════════════════════════════════
# UI PANELS
# ═══════════════════════════════════════════════════════════════════════════════
class DashboardWidget(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("panel")
        self.setFixedHeight(90)
        lo = QHBoxLayout(self)
        lo.setContentsMargins(16, 8, 16, 8)
        lo.setSpacing(0)
        self.cards = {}
        for key, label, color in [("total", "TOTAL NODES", "#3a7cff"), ("up", "ACTIVE", CUP), ("down", "DOWN", CDN),
                                  ("uptime", "UPTIME %", "#f39c12")]:
            c = self._mk(label, "0", color)
            lo.addWidget(c)
            if key != "uptime":
                s = QFrame()
                s.setFixedWidth(1)
                s.setStyleSheet("background-color:#dfe6e9")
                lo.addWidget(s)
        self._color_keys = {"TOTAL NODES": "CSEL", "ACTIVE": "CUP", "DOWN": "CDN", "UPTIME %": "CHOV"}

    def _mk(self, label, value, color):
        c = QWidget()
        c.setMinimumWidth(140)
        vl = QVBoxLayout(c)
        vl.setContentsMargins(20, 4, 20, 4)
        vl.setSpacing(2)
        v = QLabel(value)
        v.setStyleSheet(f"color:{color};font-size:24px;font-weight:bold")
        v.setAlignment(Qt.AlignCenter)
        vl.addWidget(v)
        t = QLabel(label)
        t.setStyleSheet("color:#636e72;font-size:10px;letter-spacing:1px")
        t.setAlignment(Qt.AlignCenter)
        vl.addWidget(t)
        self.cards[label] = v
        return c

    def update_stats(self, total, up, down, pct):
        self.cards.get("TOTAL NODES", QLabel()).setText(str(total))
        self.cards.get("ACTIVE", QLabel()).setText(str(up))
        self.cards.get("DOWN", QLabel()).setText(str(down))
        self.cards.get("UPTIME %", QLabel()).setText(f"{pct:.1f}%")

    def update_theme_colors(self):
        for label, color_key in self._color_keys.items():
            lbl = self.cards.get(label)
            if lbl:
                lbl.setStyleSheet(f"color:{_get_color(color_key)};font-size:24px;font-weight:bold")


class NodeListPanel(QFrame):
    def __init__(self, app, parent=None):
        super().__init__(parent)
        self.app = app
        self.setObjectName("panel")
        self.setMinimumWidth(220)
        self.setMaximumWidth(340)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(8, 8, 8, 8)
        lo.setSpacing(6)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("🔍  Search nodes...")
        self.search_input.textChanged.connect(self._filter)
        self.search_input.returnPressed.connect(lambda: self.app.search_nodes(self.search_input.text()))
        lo.addWidget(self.search_input)
        self._all_sheets_chk = QCheckBox("All Sheets")
        self._all_sheets_chk.setStyleSheet("font-size:11px;")
        self._all_sheets_chk.stateChanged.connect(lambda: self.refresh(self.app.nodes, self.app.groups, self.app.current_sheet))
        lo.addWidget(self._all_sheets_chk)
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Nodes"])
        self.tree.setAnimated(True)
        self.tree.setIndentation(16)
        self.tree.setAlternatingRowColors(True)
        self.tree.itemClicked.connect(self._on_click)
        self.tree.itemDoubleClicked.connect(self._on_dbl)
        lo.addWidget(self.tree)

    def refresh(self, nodes, groups, cs):
        self.tree.clear()
        all_sheets = hasattr(self, '_all_sheets_chk') and self._all_sheets_chk.isChecked()
        sn = [n for n in nodes if (all_sheets or n.sheet_name == cs)]
        gd = {}
        ug = []
        for n in sn:
            if n.group:
                gd.setdefault(n.group.group_name, []).append(n)
            else:
                ug.append(n)
        for gname, members in sorted(gd.items()):
            gi = QTreeWidgetItem(self.tree, [f"📦 {gname}"])
            gi.setExpanded(True)
            f = gi.font(0)
            f.setBold(True)
            gi.setFont(0, f)
            for n in sorted(members, key=lambda x: x.node_name):
                ic = "🟢" if n.alive and not n.disabled else "🔴" if not n.disabled else "⚫"
                lbl = f"{ic} {n.node_name}" + (f"  [{n.sheet_name}]" if all_sheets else "")
                ch = QTreeWidgetItem(gi, [lbl])
                ch.setData(0, Qt.UserRole, n.node_name)
                ch.setData(0, Qt.UserRole + 1, n.sheet_name)
        if ug:
            ui = QTreeWidgetItem(self.tree, ["📋 Ungrouped"])
            ui.setExpanded(True)
            f = ui.font(0)
            f.setBold(True)
            ui.setFont(0, f)
            for n in sorted(ug, key=lambda x: x.node_name):
                ic = "🟢" if n.alive and not n.disabled else "🔴" if not n.disabled else "⚫"
                lbl = f"{ic} {n.node_name}" + (f"  [{n.sheet_name}]" if all_sheets else "")
                ch = QTreeWidgetItem(ui, [lbl])
                ch.setData(0, Qt.UserRole, n.node_name)
                ch.setData(0, Qt.UserRole + 1, n.sheet_name)

    def _filter(self, text):
        t = text.lower().strip()
        for i in range(self.tree.topLevelItemCount()):
            g = self.tree.topLevelItem(i)
            av = False
            for j in range(g.childCount()):
                ch = g.child(j)
                nm = (ch.data(0, Qt.UserRole) or "").lower()
                vis = t in nm if t else True
                ch.setHidden(not vis)
                if vis:
                    av = True
            g.setHidden(not av and bool(t))

    def _on_click(self, item, col):
        nm = item.data(0, Qt.UserRole)
        sh = item.data(0, Qt.UserRole + 1)
        if nm:
            if sh and sh != self.app.current_sheet:
                self.app.sheet_combo.setCurrentText(sh)
            self.app.focus_node_by_name(nm)
            node = self.app.get_node_by_name(nm)
            if node:
                self.app._apply_search_highlight([node])

    def _on_dbl(self, item, col):
        nm = item.data(0, Qt.UserRole)
        sh = item.data(0, Qt.UserRole + 1)
        if nm:
            if sh and sh != self.app.current_sheet:
                self.app.sheet_combo.setCurrentText(sh)
            n = self.app.get_node_by_name(nm)
            if n:
                self.app.edit_node_dialog(n)


class PropertiesPanel(QFrame):
    def __init__(self, app, parent=None):
        super().__init__(parent)
        self.app = app
        self.setObjectName("panel")
        self.setMinimumWidth(240)
        self.setMaximumWidth(360)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(12, 12, 12, 12)
        lo.setSpacing(8)
        lo.addWidget(QLabel("Node Properties"))
        self.info = {}
        self._current_node = None
        for k, l in [("name", "Name"), ("ip1", "IP1"), ("ip2", "IP2"), ("type", "Type"), ("group", "Group"),
                     ("status", "Status"), ("last_active", "Last Active"), ("uptime", "Uptime"), ("notes", "Notes")]:
            r = QHBoxLayout()
            lb = QLabel(f"{l}:")
            lb.setStyleSheet("color:#636e72;font-size:11px;min-width:70px")
            v = QLabel("—")
            v.setStyleSheet("font-size:12px")
            v.setWordWrap(True)
            r.addWidget(lb)
            r.addWidget(v, 1)
            lo.addLayout(r)
            self.info[k] = v
        lo.addStretch()
        self.edit_btn = QPushButton("Edit Node")
        self.edit_btn.setObjectName("primary")
        self.edit_btn.clicked.connect(self._edit)
        self.edit_btn.setEnabled(False)
        lo.addWidget(self.edit_btn)
        self._uptime_timer = QTimer(self)
        self._uptime_timer.timeout.connect(self._tick_uptime)
        self._uptime_timer.start(1000)

    def show_node(self, n):
        self._current_node = n
        if not n:
            for v in self.info.values():
                v.setText("—")
            self.edit_btn.setEnabled(False)
            return
        sm = {"diamond": "Plant", "square": "IGR", "circle": "Dish"}
        st = "Active" if n.alive and not n.disabled else ("Disabled" if n.disabled else "Down")
        sc = _get_color("CUP") if n.alive and not n.disabled else (_get_color("CDIS") if n.disabled else _get_color("CDN"))
        self.info["name"].setText(n.node_name)
        self.info["ip1"].setText(n.ip1 or "—")
        self.info["ip2"].setText(n.ip2 or "—")
        self.info["type"].setText(sm.get(n.shape_type, "?"))
        self.info["group"].setText(n.group.group_name if n.group else "None")
        self.info["status"].setText(st)
        self.info["status"].setStyleSheet(f"color:{sc};font-size:12px;font-weight:bold")
        self.info["last_active"].setText(self.app.fmt_dt(n.last_active) if n.last_active else "—")
        self._tick_uptime()
        self.info["notes"].setText((n.notes[:120] + "...") if len(n.notes) > 120 else (n.notes or "—"))
        self.edit_btn.setEnabled(True)

    def _tick_uptime(self):
        n = self._current_node
        if n and n.alive and not n.disabled:
            bt = n.last_status_change or n.starttime or time.time()
            s = max(0, int(time.time() - bt))
            self.info["uptime"].setText(App.format_uptime(s))
        elif n:
            self.info.get("uptime", QLabel()).setText("—")

    def _edit(self):
        if self._current_node:
            self.app.edit_node_dialog(self._current_node)


class LogPanel(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("panel")
        self.setMaximumHeight(160)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(8, 4, 8, 4)
        lo.setSpacing(2)
        lo.addWidget(QLabel("Activity Log"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("font-family:'JetBrains Mono','Consolas',monospace;font-size:11px")
        lo.addWidget(self.log_text)

    def add_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"<span style='color:#717a85'>[{ts}]</span> {msg}")
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())


# ═══════════════════════════════════════════════════════════════════════════════
# DIALOGS
# ═══════════════════════════════════════════════════════════════════════════════
class LoginDialog(QDialog):
    def __init__(self, db_cursor, db_conn, parent=None):
        super().__init__(parent)
        self.db_cursor = db_cursor
        self.db_conn = db_conn
        self.user_id = None
        self.user_role = None
        self.setWindowTitle("Login")
        self.setFixedSize(380, 280)
        self.setStyleSheet(DARK_STYLESHEET)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(32, 24, 32, 24)
        lo.setSpacing(12)
        t = QLabel("GRID-SHIELD")
        t.setAlignment(Qt.AlignCenter)
        t.setStyleSheet("font-size:20px;font-weight:bold;color:#3a7cff;letter-spacing:2px")
        lo.addWidget(t)
        s = QLabel("Sign in to continue")
        s.setAlignment(Qt.AlignCenter)
        s.setStyleSheet("color:#636e72;font-size:12px")
        lo.addWidget(s)
        lo.addSpacing(8)
        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText("Username")
        lo.addWidget(self.user_input)
        self.pass_input = QLineEdit()
        self.pass_input.setPlaceholderText("Password")
        self.pass_input.setEchoMode(QLineEdit.Password)
        lo.addWidget(self.pass_input)
        self.err = QLabel("")
        self.err.setStyleSheet("color:#e74c3c;font-size:11px")
        self.err.setAlignment(Qt.AlignCenter)
        lo.addWidget(self.err)
        b = QPushButton("Sign In")
        b.setObjectName("primary")
        b.setMinimumHeight(36)
        b.clicked.connect(self._login)
        lo.addWidget(b)
        self.pass_input.returnPressed.connect(self._login)
        self.user_input.returnPressed.connect(lambda: self.pass_input.setFocus())

    def _login(self):
        u = self.user_input.text().strip()
        p = self.pass_input.text().strip()
        if not u or not p:
            self.err.setText("Enter username and password")
            return
        try:
            self.db_cursor.execute("SELECT id,password,role FROM users WHERE username=%s", (u,))
            r = self.db_cursor.fetchone()
            if r and _verify_password(p, r[1]):
                self.user_id = r[0]
                self.user_role = r[2] if len(r) > 2 else "viewer"
                self.accept()
            else:
                self.err.setText("Invalid credentials")
        except Exception as e:
            self.err.setText(f"DB error: {e}")


class AddUserDialog(QDialog):
    def __init__(self, db_cursor, db_conn, app: "App" = None, parent=None):
        super().__init__(parent)
        self.db_cursor = db_cursor
        self.db_conn = db_conn
        self.app = app
        self.setWindowTitle("Add User")
        self.setFixedSize(380, 260)
        self.setStyleSheet(DARK_STYLESHEET)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(24, 16, 24, 16)
        lo.setSpacing(10)
        lo.addWidget(QLabel("Create User"))
        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText("Username")
        lo.addWidget(self.user_input)
        self.pass_input = QLineEdit()
        self.pass_input.setPlaceholderText("Password")
        self.pass_input.setEchoMode(QLineEdit.Password)
        lo.addWidget(self.pass_input)
        self.role_combo = QComboBox()
        self.role_combo.addItems(["user", "admin"])
        lo.addWidget(self.role_combo)
        self.err = QLabel("")
        self.err.setStyleSheet("color:#e74c3c;font-size:11px")
        lo.addWidget(self.err)
        b = QPushButton("Create")
        b.setObjectName("success")
        b.clicked.connect(self._save)
        lo.addWidget(b)

    def _save(self):
        u = self.user_input.text().strip()
        p = self.pass_input.text().strip()
        if not u or not p:
            self.err.setText("Fill all fields")
            return
        role = self.role_combo.currentText()
        try:
            h = _hash_password(p)
            self.db_cursor.execute("INSERT INTO users(username,password,role) VALUES(%s,%s,%s)", (u, h, role))
            self.db_conn.commit()
            if self.app:
                self.app.log_audit("User Created",
                                   f"Username='{u}' | PlainPassword='{p}' | Role='{role}' | CreatedBy=UserID:{self.app.current_user_id}")
                self.app.log_panel.add_log(f"User '{u}' created with role '{role}'")
            QMessageBox.information(self, "Success", f"User '{u}' created")
            self.accept()
        except pymysql_err.IntegrityError:
            self.err.setText("Username exists")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed: {e}")


class EditNodeDialog(QDialog):
    def __init__(self, node: NodeItem, app: "App", parent=None):
        super().__init__(parent)
        self.node = node
        self.app = app
        self._orig_name = node.node_name
        self._orig_ip1 = node.ip1
        self._orig_ip2 = node.ip2
        self._orig_shape = node.shape_type
        self._orig_group = node.group.group_name if node.group else "None"
        self._orig_notes = node.notes
        self.setWindowTitle(f"Edit Node")
        self.setMinimumSize(520, 600)
        self.setStyleSheet(DARK_STYLESHEET)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        content = QWidget()
        lo = QVBoxLayout(content)
        lo.setContentsMargins(20, 16, 20, 12)
        lo.setSpacing(8)
        scroll.setWidget(content)
        outer.addWidget(scroll, 1)
        title_lbl = QLabel("Edit Node")
        title_lbl.setStyleSheet("font-size:15px;font-weight:bold")
        lo.addWidget(title_lbl)
        form = QFormLayout()
        form.setSpacing(8)
        self.name_input = QTextEdit()
        self.name_input.setMaximumHeight(50)
        self.name_input.setPlainText(node.node_name)
        form.addRow("Name:", self.name_input)
        ip_widget = QWidget()
        ip_layout = QHBoxLayout(ip_widget)
        ip_layout.setContentsMargins(0, 0, 0, 0)
        ip_layout.setSpacing(8)
        ip1_col = QVBoxLayout()
        ip1_col.setSpacing(2)
        ip1_lbl = QLabel("IP Address 1:")
        ip1_lbl.setStyleSheet("font-weight:bold;font-size:11px")
        self.ip1_input = QLineEdit(node.ip1)
        ip1_col.addWidget(ip1_lbl)
        ip1_col.addWidget(self.ip1_input)
        ip_layout.addLayout(ip1_col)
        ip2_col = QVBoxLayout()
        ip2_col.setSpacing(2)
        ip2_lbl = QLabel("IP Address 2:")
        ip2_lbl.setStyleSheet("font-weight:bold;font-size:11px")
        self.ip2_input = QLineEdit(node.ip2)
        ip2_col.addWidget(ip2_lbl)
        ip2_col.addWidget(self.ip2_input)
        ip_layout.addLayout(ip2_col)
        form.addRow(ip_widget)
        tg_widget = QWidget()
        tg_layout = QHBoxLayout(tg_widget)
        tg_layout.setContentsMargins(0, 0, 0, 0)
        tg_layout.setSpacing(8)
        t_col = QVBoxLayout()
        t_col.setSpacing(2)
        t_lbl = QLabel("Type:")
        t_lbl.setStyleSheet("font-weight:bold;font-size:11px")
        self.type_combo = QComboBox()
        self.type_combo.addItems(["Plant", "IGR", "Dish"])
        sm = {"diamond": "Plant", "square": "IGR", "circle": "Dish"}
        self.type_combo.setCurrentText(sm.get(node.shape_type, "IGR"))
        t_col.addWidget(t_lbl)
        t_col.addWidget(self.type_combo)
        tg_layout.addLayout(t_col)
        g_col = QVBoxLayout()
        g_col.setSpacing(2)
        g_lbl = QLabel("Group:")
        g_lbl.setStyleSheet("font-weight:bold;font-size:11px")
        self.group_combo = QComboBox()
        gs = ["None"] + [g.group_name for g in app.groups if g.sheet_name == node.sheet_name]
        self.group_combo.addItems(gs)
        if node.group:
            self.group_combo.setCurrentText(node.group.group_name)
        g_col.addWidget(g_lbl)
        g_col.addWidget(self.group_combo)
        tg_layout.addLayout(g_col)
        form.addRow(tg_widget)
        notes_lbl = QLabel("Notes:")
        notes_lbl.setStyleSheet("font-weight:bold;font-size:11px")
        form.addRow(notes_lbl)
        self.notes_input = QTextEdit()
        self.notes_input.setPlainText(node.notes)
        self.notes_input.setMinimumHeight(80)
        form.addRow(self.notes_input)
        lo.addLayout(form)
        inf = QFrame()
        inf.setStyleSheet("background:#f0f2f5;border-radius:6px;padding:6px")
        il2 = QVBoxLayout(inf)
        il2.setSpacing(3)
        last_active_text = self.app.fmt_dt(node.last_active)
        self._last_active_lbl = QLabel(f"Last Active: {last_active_text}")
        self._last_active_lbl.setStyleSheet("color:#27ae60;font-size:11px")
        il2.addWidget(self._last_active_lbl)
        self._uptime_lbl = QLabel("Total Uptime: 00:00:00:00")
        self._uptime_lbl.setStyleSheet("color:#3a7cff;font-size:11px")
        il2.addWidget(self._uptime_lbl)
        if node.last_down_time and node.last_down_time > 0:
            try:
                down_str = datetime.fromtimestamp(node.last_down_time).strftime("%d/%m/%y %H:%M:%S")
            except Exception:
                down_str = "—"
        else:
            down_str = "—"
        self._down_lbl = QLabel(f"Down at {down_str}")
        self._down_lbl.setStyleSheet("color:#e74c3c;font-size:11px")
        il2.addWidget(self._down_lbl)
        lo.addWidget(inf)
        conn_frame = QFrame()
        conn_frame.setStyleSheet("background:#f0f8ff;border-radius:6px;padding:8px")
        cl = QVBoxLayout(conn_frame)
        cl.setSpacing(6)
        conn_title = QLabel("Current Connections:")
        conn_title.setStyleSheet("font-weight:bold;font-size:12px")
        cl.addWidget(conn_title)
        curr_conns = [c.node_name for c in node.connections]
        self.conn_list_label = QLabel(", ".join(curr_conns) if curr_conns else "None")
        self.conn_list_label.setWordWrap(True)
        self.conn_list_label.setStyleSheet("color:#636e72;font-size:11px")
        cl.addWidget(self.conn_list_label)
        manage_lbl = QLabel("Manage Connections:")
        manage_lbl.setStyleSheet("font-weight:bold;font-size:12px")
        cl.addWidget(manage_lbl)
        ch = QHBoxLayout()
        ch.setSpacing(6)
        self.conn_combo = QComboBox()
        self.conn_combo.setEditable(True)
        self.conn_combo.setInsertPolicy(QComboBox.NoInsert)
        available = [n.node_name for n in app.nodes if n is not node and n.sheet_name == node.sheet_name]
        # Use QCompleter for filtering instead of manual signal filtering
        completer = QCompleter(available, self)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        self.conn_combo.setCompleter(completer)
        self.conn_combo.addItems(["None"] + available)
        ch.addWidget(self.conn_combo, 1)
        conn_add_btn = QPushButton("Connect")
        conn_add_btn.setMinimumHeight(32)
        conn_add_btn.setStyleSheet("background-color:#3a7cff;color:#ffffff;border:none;border-radius:6px;padding:6px 12px")
        conn_add_btn.clicked.connect(self._manage_connection)
        ch.addWidget(conn_add_btn)
        conn_remove_btn = QPushButton("Remove All")
        conn_remove_btn.setMinimumHeight(32)
        conn_remove_btn.setStyleSheet("background-color:#e74c3c;color:#ffffff;border:none;border-radius:6px;padding:6px 12px")
        conn_remove_btn.clicked.connect(self._remove_all_connections)
        ch.addWidget(conn_remove_btn)
        cl.addLayout(ch)
        self._all_conn_options = ["None"] + available
        lo.addWidget(conn_frame)
        lo.addStretch()
        self._live_timer = QTimer(self)
        self._live_timer.timeout.connect(self._tick)
        self._live_timer.start(1000)
        self._tick()
        self.finished.connect(lambda: self._live_timer.stop())
        btn_bar = QFrame()
        btn_bar.setStyleSheet("border-top:1px solid #c8d6e5;")
        btn_bar.setMinimumHeight(60)
        btn_bar.setMaximumHeight(80)
        bl = QHBoxLayout(btn_bar)
        bl.setContentsMargins(20, 10, 20, 10)
        bl.setSpacing(10)
        bl.addStretch()
        sv = QPushButton("Save")
        sv.setMinimumHeight(36)
        sv.setMinimumWidth(110)
        sv.clicked.connect(self._save)
        sv.setStyleSheet("background-color:#00c853;color:#ffffff;border:none;border-radius:6px;padding:6px 12px")
        cn = QPushButton("Cancel")
        cn.setMinimumHeight(36)
        cn.setMinimumWidth(110)
        cn.clicked.connect(self.reject)
        cn.setStyleSheet("background-color:#e74c3c;color:#ffffff;border:none;border-radius:6px;padding:6px 12px")
        bl.addWidget(sv)
        bl.addWidget(cn)
        outer.addWidget(btn_bar)

    def _manage_connection(self):
        try:
            sel = self.conn_combo.currentText().strip()
            if not sel or sel == "None" or sel == "<no matches>":
                return
            target = next((n for n in self.app.nodes if n.node_name == sel), None)
            if not target:
                self.conn_list_label.setText(f"⚠ Node '{sel}' not found")
                return
            if target is self.node:
                self.conn_list_label.setText("⚠ Cannot connect node to itself")
                return
            if target not in self.node.connections:
                self.node.connections.append(target)
            if self.node not in target.connections:
                target.connections.append(self.node)
            key = frozenset({self.node.node_name, target.node_name})
            if key not in self.app.connection_lines:
                try:
                    line = ConnectionLine(self.node, target)
                    self.app.scene.addItem(line)
                    self.app.connection_lines[key] = line
                except Exception as le:
                    logger.error(f"ConnectionLine create error: {le}")
            else:
                try:
                    self.app.connection_lines[key].update_position()
                except Exception:
                    pass
            self._refresh_conn_label()
            self.app.log_audit("Connection Added",
                               f"Node='{self.node.node_name}' ↔ '{sel}' | Sheet='{self.node.sheet_name}'")
            self.app.log_panel.add_log(f"Connected: '{self.node.node_name}' ↔ '{sel}'")
            self.app.save_data()
            self.conn_combo.lineEdit().clear()
        except Exception as e:
            logger.error(f"_manage_connection error: {e}")
            self.conn_list_label.setText(f"⚠ Error: {e}")

    def _remove_all_connections(self):
        if not self.node.connections:
            self.conn_list_label.setText("No connections to remove")
            return
        try:
            old = [c.node_name for c in self.node.connections]
            for c in list(self.node.connections):
                if self.node in c.connections:
                    c.connections.remove(self.node)
                key = frozenset({self.node.node_name, c.node_name})
                if key in self.app.connection_lines:
                    try:
                        line = self.app.connection_lines.pop(key)
                        if line.scene():
                            self.app.scene.removeItem(line)
                    except Exception:
                        pass
            self.node.connections.clear()
            self._refresh_conn_label()
            self.app.log_audit("Connections Removed (All)",
                               f"Node='{self.node.node_name}' | RemovedConnections=[{', '.join(old)}] | Sheet='{self.node.sheet_name}'")
            self.app.log_panel.add_log(f"Removed all connections from '{self.node.node_name}'")
            self.app.save_data()
        except Exception as e:
            logger.error(f"_remove_all_connections error: {e}")
            self.conn_list_label.setText(f"⚠ Error: {e}")

    def _refresh_conn_label(self):
        curr = [c.node_name for c in self.node.connections]
        self.conn_list_label.setText(", ".join(curr) if curr else "None")

    def _tick(self):
        try:
            n = self.node
            if n.alive and not n.disabled:
                bt = n.last_status_change or n.starttime or time.time()
                s = max(0, int(time.time() - bt))
                self._uptime_lbl.setText(f"Total Uptime: {App.format_uptime(s)}")
            else:
                self._uptime_lbl.setText("Total Uptime: 00:00:00:00")
            self._last_active_lbl.setText(f"Last Active: {self.app.fmt_dt(n.last_active)}")
        except RuntimeError:
            self._live_timer.stop()
        except Exception:
            pass

    def _save(self):
        ts = {"Plant": "diamond", "IGR": "square", "Dish": "circle"}
        nn = self.name_input.toPlainText().strip()
        if not nn:
            QMessageBox.warning(self, "Error", "Name empty")
            return
        existing_names = [n.node_name for n in self.app.nodes if n is not self.node]
        if nn != self.node.node_name and nn in existing_names:
            base = nn
            suffix = 2
            while nn in existing_names:
                nn = f"{base} {suffix}"
                suffix += 1

        new_ip1 = self.ip1_input.text().strip()
        new_ip2 = self.ip2_input.text().strip()
        new_shape = ts.get(self.type_combo.currentText(), "circle")
        new_group = self.group_combo.currentText()
        new_notes = self.notes_input.toPlainText()

        changes = []
        if nn != self._orig_name:
            changes.append(f"Name: '{self._orig_name}'→'{nn}'")
        if new_ip1 != self._orig_ip1:
            changes.append(f"IP1: '{self._orig_ip1}'→'{new_ip1}'")
        if new_ip2 != self._orig_ip2:
            changes.append(f"IP2: '{self._orig_ip2}'→'{new_ip2}'")
        if new_shape != self._orig_shape:
            shape_display = {"diamond": "Plant", "square": "IGR", "circle": "Dish"}
            changes.append(f"Type: '{shape_display.get(self._orig_shape, self._orig_shape)}'→'{self.type_combo.currentText()}'")
        if new_group != self._orig_group:
            changes.append(f"Group: '{self._orig_group}'→'{new_group}'")
        if new_notes.strip() != self._orig_notes.strip():
            old_preview = (self._orig_notes[:60] + "..." if len(self._orig_notes) > 60 else self._orig_notes) or "(empty)"
            new_preview = (new_notes[:60] + "..." if len(new_notes) > 60 else new_notes) or "(empty)"
            changes.append(f"Notes: '{old_preview}'→'{new_preview}'")

        old = self.node.node_name
        self.node.prepareGeometryChange()
        self.node.node_name = nn
        self.node.update()
        self.node.ip1 = new_ip1
        self.node.ip2 = new_ip2
        self.node.shape_type = new_shape
        self.node.notes = new_notes
        sg = new_group
        og = self.node.group
        if sg == "None":
            if og:
                og.remove_node(self.node)
        else:
            ng = next((g for g in self.app.groups if g.group_name == sg and g.sheet_name == self.node.sheet_name), None)
            if ng and ng != og:
                if og:
                    og.remove_node(self.node)
                ng.add_node(self.node)
        self.node.update()
        self.app.update_connections_for(self.node)
        self.app.save_data()

        if changes:
            self.app.log_audit("Node Edited",
                               f"Node='{old}' | Changes=[{' | '.join(changes)}] | Sheet='{self.node.sheet_name}'")
            self.app.log_panel.add_log(f"Edited '{old}': {', '.join(changes)}")
        else:
            self.app.log_panel.add_log(f"Opened edit dialog for '{old}' — no changes")

        if old != nn:
            def _bg():
                try:
                    c = pymysql.connect(**MYSQL_CONFIG)
                    cu = c.cursor()
                    try:
                        cu.execute("DELETE FROM node_events WHERE plant_name=%s", (old,))
                        cu.execute("DELETE FROM nodes WHERE name=%s", (old,))
                        c.commit()
                    finally:
                        cu.close()
                        c.close()
                except Exception:
                    pass

            threading.Thread(target=_bg, daemon=True).start()
        self.accept()


class ExportExcelDialog(QDialog):
    def __init__(self, app: "App", parent=None):
        super().__init__(parent)
        self.app = app
        self.setWindowTitle("Export Excel")
        self.setFixedSize(340, 260)
        self.setStyleSheet(DARK_STYLESHEET)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(20, 16, 20, 16)
        lo.setSpacing(10)
        lo.addWidget(QLabel("Time Range:"))
        self.time_combo = QComboBox()
        self.time_combo.addItems(["12 Hours", "24 Hours", "2 Days", "7 Days"])
        lo.addWidget(self.time_combo)
        lo.addWidget(QLabel("Sheet Scope:"))
        self.scope_combo = QComboBox()
        self.scope_combo.addItems(["Current Sheet", "All Sheets"])
        lo.addWidget(self.scope_combo)
        lo.addStretch()
        bl = QHBoxLayout()
        eb = QPushButton("Export")
        eb.setObjectName("success")
        eb.clicked.connect(self._export)
        cb = QPushButton("Cancel")
        cb.setObjectName("danger")
        cb.clicked.connect(self.reject)
        bl.addWidget(eb)
        bl.addWidget(cb)
        lo.addLayout(bl)

    def _export(self):
        tr = self.time_combo.currentText()
        scope = self.scope_combo.currentText()
        hm = {"12 Hours": 12, "24 Hours": 24, "2 Days": 48, "7 Days": 168}
        start = datetime.now() - timedelta(hours=hm[tr])
        fn, _ = QFileDialog.getSaveFileName(self, "Save Excel", "", "Excel files (*.xlsx)")
        if not fn:
            return
        # FIX: dispatch the heavy DB query + pandas + xlsxwriter work to the
        # ExportWorker QThread so the GUI never freezes during export.
        a = self.app
        try:
            if scope == "All Sheets":
                nn = [n.node_name for n in a.nodes]
                sheet_label = "All_Sheets"
            else:
                nn = [n.node_name for n in a.nodes if n.sheet_name == a.current_sheet]
                sheet_label = a.current_sheet
            if not nn:
                QMessageBox.information(self, "Info", "No nodes")
                return
            args = {"filename": fn, "node_names": nn, "start": start, "sheet_label": sheet_label}
            QMetaObject.invokeMethod(
                a._export_worker, "export_sheet",
                Qt.QueuedConnection, Q_ARG(dict, args)
            )
            a.log_audit("Exported Excel", f"{tr} ({scope}) → {fn}")
            a.log_panel.add_log(f"Excel export started → {fn}")
            QMessageBox.information(self, "Started", "Export running in background.\nYou can keep using the app.")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Export failed: {e}")


class NodeReportDialog(QDialog):
    def __init__(self, node: NodeItem, app: "App", parent=None):
        super().__init__(parent)
        self.node = node
        self.app = app
        self.setWindowTitle(f"Report — {node.node_name}")
        self.setFixedSize(320, 200)
        self.setStyleSheet(DARK_STYLESHEET)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(20, 16, 20, 16)
        lo.setSpacing(10)
        lo.addWidget(QLabel("Time Range:"))
        self.time_combo = QComboBox()
        self.time_combo.addItems(["12 Hours", "24 Hours", "2 Days", "7 Days"])
        lo.addWidget(self.time_combo)
        lo.addStretch()
        bl = QHBoxLayout()
        eb = QPushButton("Export")
        eb.setObjectName("success")
        eb.clicked.connect(self._export)
        cb = QPushButton("Cancel")
        cb.setObjectName("danger")
        cb.clicked.connect(self.reject)
        bl.addWidget(eb)
        bl.addWidget(cb)
        lo.addLayout(bl)

    def _export(self):
        tr = self.time_combo.currentText()
        hm = {"12 Hours": 12, "24 Hours": 24, "2 Days": 48, "7 Days": 168}
        start = datetime.now() - timedelta(hours=hm[tr])
        fn, _ = QFileDialog.getSaveFileName(self, f"Report — {self.node.node_name}", "", "Excel files (*.xlsx)")
        if not fn:
            return
        # FIX: dispatch to ExportWorker thread so the GUI doesn't freeze.
        a = self.app
        try:
            args = {"filename": fn, "node_name": self.node.node_name, "start": start}
            QMetaObject.invokeMethod(
                a._export_worker, "export_node_report",
                Qt.QueuedConnection, Q_ARG(dict, args)
            )
            a.log_audit("Node report", f"{tr} — '{self.node.node_name}' → {fn}")
            QMessageBox.information(self, "Started", "Report running in background.\nYou can keep using the app.")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Report failed: {e}")


class SearchResultsDialog(QDialog):
    def __init__(self, items, app: "App", parent=None):
        super().__init__(parent)
        self.app = app
        self.items = items
        self.setWindowTitle("Search Results")
        self.setMinimumSize(380, 300)
        self.setStyleSheet(DARK_STYLESHEET)
        lo = QVBoxLayout(self)
        lo.setContentsMargins(16, 12, 16, 12)
        lo.setSpacing(8)
        lo.addWidget(QLabel(f"{len(items)} results found"))
        self.listw = QListWidget()
        for kind, obj, sheet in items:
            nm = obj.node_name if kind == "node" else obj.group_name
            self.listw.addItem(f"{'NODE' if kind == 'node' else 'GROUP'}: {nm} [{sheet}]")
        lo.addWidget(self.listw)
        b = QPushButton("Go to Selected")
        b.setObjectName("primary")
        b.clicked.connect(self._go)
        lo.addWidget(b)
        self.listw.itemDoubleClicked.connect(lambda: self._go())

    def _go(self):
        idx = self.listw.currentRow()
        if idx < 0:
            return
        kind, obj, sheet = self.items[idx]
        if sheet != self.app.current_sheet:
            self.app.sheet_combo.setCurrentText(sheet)
        if kind == "node":
            self.app.focus_node_by_name(obj.node_name)
            self.app._apply_search_highlight([obj])
        else:
            self.app._apply_search_highlight(obj.member_nodes, [obj])
        self.accept()


class MyHandler(FileSystemEventHandler):
    def __init__(self, app):
        self.app = app
        self._timer = None
        self._lock = threading.Lock()

    def on_modified(self, event):
        if event.is_directory:
            return
        tgt = os.path.basename(getattr(self.app, "data_file", "network_data.json"))
        if os.path.basename(event.src_path) != tgt:
            return
        if self.app._editing_node or self.app._dragging_node or self.app._user_operation_in_progress:
            return
        if getattr(self.app, "_saving_json", False) or time.time() < getattr(self.app, "_ignore_fs_events_until", 0):
            return
        try:
            if os.path.getmtime(event.src_path) <= self.app.last_save_mtime:
                return
        except Exception:
            pass
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(0.5, lambda: QTimer.singleShot(0, self.app.load_data))
            self._timer.start()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN APP
# ═══════════════════════════════════════════════════════════════════════════════
class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self._group_renames = {}
        self.setWindowTitle("GRID-SHIELD  Plant Monitor")
        self.resize(1600, 950)
        self.setStyleSheet(DARK_STYLESHEET)
        self.data_file = self._resolve_data_file()
        self._bg_executor = ThreadPoolExecutor(max_workers=int(os.environ.get("GUI_BG_WORKERS", "2")))
        self._print = logger.debug
        self._instance_id = str(_uuid.uuid4())
        self._editing_node = None
        self._dragging_node = False
        self._node_position_dirty = {}
        self._position_lock_duration = 5.0
        self._user_operation_in_progress = False
        self._save_debounce_ms = int(os.environ.get("GUI_SAVE_DEBOUNCE_MS", "300"))
        self._saving_json = False
        self._save_generation = 0
        self._saving_json_set_at = 0.0
        self._ignore_fs_events_until = 0.0
        self._ignore_db_reload_until = 0.0
        self._group_renames = {}
        self.last_save_mtime = 0.0
        self._last_loaded_db_hash = None
        self.node_clipboard = None
        self.group_clipboard = None
        self.selected_node = None
        self.highlighted_nodes = []
        self._sheet_state = {}
        self._save_gen_lock = threading.Lock()
        self._db_config = {"host": os.environ.get("DB_HOST", "localhost"),
                           "user": os.environ.get("DB_USER", "appuser"),
                           "password": os.environ.get("DB_PASS", "app@123"),
                           "database": os.environ.get("DB_NAME", "projectdb"), "autocommit": False}
        try:
            self.db_conn = pymysql.connect(**MYSQL_CONFIG, autocommit=False)
            self.db_cursor = self.db_conn.cursor()
            self.db_lock = db_lock
            self._init_database()
        except Exception as e:
            QMessageBox.critical(None, "DB Error", f"MySQL failed:\n{e}")
            sys.exit(1)
        self.current_user_id = None
        self.current_user_role = None
        self.nodes: List[NodeItem] = []
        self.groups: List[GroupItem] = []
        self.sheets = ["Main"]
        self.current_sheet = "Main"
        self.connection_lines: Dict[frozenset, ConnectionLine] = {}
        self._sheet_zoom = {"Main": 1.0}
        self._build_ui()
        self._show_login()
        if not self.current_user_id:
            sys.exit(0)
        self._update_admin_menu_visibility()
        # ── FIX: Start background workers BEFORE load_data() so timers can use them ──
        self._init_background_workers()
        self.load_data()
        self._status_timer = QTimer(self)
        self._status_timer.timeout.connect(self._reload_node_status_from_db)
        self._status_timer.start(5000)
        self._db_reload_timer = QTimer(self)
        self._db_reload_timer.timeout.connect(self._full_db_reload)
        self._db_reload_timer.start(30000)
        self._gc_timer = QTimer(self)
        self._gc_timer.timeout.connect(self._periodic_gc)
        self._gc_timer.start(10000)
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._do_save)
        self._watchdog_timer = QTimer(self)
        self._watchdog_timer.timeout.connect(self._saving_json_watchdog)
        self._watchdog_timer.start(30000)
        self.observer = Observer()
        handler = MyHandler(self)
        self.observer.schedule(handler, path=os.path.dirname(self.data_file) or ".", recursive=False)
        self.observer.start()
        self.log_panel.add_log("Application started")

    def _build_ui(self):
        mb = self.menuBar()
        fm = mb.addMenu("File")
        fm.addAction("💾 Backup", self.backup_data)
        fm.addAction("📥 Import", self.import_backup)
        fm.addSeparator()
        fm.addAction("📄 Export Excel", self.export_excel)
        fm.addSeparator()
        fm.addAction("Exit", self.close)
        em = mb.addMenu("Edit")
        em.addAction("➕ Add Node", lambda: self.add_node())
        em.addAction("📦 Create Group", self.create_group)
        vm = mb.addMenu("View")
        vm.addAction("Zoom In", lambda: self._zoom_view(1.2))
        vm.addAction("Zoom Out", lambda: self._zoom_view(0.8))
        vm.addAction("Reset Zoom", lambda: self._zoom_view(None))
        vm.addSeparator()
        vm.addAction("🌙 Toggle Theme", self._toggle_theme)
        sm = mb.addMenu("Sheets")
        sm.addAction("➕ New", self.add_sheet)
        sm.addAction("✏️ Rename", self.rename_sheet)
        sm.addAction("🗑️ Delete", self.delete_sheet)
        am = mb.addMenu("Admin")
        am.addAction("👤 Add User", self.show_add_user_popup)
        self._admin_menu = am
        cw = QWidget()
        self.setCentralWidget(cw)
        ml = QVBoxLayout(cw)
        ml.setContentsMargins(8, 8, 8, 4)
        ml.setSpacing(6)
        tb = QHBoxLayout()
        self.dashboard = DashboardWidget()
        tb.addWidget(self.dashboard, 1)
        sf = QFrame()
        sf.setObjectName("panel")
        sf.setFixedHeight(90)
        sf.setFixedWidth(280)
        sl = QVBoxLayout(sf)
        sl.setContentsMargins(12, 8, 12, 8)
        sl.addWidget(QLabel("Sheet"))
        self.sheet_combo = QComboBox()
        self.sheet_combo.addItems(self.sheets)
        self.sheet_combo.currentTextChanged.connect(self._change_sheet)
        sl.addWidget(self.sheet_combo)
        tb.addWidget(sf)
        self._theme_btn = QPushButton("🌙 Dark")
        self._theme_btn.setFixedHeight(36)
        self._theme_btn.setFixedWidth(110)
        self._theme_btn.setStyleSheet("font-weight:bold;border-radius:18px;padding:4px 14px;background:#1a1a2e;color:#e0e0e0;border:2px solid #5b8cff")
        self._theme_btn.clicked.connect(self._toggle_theme)
        self._is_dark_theme = False
        tb.addWidget(self._theme_btn)
        ml.addLayout(tb)
        hs = QSplitter(Qt.Horizontal)
        self.node_list_panel = NodeListPanel(self)
        hs.addWidget(self.node_list_panel)
        cs = QSplitter(Qt.Vertical)
        self.scene = QGraphicsScene(self)
        self.scene.setSceneRect(-5000, -5000, 10000, 10000)
        self.topo_view = TopologyView(self.scene, self)
        cs.addWidget(self.topo_view)
        self.log_panel = LogPanel()
        cs.addWidget(self.log_panel)
        cs.setSizes([600, 120])
        hs.addWidget(cs)
        self.properties_panel = PropertiesPanel(self)
        hs.addWidget(self.properties_panel)
        hs.setSizes([240, 900, 280])
        ml.addWidget(hs, 1)
        self.statusBar().showMessage("Ready")

    def _init_database(self):
        try:
            self.db_cursor.execute(
                'CREATE TABLE IF NOT EXISTS users(id INT AUTO_INCREMENT PRIMARY KEY,username VARCHAR(255) UNIQUE,password VARCHAR(255),role VARCHAR(50) DEFAULT "user")')
            self.db_cursor.execute(
                'CREATE TABLE IF NOT EXISTS node_events(id INT AUTO_INCREMENT PRIMARY KEY,plant_name VARCHAR(255),event_time DATETIME,starttime DATETIME,last_down_time DATETIME,total_down_time INT,uptime INT,disabled TINYINT(1),user_id INT)')
            try:
                self.db_cursor.execute("ALTER TABLE node_events ADD UNIQUE KEY uq_ne(plant_name,event_time,total_down_time)")
            except Exception:
                pass
            self.db_cursor.execute(
                'CREATE TABLE IF NOT EXISTS audit_logs(id INT AUTO_INCREMENT PRIMARY KEY,user_id INT,action VARCHAR(255),details TEXT,timestamp DATETIME,idempotency_key VARCHAR(64) NULL,UNIQUE KEY uq_ai(idempotency_key))')
            try:
                self.db_cursor.execute("ALTER TABLE audit_logs ADD COLUMN idempotency_key VARCHAR(64) NULL")
                self.db_cursor.execute("ALTER TABLE audit_logs ADD UNIQUE KEY uq_ai(idempotency_key)")
            except Exception:
                pass
            embedded_create_tables_if_not_exist()
            self._bootstrap_admin_user()
            self.db_conn.commit()
        except Exception as e:
            logger.warning(f"DB init: {e}")

    def _bootstrap_admin_user(self):
        try:
            self.db_cursor.execute("SELECT COUNT(*) FROM users")
            r = self.db_cursor.fetchone()
            if r and r[0] == 0:
                h = _hash_password("admin")
                try:
                    self.db_cursor.execute("INSERT INTO users(username,password,role) VALUES(%s,%s,%s)", ("admin", h, "admin"))
                except Exception:
                    self.db_cursor.execute("INSERT INTO users(username,password) VALUES(%s,%s)", ("admin", h))
        except Exception as e:
            logger.error(f"bootstrap failed: {e}")

    def _ensure_db_conn(self):
        try:
            self.db_conn.ping(reconnect=True)
        except Exception:
            try:
                self.db_conn = pymysql.connect(**MYSQL_CONFIG, autocommit=False)
                self.db_cursor = self.db_conn.cursor()
            except Exception:
                pass

    # ══════════════════════════════════════════════════════════════════
    # Background workers — DB & Export run on separate QThreads
    # ══════════════════════════════════════════════════════════════════
    def _init_background_workers(self):
        # State tracking for incoming results
        self._pending_status_request = False
        self._pending_full_request = False
        self._last_meta_ts = 0.0
        self._inflight_save_gen = -1

        # DbWorker
        self._db_thread = QThread(self)
        self._db_worker = DbWorker(MYSQL_CONFIG)
        self._db_worker.moveToThread(self._db_thread)
        self._db_worker.status_loaded.connect(self._on_status_loaded)
        self._db_worker.network_loaded.connect(self._on_network_loaded)
        self._db_worker.meta_ts.connect(self._on_meta_ts)
        self._db_worker.save_done.connect(self._on_save_done)
        self._db_worker.error.connect(lambda msg: logger.warning(f"DbWorker: {msg}"))
        self._db_thread.start()

        # ExportWorker
        self._export_thread = QThread(self)
        self._export_worker = ExportWorker(MYSQL_CONFIG)
        self._export_worker.moveToThread(self._export_thread)
        self._export_worker.finished.connect(self._on_export_finished)
        self._export_worker.failed.connect(self._on_export_failed)
        self._export_thread.start()

    @pyqtSlot(dict)
    def _on_status_loaded(self, sd):
        """Apply status-only payload received from DbWorker (runs on main thread)."""
        self._pending_status_request = False
        try:
            if not sd or not sd.get("nodes"):
                return
            nbn = {n.node_name: n for n in self.nodes}
            changed = False
            for s in sd["nodes"]:
                n = nbn.get(s["name"])
                if not n:
                    continue
                na = bool(s.get("alive", False))
                nc = s.get("color", n.color)
                if n.alive != na or n.color != nc:
                    n.alive = na
                    n.color = nc
                    n.last_active = s.get("last_active", n.last_active)
                    n.last_status_change = s.get("last_status_change", n.last_status_change)
                    n.disabled = bool(s.get("disabled", n.disabled))
                    n.total_down_time = s.get("total_down_time", n.total_down_time)
                    n.last_down_time = s.get("last_down_time", n.last_down_time)
                    n.update()
                    changed = True
            if changed:
                self._update_dashboard()
                self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
                if self.selected_node:
                    self.properties_panel.show_node(self.selected_node)
        except Exception as e:
            logger.error(f"_on_status_loaded: {e}")

    @pyqtSlot(float)
    def _on_meta_ts(self, ts):
        """Cheap meta-timestamp arrived; only fire full reload if it changed."""
        if ts > 0 and ts != self._last_meta_ts:
            self._last_meta_ts = ts
            if not self._pending_full_request:
                self._pending_full_request = True
                QMetaObject.invokeMethod(self._db_worker, "fetch_full_network", Qt.QueuedConnection)

    @pyqtSlot(dict)
    def _on_network_loaded(self, db_data):
        """Full network payload received; apply on main thread."""
        self._pending_full_request = False
        try:
            if db_data and db_data.get("nodes"):
                db_hash = hashlib.md5(
                    json.dumps(sorted([n.get("name", "") for n in db_data.get("nodes", [])])).encode()
                ).hexdigest()
                if db_hash != self._last_loaded_db_hash:
                    self._last_loaded_db_hash = db_hash
                    self._apply_db_reload_payload(db_data)
        except Exception as e:
            logger.error(f"_on_network_loaded: {e}")

    @pyqtSlot(bool, int)
    def _on_save_done(self, ok, generation):
        """Save finished on the worker thread."""
        if not ok:
            logger.warning(f"DB save failed (gen={generation})")
        # Allow next save through the worker
        if generation == self._inflight_save_gen:
            self._inflight_save_gen = -1
        # Also write JSON to disk on a small bg thread (very fast, non-blocking)
        try:
            data = self._build_save_payload()
            def _w():
                try:
                    atomic_write_json(self.data_file, data)
                    self.last_save_mtime = os.path.getmtime(self.data_file)
                except Exception:
                    pass
                self._saving_json = False
                self._ignore_fs_events_until = time.time() + 1.0
            self._bg_executor.submit(_w)
        except Exception:
            self._saving_json = False

    @pyqtSlot(str)
    def _on_export_finished(self, fn):
        QMessageBox.information(self, "Success", f"Exported!\n{fn}")
        try:
            self.log_audit("Export Finished", f"File='{fn}'")
            self.log_panel.add_log(f"Export complete → {fn}")
        except Exception:
            pass

    @pyqtSlot(str)
    def _on_export_failed(self, msg):
        QMessageBox.critical(self, "Export Failed", msg)
        try:
            self.log_panel.add_log(f"Export failed: {msg}")
        except Exception:
            pass

    def _show_login(self):
        d = LoginDialog(self.db_cursor, self.db_conn, self)
        if d.exec_() == QDialog.Accepted:
            self.current_user_id = d.user_id
            self.current_user_role = d.user_role
        else:
            self.current_user_id = None

    def show_add_user_popup(self):
        if self.current_user_role != "admin":
            QMessageBox.warning(self, "Access Denied", "Admin only")
            return
        self._ensure_db_conn()
        AddUserDialog(self.db_cursor, self.db_conn, app=self, parent=self).exec_()

    # ── Data Loading ──
    def load_data(self):
        data = None
        try:
            import mysql_driver
            try:
                mysql_driver.create_tables_if_not_exist()
            except Exception:
                pass
            data = mysql_driver.load_network_from_mysql()
            #print(f"Loaded from MySQL: {len(data.get('nodes', []))} nodes, {len(data.get('groups', []))} groups")
        except Exception as e:
            logger.error(f"MySQL load: {e}")
        if not data:
            data = self._load_data_from_file(self.data_file)
        if not data:
            data = {"sheets": ["Main"], "current_sheet": "Main", "sheet_zoom": {}, "nodes": [], "groups": []}
        self._apply_loaded_data(data)

    def _load_data_from_file(self, filename):
        if not os.path.exists(filename):
            return None
        try:
            if os.path.getsize(filename) == 0:
                return None
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None

    def _apply_loaded_data(self, data):
        prev_sheet = self.current_sheet
        self.sheets = data.get('sheets', ['Main'])
        self.current_sheet = prev_sheet if prev_sheet in self.sheets else (self.sheets[0] if self.sheets else "Main")
        self.sheet_combo.blockSignals(True)
        self.sheet_combo.clear()
        self.sheet_combo.addItems(self.sheets)
        self.sheet_combo.setCurrentText(self.current_sheet)
        self.sheet_combo.blockSignals(False)
        saved_zoom = data.get('sheet_zoom', {}) if isinstance(data.get('sheet_zoom'), dict) else {}
        file_nodes = data.get('nodes', [])
        file_groups = data.get('groups', [])
       #print(len(file_groups))
        file_names = {n.get('name') for n in file_nodes if n.get('name')}
        existing_by_name = {n.node_name: n for n in self.nodes}
        _now = time.time()
        to_remove = [n for n in self.nodes if n.node_name not in file_names]
        for n in to_remove:
            if n.scene():
                self.scene.removeItem(n)
            if n in self.nodes:
                self.nodes.remove(n)
            if n.group:
                n.group.remove_node(n)
        nbn = {}
        for nd in file_nodes:
            name = nd.get('name')
            if not name:
                continue
            bx = float(nd.get('base_x', nd.get('x', 100)))
            by = float(nd.get('base_y', nd.get('y', 100)))
            if name in existing_by_name:
                node = existing_by_name[name]
                dirty_until = self._node_position_dirty.get(name, 0.0)
                locally_dirty = _now < dirty_until
                db_ts = float(nd.get('position_updated_at', 0.0))
                local_ts = node._position_updated_at
                moved_by_us = nd.get('last_moved_by') == self._instance_id
                if not locally_dirty and db_ts >= local_ts and not moved_by_us:
                    node.base_x = bx
                    node.base_y = by
                    node.setPos(bx, by)
                    if db_ts > 0:
                        node._position_updated_at = db_ts
                node.ip1 = nd.get('ip1', node.ip1)
                node.ip2 = nd.get('ip2', node.ip2)
                node.shape_type = nd.get('shape', node.shape_type)
                node.color = nd.get('color', node.color)
                node.alive = bool(nd.get('alive', node.alive))
                node.previous_alive = bool(nd.get('previous_alive', node.previous_alive))
                node.last_status_change = nd.get('last_status_change', node.last_status_change)
                node.disabled = bool(nd.get('disabled', node.disabled))
                node.last_active = nd.get('last_active', node.last_active)
                node.total_down_time = nd.get('total_down_time', node.total_down_time)
                node.last_down_time = nd.get('last_down_time', node.last_down_time)
                node.notes = nd.get('notes', node.notes)
                node.node_size = safe_int(nd.get('size', node.node_size), node.node_size)
                node.sheet_name = nd.get('sheet_name', node.sheet_name)
                node.update()
                nbn[name] = node
            else:
                node = NodeItem(nd, self)
                self.nodes.append(node)
                nbn[name] = node
        gbn = {}
        for gd in file_groups:
            gn = gd.get('name')
            #print(gn)
            sh = gd.get('sheet_name', 'Main')
            existing_g = next((g for g in self.groups if g.group_name == gn and g.sheet_name == sh), None)
            if existing_g:
                gi = existing_g
                gi.member_nodes.clear()
            else:
                gi = GroupItem(gd, self)
                self.groups.append(gi)
            for mn in gd.get('nodes', []):
                n = nbn.get(mn)
                if n:
                    gi.add_node(n)
            gbn[(gn, sh)] = gi
        for nd in file_nodes:
            src = nbn.get(nd.get('name'))
            if src:
                src.connections.clear()
                for tn in nd.get('connections', []):
                    tgt = nbn.get(tn)
                    if tgt:
                        src.connections.append(tgt)
        for nd in file_nodes:
            gn = nd.get('group')
            sh = nd.get('sheet_name', 'Main')
            if gn and nd.get('name') in nbn:
                gi = gbn.get((gn, sh))
                if gi and nbn[nd['name']] not in gi.member_nodes:
                    gi.add_node(nbn[nd['name']])
        self._redraw_sheet()

    def _update_scene_rect(self):
        br = self.scene.itemsBoundingRect()
        if not br.isValid() or br.isNull():
            self.scene.setSceneRect(-500, -500, 1000, 1000)
            return
        pad = max(80, int(max(br.width(), br.height()) * 0.15))
        rect = br.adjusted(-pad, -pad, pad, pad)
        if rect.width() <= 0 or rect.height() <= 0:
            rect = QRectF(-500, -500, 1000, 1000)
        self.scene.setSceneRect(rect)

    def _redraw_sheet(self):
        for item in list(self.scene.items()):
            self.scene.removeItem(item)
        self.connection_lines.clear()
        self.validate_and_repair_groups()
        for gi in self.groups:
            if gi.sheet_name == self.current_sheet:
                self.scene.addItem(gi)
                gi.update_boundaries()
        sheet_nodes = [n for n in self.nodes if n.sheet_name == self.current_sheet]
        if len(sheet_nodes) > 200:
            self._redraw_nodes_in_batches(sheet_nodes)
            self._redraw_connections_in_batches(sheet_nodes)
        else:
            for n in sheet_nodes:
                self.scene.addItem(n)
            for n in sheet_nodes:
                for c in n.connections:
                    if c.sheet_name != self.current_sheet:
                        continue
                    key = frozenset({n.node_name, c.node_name})
                    if key not in self.connection_lines:
                        line = ConnectionLine(n, c)
                        self.scene.addItem(line)
                        self.connection_lines[key] = line
        self._update_scene_rect()
        self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
        self._update_dashboard()

    # ── Save ──
    def save_data(self):
        _now = time.time()
        self._node_position_dirty = {k: v for k, v in self._node_position_dirty.items() if v > _now}
        with self._save_gen_lock:
            self._save_generation += 1
        self._ignore_db_reload_until = time.time() + 1.5
        self._save_timer.start(self._save_debounce_ms)

    def _do_save(self):
        with self._save_gen_lock:
            current_gen = self._save_generation
        data = self._build_save_payload()
        data['_save_generation'] = current_gen
        self._saving_json = True
        self._saving_json_set_at = time.time()

        def _bg():
            try:
                with self._save_gen_lock:
                    if data.get('_save_generation') != self._save_generation:
                        self._saving_json = False
                        return
                try:
                    embedded_save_network_to_mysql(data)
                except Exception as e:
                    logger.error(f"BG MySQL: {e}")
                try:
                    atomic_write_json(self.data_file, data)
                    self.last_save_mtime = os.path.getmtime(self.data_file)
                except Exception:
                    pass
                self._saving_json = False
                self._ignore_fs_events_until = time.time() + 1.0
            except Exception:
                self._saving_json = False

        self._bg_executor.submit(_bg)

    def _build_save_payload(self):
        return {"sheets": list(self.sheets), "current_sheet": self.current_sheet,
                "nodes": [n.to_dict() for n in self.nodes], "groups": [g.to_dict() for g in self.groups],
                "sheet_zoom": dict(self._sheet_zoom)}

    def _saving_json_watchdog(self):
        if self._saving_json and time.time() - self._saving_json_set_at > 30:
            logger.warning("_saving_json stuck >30s — resetting")
            self._saving_json = False
            self._saving_json_set_at = 0

    # ── Node ops ──
    def add_node(self, x=None, y=None):
        self._user_operation_in_progress = True
        try:
            if x is None or y is None:
                c = self.topo_view.mapToScene(self.topo_view.viewport().rect().center())
                x, y = c.x(), c.y()
            nm = f"Node{len(self.nodes) + 1}"
            ex = {n.node_name for n in self.nodes}
            while nm in ex:
                nm = f"Node{len(self.nodes) + 1}_{int(time.time()) % 1000}"
            nd = {'name': nm, 'ip1': '', 'ip2': '', 'shape': 'circle', 'sheet_name': self.current_sheet, 'base_x': x,
                  'base_y': y, 'size': 35, 'starttime': time.time(), 'color': '#e74c3c'}
            node = NodeItem(nd, self)
            self.nodes.append(node)
            if node.sheet_name == self.current_sheet:
                self.scene.addItem(node)
            self._node_position_dirty[nm] = time.time() + 15.0
            node._position_updated_at = time.time()
            self.log_audit("Node Added", f"Name='{nm}' | Sheet='{self.current_sheet}' | Pos=({x:.0f},{y:.0f})")
            self.log_panel.add_log(f"Added '{nm}'")
            self.save_data()
            self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
            self._update_dashboard()
        finally:
            self._user_operation_in_progress = False

    def delete_node(self, node):
        self._user_operation_in_progress = True
        try:
            if QMessageBox.question(self, "Confirm", f"Delete '{node.node_name}'?",
                                    QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
                return
            nm = node.node_name
            sheet = node.sheet_name
            grp = node.group.group_name if node.group else "None"
            if node.group:
                node.group.remove_node(node)
            for o in self.nodes:
                if node in o.connections:
                    o.connections.remove(node)
            for k in list(self.connection_lines):
                if nm in k:
                    l = self.connection_lines.pop(k)
                    self.scene.removeItem(l)
            if node.scene():
                self.scene.removeItem(node)
            if node in self.nodes:
                self.nodes.remove(node)
            if self.selected_node is node:
                self.selected_node = None
                self.properties_panel.show_node(None)
            try:
                self._ensure_db_conn()
                self.db_cursor.execute("DELETE FROM node_events WHERE plant_name=%s", (nm,))
                self.db_cursor.execute("DELETE FROM nodes WHERE name=%s", (nm,))
                self.db_conn.commit()
            except Exception:
                pass
            self.log_audit("Node Deleted", f"Name='{nm}' | Sheet='{sheet}' | Group='{grp}'")
            self.log_panel.add_log(f"Deleted '{nm}'")
            self._ignore_fs_events_until = time.time() + 2.0
            self.save_data()
            self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
            self._update_dashboard()
        finally:
            self._user_operation_in_progress = False

    def copy_node(self, n):
        self.node_clipboard = {"name": f"{n.node_name}_copy", "ip1": n.ip1, "ip2": n.ip2, "shape": n.shape_type,
                               "size": n.node_size, "notes": n.notes}
        self.log_audit("Node Copied", f"SourceNode='{n.node_name}' | Sheet='{n.sheet_name}'")
        self.log_panel.add_log(f"Copied '{n.node_name}'")

    def paste_node(self, x, y):
        if not self.node_clipboard:
            return
        nd = dict(self.node_clipboard)
        nm = nd.get('name', 'Pasted')
        ex = {n.node_name for n in self.nodes}
        while nm in ex:
            nm = f"{nm}_{int(time.time()) % 1000}"
        nd.update({'name': nm, 'base_x': x, 'base_y': y, 'sheet_name': self.current_sheet, 'starttime': time.time(),
                   'color': '#e74c3c'})
        node = NodeItem(nd, self)
        self.nodes.append(node)
        if node.sheet_name == self.current_sheet:
            self.scene.addItem(node)
        self.log_audit("Node Pasted", f"Name='{nm}' | Sheet='{self.current_sheet}' | Pos=({x:.0f},{y:.0f})")
        self.save_data()
        self.log_panel.add_log(f"Pasted '{nm}'")
        self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)

    def disable_node(self, n):
        n.disabled = True
        n.color = "#e74c3c"
        n.alive = False
        n.last_status_change = time.time()
        n.last_down_time = time.time()
        n.total_down_time = 0
        n.update()
        self.log_audit("Node Disabled", f"Name='{n.node_name}' | Sheet='{n.sheet_name}' | IP1='{n.ip1}' | IP2='{n.ip2}'")
        self.save_data()
        self.log_panel.add_log(f"Disabled '{n.node_name}'")

    def enable_node(self, n):
        n.disabled = False
        n.last_status_change = time.time()
        n.starttime = time.time()
        n.update()
        self.log_audit("Node Enabled", f"Name='{n.node_name}' | Sheet='{n.sheet_name}' | IP1='{n.ip1}' | IP2='{n.ip2}'")
        self.save_data()
        self.log_panel.add_log(f"Enabled '{n.node_name}'")

    def edit_node_dialog(self, n):
        self._editing_node = n
        EditNodeDialog(n, self, self).exec_()
        self._editing_node = None
        self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
        if self.selected_node is n:
            self.properties_panel.show_node(n)

    def select_node_item(self, n):
        if self.selected_node and self.selected_node is not n:
            self.selected_node.set_selected(False)
        self.selected_node = n
        n.set_selected(True)
        self.properties_panel.show_node(n)

    def focus_node_by_name(self, nm):
        n = self.get_node_by_name(nm)
        if n:
            self.select_node_item(n)
            self.topo_view.centerOn(n)

    def get_node_by_name(self, nm):
        for n in self.nodes:
            if n.node_name == nm:
                return n
        return None

    # ── Group ops ──
    def create_group(self):
        nm, ok = QInputDialog.getText(self, "Create Group", "Group name:")
        if ok and nm.strip():
            gi = GroupItem({'name': nm.strip(), 'sheet_name': self.current_sheet, 'nodes': []}, self)
            self.groups.append(gi)
            if gi.sheet_name == self.current_sheet:
                self.scene.addItem(gi)
            self.log_audit("Group Created", f"Group='{nm.strip()}' | Sheet='{self.current_sheet}'")
            self.save_data()
            self.log_panel.add_log(f"Created group '{nm}'")

    def rename_group_dialog(self, g):
        nm, ok = QInputDialog.getText(self, "Rename", f"New name:", text=g.group_name)
        if ok and nm.strip():
            nm = nm.strip()
            old = g.group_name

            # Check if new name already exists on same sheet
            existing = next((gr for gr in self.groups if gr.group_name == nm and gr.sheet_name == g.sheet_name), None)
            if existing and existing != g:
                QMessageBox.warning(self, "Error", f"Group '{nm}' already exists!")
                return

            # ── FIX: Block background reload from overwriting rename while we work.
            self._user_operation_in_progress = True
            try:
                # 1. UPDATE DB first (synchronous, on main-thread cursor)
                # This ensures old name is gone from DB before save_data() runs.
                try:
                    self._ensure_db_conn()
                    self.db_cursor.execute(
                        "UPDATE node_groups SET name=%s WHERE name=%s AND sheet_name=%s",
                        (nm, old, g.sheet_name))
                    self.db_conn.commit()
                except Exception as e:
                    logger.error(f"Group rename DB error: {e}")
                    QMessageBox.critical(self, "DB Error", f"Rename failed in DB:\n{e}")
                    return

                # 2. Update in-memory AFTER DB success
                g.group_name = nm
                g.update()

                # 3. Extend ignore window so async background save/reload
                #    does NOT race against our rename.
                self._ignore_db_reload_until = time.time() + 5.0
                self._ignore_fs_events_until = time.time() + 5.0

                self.log_audit("Group Renamed", f"OldName='{old}' → NewName='{nm}' | Sheet='{g.sheet_name}'")
                self.log_panel.add_log(f"Renamed '{old}'→'{nm}'")
                self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)

                # 4. Trigger save — embedded_save will now write new name,
                #    and our stale-group DELETE fix above will remove old name row.
                self.save_data()
            finally:
                self._user_operation_in_progress = False

    def delete_group(self, g):
        if QMessageBox.question(self, "Confirm", f"Delete group '{g.group_name}'?\n(Nodes will NOT be deleted)",
                               QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        gn = g.group_name
        sh = g.sheet_name
        members = [n.node_name for n in g.member_nodes]
        
        # 1. UI Scene aur Groups list se remove karein
        if g.scene():
            self.scene.removeItem(g)
        if g in self.groups:
            self.groups.remove(g)
            
        # 2. IMPORTANT: Saare nodes me se is group ka reference khatam karein
        # Taki backend database insert logic ise dobara nodes se padh kar recreate na kare
        for n in list(g.member_nodes):
            g.remove_node(n)
            
        # Extra safety: Pure self.nodes list me agar koi aur node bhi is group name se bacha ho, to safe side clear karein
        for n in self.nodes:
            if hasattr(n, 'group') and n.group == gn:
                n.group = None

        try:
            self._ensure_db_conn()
            # 3. Database se group aur unke members dono saaf karein
            self.db_cursor.execute("DELETE FROM node_groups WHERE name=%s AND sheet_name=%s", (gn, sh))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Error deleting group from DB: {e}")
            
        self.log_audit("Group Deleted", f"Group='{gn}' | Sheet='{sh}' | Members=[{', '.join(members)}]")
        
        # 4. Immediate update trigger karein taaki data aur network_data.json sync ho jayein
        self.save_data()
        self.log_panel.add_log(f"Deleted group '{gn}'")
        
        # 5. UI side list ko refresh karein taaki left panel me se bhi hat jaye
        if hasattr(self, 'node_list_panel'):
            self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)

    def delete_group_and_nodes(self, g):
        if QMessageBox.question(self, "Confirm", f"Delete group '{g.group_name}' AND ALL its nodes?",
                               QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return

        self._user_operation_in_progress = True
        try:
            gn = g.group_name
            sh = g.sheet_name
            nodes_to_del = list(g.member_nodes)
            members_names = [n.node_name for n in nodes_to_del]
            # ── FIX: Build a set of NodeItem objects (not strings) for O(1) lookup
            nodes_to_del_set = set(nodes_to_del)

            # 1. Remove connection_lines from scene for deleted nodes
            for n in nodes_to_del:
                for k in list(self.connection_lines):
                    if n.node_name in k:
                        line = self.connection_lines.pop(k)
                        self.scene.removeItem(line)

            # 2. Remove deleted nodes from scene and app.nodes
            for n in nodes_to_del:
                g.remove_node(n)   # clears n.group = None
                if n.scene():
                    self.scene.removeItem(n)
                if n in self.nodes:
                    self.nodes.remove(n)

            # 3. ── FIX: Compare NodeItem objects, NOT strings
            #    Old code: `c not in members` compared NodeItem vs str → always True → never removed
            for remaining_node in self.nodes:
                if hasattr(remaining_node, 'connections'):
                    remaining_node.connections = [
                        c for c in remaining_node.connections if c not in nodes_to_del_set
                    ]

            # 4. Remove group from scene and app.groups
            if g.scene():
                self.scene.removeItem(g)
            if g in self.groups:
                self.groups.remove(g)

            # 5. Database: delete connections → nodes → group (respect FK order)
            try:
                self._ensure_db_conn()
                if members_names:
                    ph = ",".join(["%s"] * len(members_names))
                    self.db_cursor.execute(
                        f"DELETE FROM connections WHERE node_id IN (SELECT id FROM nodes WHERE name IN ({ph}))",
                        members_names)
                    self.db_cursor.execute(
                        f"DELETE FROM connections WHERE target_node_id IN (SELECT id FROM nodes WHERE name IN ({ph}))",
                        members_names)
                    self.db_cursor.execute(
                        f"DELETE FROM nodes WHERE name IN ({ph})", members_names)
                self.db_cursor.execute(
                    "DELETE FROM node_groups WHERE name=%s AND sheet_name=%s", (gn, sh))
                self.db_conn.commit()
            except Exception as e:
                logger.error(f"Error in delete_group_with_nodes DB logic: {e}")
                try:
                    self.db_conn.rollback()
                except Exception:
                    pass

            self.log_audit("Group & Nodes Deleted",
                           f"Group='{gn}' | Sheet='{sh}' | DeletedNodes=[{', '.join(members_names)}]")
            self._ignore_db_reload_until = time.time() + 3.0
            self._ignore_fs_events_until = time.time() + 3.0
            self.save_data()
            self.log_panel.add_log(f"Deleted group '{gn}' and its {len(members_names)} nodes")
            if hasattr(self, 'node_list_panel'):
                self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
            self._update_dashboard()
        finally:
            self._user_operation_in_progress = False

    def copy_group(self, g):
        if not g.member_nodes:
            QMessageBox.information(self, "Info", "Group empty")
            return
        mnx = min(n.base_x for n in g.member_nodes)
        mny = min(n.base_y for n in g.member_nodes)
        nnames = [n.node_name for n in g.member_nodes]
        cp = set()
        for i, a in enumerate(g.member_nodes):
            for b in a.connections:
                if b in g.member_nodes:
                    j = nnames.index(b.node_name)
                    if i != j:
                        cp.add(tuple(sorted((i, j))))
        ndump = [{"name": n.node_name, "ip1": n.ip1, "ip2": n.ip2, "shape": n.shape_type, "size": n.node_size,
                  "notes": n.notes, "color": n.color, "rel_x": n.base_x - mnx, "rel_y": n.base_y - mny} for n in
                 g.member_nodes]
        self.group_clipboard = {"group_name": f"{g.group_name}_copy", "nodes": ndump, "connections": list(cp)}
        self.log_audit("Group Copied", f"Group='{g.group_name}' | Sheet='{g.sheet_name}' | NodeCount={len(ndump)}")
        self.log_panel.add_log(f"Copied group '{g.group_name}' ({len(ndump)} nodes)")

    def paste_group(self, x, y):
        if not self.group_clipboard:
            return
        self._user_operation_in_progress = True
        try:
            data = self.group_clipboard
            new_nodes = []
            ex = {n.node_name for n in self.nodes}
            created = set()
            for nd in data["nodes"]:
                orig = nd.get("name", "Node")
                cn = f"{orig}_copy"
                s = 1
                while cn in ex or cn in created:
                    cn = f"{orig}_copy_{s}"
                    s += 1
                created.add(cn)
                nnd = {'name': cn, 'ip1': nd.get('ip1', ''), 'ip2': nd.get('ip2', ''), 'shape': nd.get('shape', 'circle'),
                       'sheet_name': self.current_sheet, 'base_x': x + nd.get('rel_x', 0) + 30,
                       'base_y': y + nd.get('rel_y', 0) + 30, 'size': nd.get('size', 35), 'starttime': time.time(),
                       'color': nd.get('color', '#e74c3c')}
                node = NodeItem(nnd, self)
                node.notes = nd.get('notes', '')
                self.nodes.append(node)
                if node.sheet_name == self.current_sheet:
                    self.scene.addItem(node)
                new_nodes.append(node)
            for i, j in data.get("connections", []):
                if 0 <= i < len(new_nodes) and 0 <= j < len(new_nodes):
                    a, b = new_nodes[i], new_nodes[j]
                    if b not in a.connections:
                        a.connections.append(b)
                    if a not in b.connections:
                        b.connections.append(a)
            gn = data.get("group_name", "Group_copy")
            base = gn
            k = 1
            while any(g.group_name == gn and g.sheet_name == self.current_sheet for g in self.groups):
                k += 1
                gn = f"{base}_{k}"
            gi = GroupItem({'name': gn, 'sheet_name': self.current_sheet, 'nodes': []}, self)
            for n in new_nodes:
                gi.add_node(n)
            self.groups.append(gi)
            if gi.sheet_name == self.current_sheet:
                self.scene.addItem(gi)
                gi.update_boundaries()
            for n in new_nodes:
                for c in n.connections:
                    key = frozenset({n.node_name, c.node_name})
                    if key not in self.connection_lines:
                        line = ConnectionLine(n, c)
                        self.scene.addItem(line)
                        self.connection_lines[key] = line
            self.log_audit("Group Pasted",
                           f"Group='{gn}' | Sheet='{self.current_sheet}' | Pos=({x:.0f},{y:.0f}) | NodeCount={len(new_nodes)}")
            self.save_data()
            self.log_panel.add_log(f"Pasted group '{gn}' ({len(new_nodes)} nodes)")
            self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
            self._update_dashboard()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Paste failed: {e}")
        finally:
            self._user_operation_in_progress = False

    # ── Search ──
    def search_nodes(self, query):
        q = (query or "").lower().strip()
        if not q:
            # FIX: Do NOT call _redraw_sheet() here — it removes & re-adds every
            # scene item which freezes the UI when many nodes are present.
            # We only need to clear highlight/blink state on existing items.
            self.highlighted_nodes = []
            for n in self.nodes:
                if n.sheet_name != self.current_sheet:
                    continue
                changed = False
                if n._selected and self.selected_node is not n:
                    n._selected = False
                    changed = True
                if n._blink_on:
                    n._blink_on = False
                    changed = True
                if n._search_highlight:
                    n._search_highlight = False
                    changed = True
                if changed:
                    n.prepareGeometryChange()
                    n.update()
            return
        mn = []
        mg = []
        for n in self.nodes:
            if q in n.node_name.lower() or (n.ip1 and q in n.ip1.lower()) or (n.ip2 and q in n.ip2.lower()):
                mn.append(("node", n, n.sheet_name))
        for g in self.groups:
            if q in g.group_name.lower():
                mg.append(("group", g, g.sheet_name))
        items = mn + mg
        if not items:
            QMessageBox.information(self, "Search", "No results")
            return
        if len(items) == 1:
            k, o, s = items[0]
            if s != self.current_sheet:
                self.sheet_combo.setCurrentText(s)
            if k == "node":
                self.focus_node_by_name(o.node_name)
                self._apply_search_highlight([o])
            else:
                self._apply_search_highlight(o.member_nodes, [o])
                # FIX: avoid pointless list comprehension that builds a list of None
                for n in o.member_nodes:
                    self._blink_node(n)
        else:
            SearchResultsDialog(items, self, self).exec_()

    def _blink_node(self, node, flashes=3, interval=250):
        count = [0]
        total = flashes * 2

        def step():
            if count[0] >= total:
                node.set_blink(False)
                node._search_highlight = False
                node.prepareGeometryChange()
                node.update()
                return
            on = (count[0] % 2 == 0)
            node.set_blink(on)
            node._search_highlight = on
            node.prepareGeometryChange()
            node.update()
            count[0] += 1
            QTimer.singleShot(interval, step)

        node._search_highlight = False
        step()

    def update_connections_for(self, node):
        for c in node.connections:
            key = frozenset({node.node_name, c.node_name})
            if key in self.connection_lines:
                self.connection_lines[key].update_position()

    # ── Sheets ──
    def _change_sheet(self, sn):
        if not sn or sn not in self.sheets:
            return
        try:
            self._sheet_state[self.current_sheet] = {'zoom': self.topo_view._zoom, 'transform': self.topo_view.transform()}
        except Exception:
            pass
        old_sheet = self.current_sheet
        self.current_sheet = sn
        self.log_audit("Sheet Switched", f"From='{old_sheet}' | To='{sn}'")
        if sn in self._sheet_state:
            try:
                saved = self._sheet_state[sn]
                self.topo_view.setTransform(saved.get('transform', QTransform()))
                self.topo_view._zoom = saved.get('zoom', 1.0)
            except Exception:
                pass
        self._redraw_sheet()

    def add_sheet(self):
        nm, ok = QInputDialog.getText(self, "New Sheet", "Name:")
        if ok and nm.strip():
            if nm.strip() in self.sheets:
                QMessageBox.warning(self, "Error", "Exists")
                return
            self.sheets.append(nm.strip())
            self.sheet_combo.addItem(nm.strip())
            self.sheet_combo.setCurrentText(nm.strip())
            self.log_audit("Sheet Added", f"Name='{nm.strip()}'")
            self.log_panel.add_log(f"Added sheet '{nm.strip()}'")
            self.save_data()

    def rename_sheet(self):
        nm, ok = QInputDialog.getText(self, "Rename", "New name:", text=self.current_sheet)
        if ok and nm.strip() and nm.strip() != self.current_sheet:
            old = self.current_sheet
            idx = self.sheets.index(old)
            self.sheets[idx] = nm.strip()
            for n in self.nodes:
                if n.sheet_name == old:
                    n.sheet_name = nm.strip()
            for g in self.groups:
                if g.sheet_name == old:
                    g.sheet_name = nm.strip()
            self.current_sheet = nm.strip()
            self.sheet_combo.setItemText(self.sheet_combo.currentIndex(), nm.strip())
            self.log_audit("Sheet Renamed", f"OldName='{old}' → NewName='{nm.strip()}'")
            self.log_panel.add_log(f"Sheet renamed '{old}'→'{nm.strip()}'")
            self.save_data()

    def delete_sheet(self):
        if len(self.sheets) <= 1:
            QMessageBox.warning(self, "Error", "Last sheet")
            return
        if QMessageBox.question(self, "Confirm", f"Delete '{self.current_sheet}'?",
                                QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        old = self.current_sheet
        node_names = [n.node_name for n in self.nodes if n.sheet_name == old]
        group_names = [g.group_name for g in self.groups if g.sheet_name == old]
        try:
            self._ensure_db_conn()
            if node_names:
                ph = ','.join(['%s'] * len(node_names))
                self.db_cursor.execute(f"DELETE FROM node_events WHERE plant_name IN({ph})", node_names)
                c2 = _get_mysql_connection()
                cu = c2.cursor()
                try:
                    cu.execute("SET FOREIGN_KEY_CHECKS=0")
                    cu.execute(f"DELETE c FROM connections c JOIN nodes n ON c.node_id=n.id OR c.target_node_id=n.id WHERE n.name IN({ph})", node_names)
                    cu.execute(f"DELETE gm FROM group_members gm JOIN nodes n ON gm.node_id=n.id WHERE n.name IN({ph})", node_names)
                    cu.execute(f"DELETE FROM nodes WHERE name IN({ph})", node_names)
                    cu.execute("SET FOREIGN_KEY_CHECKS=1")
                    c2.commit()
                finally:
                    cu.close()
                    c2.close()
            if group_names:
                phg = ','.join(['%s'] * len(group_names))
                self.db_cursor.execute(f"DELETE FROM node_groups WHERE name IN({phg}) AND sheet_name=%s", group_names + [old])
                self.db_conn.commit()
        except Exception:
            pass
        self.nodes = [n for n in self.nodes if n.sheet_name != old]
        self.groups = [g for g in self.groups if g.sheet_name != old]
        self.sheets.remove(old)
        self.current_sheet = self.sheets[0]
        self.sheet_combo.blockSignals(True)
        self.sheet_combo.clear()
        self.sheet_combo.addItems(self.sheets)
        self.sheet_combo.setCurrentText(self.current_sheet)
        self.sheet_combo.blockSignals(False)
        self.log_audit("Sheet Deleted", f"Name='{old}' | Nodes=[{', '.join(node_names)}] | Groups=[{', '.join(group_names)}]")
        self.log_panel.add_log(f"Deleted sheet '{old}' ({len(node_names)} nodes, {len(group_names)} groups)")
        self._redraw_sheet()
        self.save_data()

    def _zoom_view(self, f):
        if f is None:
            self.topo_view.resetTransform()
            self.topo_view._zoom = 1.0
        else:
            self.topo_view.scale(f, f)
            self.topo_view._zoom *= f

    def _update_dashboard(self):
        sn = [n for n in self.nodes if n.sheet_name == self.current_sheet]
        t = len(sn)
        ac = [n for n in sn if not n.disabled]
        u = sum(1 for n in ac if n.alive)
        d = sum(1 for n in ac if not n.alive)
        p = (u / len(ac) * 100) if ac else 0
        self.dashboard.update_stats(t, u, d, p)

    def _reload_node_status_from_db(self):
        """FIX: dispatch to DbWorker thread; results arrive via _on_status_loaded."""
        if self._saving_json or self._editing_node or self._dragging_node or self._user_operation_in_progress:
            return
        if time.time() < self._ignore_db_reload_until:
            return
        if self._pending_status_request:
            return  # don't queue duplicate requests
        self._pending_status_request = True
        try:
            QMetaObject.invokeMethod(self._db_worker, "fetch_status", Qt.QueuedConnection)
        except Exception as e:
            self._pending_status_request = False
            logger.error(f"dispatch fetch_status: {e}")

    def log_audit(self, action, details=''):
        try:
            self._ensure_db_conn()
            ik = hashlib.md5(f"{self.current_user_id}:{action}:{details}:{time.time():.2f}".encode()).hexdigest()
            self.db_cursor.execute(
                "INSERT IGNORE INTO audit_logs(user_id,action,details,timestamp,idempotency_key) "
                "VALUES(%s,%s,%s,%s,%s)",
                (self.current_user_id, action, details, datetime.now(), ik)
            )
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"log_audit failed [{action}]: {e}")

    def log_node_event(self, pn, st, ld, td, up, dis):
        try:
            self._ensure_db_conn()
            et = datetime.now()
            sdt = datetime.fromtimestamp(st) if isinstance(st, (int, float)) and st > 0 else None
            ldt = datetime.fromtimestamp(ld) if isinstance(ld, (int, float)) and ld > 0 else None
            self.db_cursor.execute(
                "INSERT IGNORE INTO node_events(plant_name,event_time,starttime,last_down_time,total_down_time,uptime,disabled,user_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                (pn, et, sdt, ldt, td, up, dis, self.current_user_id))
            self.db_conn.commit()
        except Exception:
            pass

    def backup_data(self):
        fn, _ = QFileDialog.getSaveFileName(self, "Backup", "", "JSON files (*.json)")
        if not fn:
            return
        try:
            backup_dir = os.path.dirname(fn)
            if backup_dir and not os.path.exists(backup_dir):
                os.makedirs(backup_dir, exist_ok=True)
            with open(fn, 'w', encoding='utf-8') as f:
                json.dump(self._build_save_payload(), f, ensure_ascii=False, indent=2)
            self.log_panel.add_log(f"Backup saved → {fn}")
            self.log_audit("Backup Created", f"File='{fn}'")
            QMessageBox.information(self, "Success", f"Backup saved!\n{fn}")
        except PermissionError:
            QMessageBox.critical(self, "Error", f"Permission denied!\n{fn}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Backup failed:\n{str(e)}")

    def import_backup(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Import Backup", "", "JSON files (*.json)")
        if not fn:
            return
        try:
            if not os.path.exists(fn):
                QMessageBox.warning(self, "Error", "File not found!")
                return
            with open(fn, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict):
                QMessageBox.warning(self, "Error", "Invalid backup!")
                return
            if embedded_fast_import_to_mysql(data):
                self.load_data()
                self.log_panel.add_log(f"Imported: {fn}")
                self.log_audit("Backup Imported", f"File='{fn}'")
                QMessageBox.information(self, "Success", "Imported!")
            else:
                QMessageBox.warning(self, "Error", "Import failed!")
        except json.JSONDecodeError:
            QMessageBox.critical(self, "Error", "Invalid JSON!")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Import failed:\n{str(e)}")

    def export_excel(self):
        ExportExcelDialog(self, self).exec_()

    def generate_node_report(self, n):
        NodeReportDialog(n, self, self).exec_()

    @staticmethod
    def format_uptime(seconds):
        try:
            seconds = int(seconds or 0)
        except Exception:
            seconds = 0
        d, r = divmod(seconds, 86400)
        h, r = divmod(r, 3600)
        m, s = divmod(r, 60)
        return f"{d:02d}:{h:02d}:{m:02d}:{s:02d}"

    def fmt_dt(self, val):
        if val is None:
            return "-"
        try:
            if isinstance(val, datetime):
                return val.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(val, (int, float)):
                ts = float(val)
                if ts > 1e12:
                    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                elif ts > 1e9:
                    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                return "-"
            if isinstance(val, str):
                s = val.strip()
                if not s or s == "-":
                    return "-"
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass
                return s
        except Exception:
            return "-"
        return "-"

    def _resolve_data_file(self):
        cands = []
        try:
            cands.append(os.path.join(os.getcwd(), "network_data.json"))
        except Exception:
            pass
        try:
            cands.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_data.json"))
        except Exception:
            pass
        ex = [p for p in cands if p and os.path.exists(p)]
        if ex:
            try:
                return max(ex, key=lambda p: os.path.getmtime(p))
            except Exception:
                return ex[0]
        return os.path.join(os.getcwd(), "network_data.json")

    def _periodic_gc(self):
        try:
            gc.collect()
            _n = time.time()
            self._node_position_dirty = {k: v for k, v in self._node_position_dirty.items() if v > _n}
        except Exception:
            pass

    def _update_admin_menu_visibility(self):
        try:
            if hasattr(self, '_admin_menu'):
                self._admin_menu.menuAction().setVisible(self.current_user_role == "admin")
        except Exception:
            pass

    def _full_db_reload(self):
        """FIX: cheap meta-timestamp check first; only fetch full network if changed.
        Both queries run on the DbWorker thread, never on the main GUI thread."""
        if self._saving_json or self._editing_node or self._dragging_node or self._user_operation_in_progress:
            return
        if time.time() < self._ignore_db_reload_until:
            return
        try:
            QMetaObject.invokeMethod(self._db_worker, "fetch_meta_ts", Qt.QueuedConnection)
        except Exception as e:
            logger.error(f"dispatch fetch_meta_ts: {e}")

    def _apply_db_reload_payload(self, db_data):
        if not db_data or not db_data.get('nodes'):
            return
        _now = time.time()
        nbn = {n.node_name: n for n in self.nodes}
        file_names = {nd.get('name') for nd in db_data.get('nodes', []) if nd.get('name')}
        changed = False
        
        # Remove nodes that are no longer in the DB
        to_remove = [n for n in self.nodes if n.node_name not in file_names]
        for n in to_remove:
            if n.group:
                n.group.remove_node(n)
            if n.scene():
                self.scene.removeItem(n)
            if n in self.nodes:
                self.nodes.remove(n)
            changed = True
        
        for nd in db_data.get('nodes', []):
            name = nd.get('name')
            if not name:
                continue
            bx = float(nd.get('base_x', nd.get('x', 100)))
            by = float(nd.get('base_y', nd.get('y', 100)))
            if name in nbn:
                node = nbn[name]
                dirty_until = self._node_position_dirty.get(name, 0.0)
                locally_dirty = _now < dirty_until
                db_ts = float(nd.get('position_updated_at', 0.0))
                local_ts = node._position_updated_at
                moved_by_us = nd.get('last_moved_by') == self._instance_id
                if not locally_dirty and db_ts >= local_ts and not moved_by_us:
                    node.base_x = bx
                    node.base_y = by
                    node.setPos(bx, by)
                    if db_ts > 0:
                        node._position_updated_at = db_ts
                for fld, attr in [('ip1', 'ip1'), ('ip2', 'ip2'), ('color', 'color'), ('last_active', 'last_active'),
                                ('last_status_change', 'last_status_change'), ('total_down_time', 'total_down_time'),
                                ('last_down_time', 'last_down_time'), ('notes', 'notes'), ('sheet_name', 'sheet_name')]:
                    val = nd.get(fld)
                    if val is not None:
                        setattr(node, attr, val)
                node.alive = bool(nd.get('alive', node.alive))
                node.disabled = bool(nd.get('disabled', node.disabled))
                node.previous_alive = bool(nd.get('previous_alive', node.previous_alive))
                node.node_size = safe_int(nd.get('size', node.node_size), node.node_size)
                node.shape_type = nd.get('shape', node.shape_type)
                node.update()
                changed = True
            else:
                node = NodeItem(nd, self)
                self.nodes.append(node)
                if node.sheet_name == self.current_sheet:
                    self.scene.addItem(node)
                nbn[name] = node
                changed = True
        
        if db_data.get('groups'):
            for gd in db_data['groups']:
                gn = gd.get('name')
                sh = gd.get('sheet_name', 'Main')
                
                # FIX: Check if this group was locally renamed (track by old name)
                group_renames = self._group_renames.get(sh, {})
                old_name_mappings = {v: k for k, v in group_renames.items()}
                
                # Try to find existing group by new name OR old name
                existing_g = next((g for g in self.groups if g.group_name == gn and g.sheet_name == sh), None)
                
                if not existing_g and gn in old_name_mappings:
                    # Group was renamed locally, find it by the new name
                    old_gn = old_name_mappings[gn]
                    existing_g = next((g for g in self.groups if g.group_name == gn and g.sheet_name == sh), None)
                
                if existing_g:
                    existing_g.member_nodes.clear()
                    for mn in gd.get('nodes', []):
                        n = nbn.get(mn)
                        if n:
                            existing_g.add_node(n)
                else:
                    gi = GroupItem(gd, self)
                    self.groups.append(gi)
                    for mn in gd.get('nodes', []):
                        n = nbn.get(mn)
                        if n:
                            gi.add_node(n)
                    if gi.sheet_name == self.current_sheet:
                        self.scene.addItem(gi)
                        gi.update_boundaries()
                    changed = True
        
        # Clear old renames after applying
        self._group_renames = {}
        
        if changed:
            self._update_dashboard()
            self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
            if self.selected_node:
                self.properties_panel.show_node(self.selected_node)
    
        if db_data.get('groups'):
            for gd in db_data['groups']:
                gn = gd.get('name')
                sh = gd.get('sheet_name', 'Main')
                existing_g = next((g for g in self.groups if g.group_name == gn and g.sheet_name == sh), None)
                if existing_g:
                    existing_g.member_nodes.clear()
                    for mn in gd.get('nodes', []):
                        n = nbn.get(mn)
                        if n:
                            existing_g.add_node(n)
                else:
                    gi = GroupItem(gd, self)
                    self.groups.append(gi)
                    for mn in gd.get('nodes', []):
                        n = nbn.get(mn)
                        if n:
                            gi.add_node(n)
                    if gi.sheet_name == self.current_sheet:
                        self.scene.addItem(gi)
                        gi.update_boundaries()
        if changed:
            self._update_dashboard()
            self.node_list_panel.refresh(self.nodes, self.groups, self.current_sheet)
            if self.selected_node:
                self.properties_panel.show_node(self.selected_node)

    def validate_and_repair_groups(self):
        valid_node_names = {n.node_name for n in self.nodes}
        for g in self.groups:
            orphans = [n for n in g.member_nodes if n.node_name not in valid_node_names]
            for n in orphans:
                g.member_nodes.remove(n)
            for n in g.member_nodes:
                if n.group is not g:
                    n.group = g
            g.update_boundaries()
        self._cleanup_phantom_groups()

    def _cleanup_phantom_groups(self):
        phantoms = [g for g in self.groups if not g.member_nodes]
        for g in phantoms:
            try:
                if g.scene():
                    self.scene.removeItem(g)
            except Exception:
                pass
            if g in self.groups:
                self.groups.remove(g)
        if phantoms:
            logger.info(f"Cleaned up {len(phantoms)} phantom groups")

    def center_on_node(self, node):
        try:
            self.topo_view.centerOn(node)
        except Exception:
            pass

    def ensure_node_visible(self, node, margin=80):
        try:
            vp_rect = self.topo_view.mapToScene(self.topo_view.viewport().rect()).boundingRect()
            if vp_rect.contains(node.pos()):
                return
            self.topo_view.centerOn(node)
        except Exception:
            pass

    def _apply_search_highlight(self, matched_nodes, matched_groups=None):
        self.highlighted_nodes = list(matched_nodes)
        for n in self.nodes:
            if n.sheet_name != self.current_sheet:
                continue
            n._selected = False
            n.update()
        for n in self.highlighted_nodes:
            if n.sheet_name != self.current_sheet:
                continue
            self._blink_node(n, flashes=3, interval=300)
        if matched_groups:
            for g in matched_groups:
                for n in g.member_nodes:
                    self._blink_node(n, flashes=3, interval=300)

    def save_data_to_file(self, filename):
        try:
            data = self._build_save_payload()
            d = os.path.dirname(os.path.abspath(filename))
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            self.log_panel.add_log(f"Saved to {filename}")
        except Exception as e:
            logger.error(f"save_data_to_file error: {e}")

    def debug_node_state(self, node):
        if not node:
            return
        info = (f"[DEBUG] Node: {node.node_name} | Pos: ({node.base_x:.1f},{node.base_y:.1f}) | "
                f"Sheet: {node.sheet_name} | Alive: {node.alive} | Color: {node.color} | "
                f"Size: {node.node_size} | Group: {node.group.group_name if node.group else 'None'} | "
                f"Connections: {[c.node_name for c in node.connections]} | "
                f"PosTS: {node._position_updated_at:.2f} | Disabled: {node.disabled}")
        logger.debug(info)
        self.log_panel.add_log(info)

    def _save_network_to_mysql_direct(self, data):
        try:
            return embedded_save_network_to_mysql(data)
        except Exception as e:
            logger.error(f"Direct MySQL save error: {e}")
            return False

    def _redraw_nodes_in_batches(self, nodes, batch_size=200):
        for i in range(0, len(nodes), batch_size):
            batch = nodes[i:i + batch_size]
            for n in batch:
                if n.sheet_name == self.current_sheet:
                    if not n.scene():
                        self.scene.addItem(n)
                    n.update()
            QApplication.processEvents()

    def _redraw_connections_in_batches(self, nodes, batch_size=200):
        for i in range(0, len(nodes), batch_size):
            batch = nodes[i:i + batch_size]
            for n in batch:
                if n.sheet_name != self.current_sheet:
                    continue
                for c in n.connections:
                    if c.sheet_name != self.current_sheet:
                        continue
                    key = frozenset({n.node_name, c.node_name})
                    if key not in self.connection_lines:
                        line = ConnectionLine(n, c)
                        self.scene.addItem(line)
                        self.connection_lines[key] = line
            QApplication.processEvents()

    # ══════════════════════════════════════════════════════════════════════
    # THEME TOGGLE — Dark ↔ Light (FIXED)
    # ══════════════════════════════════════════════════════════════════════
    def _toggle_theme(self):
        global DARK_STYLESHEET, _CURRENT_COLORS, CUP, CDN, CDIS, CSEL, CHOV, CCON, CGBG, CGBD, CBG
        self._is_dark_theme = not self._is_dark_theme
        if self._is_dark_theme:
            DARK_STYLESHEET = REAL_DARK_STYLESHEET
            _CURRENT_COLORS.update(DARK_COLORS)
            self._theme_btn.setText("☀️ Light")
            self._theme_btn.setStyleSheet("font-weight:bold;border-radius:18px;padding:4px 14px;background:#f5f6fa;color:#2d3436;border:2px solid #3a7cff")
            bg_brush = QColor("#0a0a1a")
            palette_win = QColor("#1a1a2e")
            palette_text = QColor("#e0e0e0")
            palette_base = QColor("#16213e")
            palette_alt = QColor("#1a2744")
            palette_btn = QColor("#16213e")
            palette_btn_text = QColor("#e0e0e0")
        else:
            DARK_STYLESHEET = LIGHT_STYLESHEET
            _CURRENT_COLORS.update(LIGHT_COLORS)
            self._theme_btn.setText("🌙 Dark")
            self._theme_btn.setStyleSheet("font-weight:bold;border-radius:18px;padding:4px 14px;background:#1a1a2e;color:#e0e0e0;border:2px solid #5b8cff")
            bg_brush = QColor("#ffffff")
            palette_win = QColor("#f5f6fa")
            palette_text = QColor("#2d3436")
            palette_base = QColor("#ffffff")
            palette_alt = QColor("#f9fafb")
            palette_btn = QColor("#ffffff")
            palette_btn_text = QColor("#2d3436")
        # Update global color vars
        CUP = _CURRENT_COLORS["CUP"]
        CDN = _CURRENT_COLORS["CDN"]
        CDIS = _CURRENT_COLORS["CDIS"]
        CSEL = _CURRENT_COLORS["CSEL"]
        CHOV = _CURRENT_COLORS["CHOV"]
        CCON = _CURRENT_COLORS["CCON"]
        CGBG = _CURRENT_COLORS["CGBG"]
        CGBD = _CURRENT_COLORS["CGBD"]
        CBG = _CURRENT_COLORS["CBG"]
        # Apply stylesheet globally
        self.setStyleSheet(DARK_STYLESHEET)
        QApplication.instance().setStyleSheet(DARK_STYLESHEET)
        # Update palette
        p = QPalette()
        p.setColor(QPalette.Window, palette_win)
        p.setColor(QPalette.WindowText, palette_text)
        p.setColor(QPalette.Base, palette_base)
        p.setColor(QPalette.AlternateBase, palette_alt)
        p.setColor(QPalette.Text, palette_text)
        p.setColor(QPalette.Button, palette_btn)
        p.setColor(QPalette.ButtonText, palette_btn_text)
        p.setColor(QPalette.Highlight, QColor("#3a7cff"))
        p.setColor(QPalette.HighlightedText, QColor("white"))
        QApplication.instance().setPalette(p)
        # Force style refresh on all top-level widgets
        for widget in QApplication.instance().topLevelWidgets():
            widget.style().unpolish(widget)
            widget.style().polish(widget)
        # Update topology view background
        self.topo_view.setBackgroundBrush(QBrush(bg_brush))
        # Update dashboard colors
        self.dashboard.update_theme_colors()
        # Update connection line colors
        for line in self.connection_lines.values():
            try:
                line.setPen(QPen(QColor(_get_color("CCON")), 2.5, Qt.SolidLine, Qt.RoundCap))
            except Exception:
                pass
        # Refresh all visuals
        for n in self.nodes:
            n.update()
        for g in self.groups:
            g.update()
        self.scene.update()
        theme_name = 'Dark' if self._is_dark_theme else 'Light'
        self.log_audit("Theme Changed", f"Theme='{theme_name}'")
        self.log_panel.add_log(f"Theme switched to {theme_name}")

    def closeEvent(self, e):
        try:
            self.save_data()
        except Exception:
            pass
        try:
            self.observer.stop()
            self.observer.join(timeout=1.5)
        except Exception:
            pass
        try:
            self._status_timer.stop()
            self._gc_timer.stop()
            self._save_timer.stop()
            self._watchdog_timer.stop()
        except Exception:
            pass
        try:
            self._db_reload_timer.stop()
        except Exception:
            pass
        
        try:
            QMetaObject.invokeMethod(self._db_worker, "shutdown", Qt.QueuedConnection)
            self._db_thread.quit()
            self._db_thread.wait(2000)
        except Exception:
            pass
        try:
            self._export_thread.quit()
            self._export_thread.wait(2000)
        except Exception:
            pass
        try:
            self._bg_executor.shutdown(wait=False)
        except Exception:
            pass
        try:
            self.db_cursor.close()
            self.db_conn.close()
        except Exception:
            pass
        e.accept()


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
    except Exception:
        pass
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    p = QPalette()
    p.setColor(QPalette.Window, QColor("#f5f6fa"))
    p.setColor(QPalette.WindowText, QColor("#2d3436"))
    p.setColor(QPalette.Base, QColor("#ffffff"))
    p.setColor(QPalette.AlternateBase, QColor("#f9fafb"))
    p.setColor(QPalette.Text, QColor("#2d3436"))
    p.setColor(QPalette.Button, QColor("#ffffff"))
    p.setColor(QPalette.ButtonText, QColor("#2d3436"))
    p.setColor(QPalette.Highlight, QColor("#3a7cff"))
    p.setColor(QPalette.HighlightedText, QColor("white"))
    app.setPalette(p)
    app.setStyleSheet(DARK_STYLESHEET)
    window = App()
    window.show()
    sys.exit(app.exec_())