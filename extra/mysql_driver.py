"""Direct MySQL storage for network data.

This module provides load/save functions that work directly with MySQL,
replacing the SQLite intermediate layer (db.py).

Database: projectdb
Tables: sheets, node_groups, nodes, group_members, connections
"""
import pymysql
import json
from typing import Dict, List, Any, Optional

# MySQL connection parameters
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
    """Load the entire network structure from MySQL normalized tables.
    
    Returns the full network data dict compatible with the JSON format,
    or None if tables are empty or don't exist.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        try:
            # Load sheets
            cursor.execute("SELECT name, zoom FROM sheets")
            sheets_rows = cursor.fetchall()
            sheets = [row['name'] for row in sheets_rows]
            if not sheets:
                sheets = ["Main"]
            
            # Load nodes with their attributes
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
            
            # Load connections
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
            
            # Load groups
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
            
            # Load group memberships
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
                    # Also set the group on the node
                    if node_name in nodes_by_name:
                        nodes_by_name[node_name]['group'] = groups_by_id[group_id]['name']
            
            # Build the complete network structure
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


def create_tables_if_not_exist() -> bool:
    """Ensure normalized tables exist in the MySQL database.

    This is safe to call on application startup; it will create any missing
    tables required by the application schema.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        try:
            # Create tables if they don't exist
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sheets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE,
                zoom DOUBLE
            ) ENGINE=InnoDB;
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS node_groups (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE,
                sheet_name VARCHAR(255)
            ) ENGINE=InnoDB;
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE,
                ip1 VARCHAR(64),
                ip2 VARCHAR(64),
                shape VARCHAR(50),
                sheet_name VARCHAR(255),
                base_x DOUBLE,
                base_y DOUBLE,
                size INT,
                starttime DOUBLE,
                total_down_time BIGINT,
                last_active VARCHAR(255),
                color VARCHAR(32),
                alive TINYINT(1),
                previous_alive TINYINT(1),
                last_status_change DOUBLE,
                disabled TINYINT(1),
                last_down_time DOUBLE,
                notes TEXT
            ) ENGINE=InnoDB;
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_members (
                group_id INT,
                node_id INT,
                PRIMARY KEY (group_id, node_id),
                FOREIGN KEY (group_id) REFERENCES node_groups(id) ON DELETE CASCADE,
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS connections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                node_id INT,
                target_node_id INT,
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (target_node_id) REFERENCES nodes(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
            ''')

            conn.commit()
            return True
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error creating tables in MySQL: {e}")
        return False


def save_network_to_mysql(data: Dict[str, Any]) -> bool:
    """Save the network structure to MySQL normalized tables.
    
    Takes the full network data dict and decomposes it into normalized tables.
    Returns True if successful, False otherwise.

    NOTE: some MySQL installations enable ``sql_safe_updates`` by default.  this
    prevents ``DELETE`` statements without a ``WHERE`` or ``LIMIT`` clause.  the
    existing implementation empties all of the normalized tables using bare
    ``DELETE FROM ...`` commands which will fail under safe‑updates.  the
    original request from the user is to disable safe updates before performing
    the removals and then re‑enable them afterwards so that imports run
    quickly and existing rows are cleared out.
    """
    # we retry once if the first attempt fails due to a closed connection. this
    # covers spurious ``Already closed`` errors that were observed on Windows
    # when the server dropped the socket mid‑transaction.
    for attempt in range(2):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            try:
                # start a transaction and disable foreign key checks while we wipe
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")

                # temporarily turn off sql_safe_updates so that "DELETE FROM ..." is
                # allowed; restore the session setting at the end of the transaction.
                cursor.execute("SET SQL_SAFE_UPDATES = 0")
                
                # Get existing groups BEFORE deleting anything (for smart rename handling)
                cursor.execute("SELECT id, name, sheet_name FROM node_groups")
                existing_groups_rows = cursor.fetchall()
                existing_group_map = {}  # Maps (old_name, sheet_name) -> id
                for row in existing_groups_rows:
                    group_id = row[0] if isinstance(row, tuple) else row['id']
                    group_name = row[1] if isinstance(row, tuple) else row['name']
                    sheet_name = row[2] if isinstance(row, tuple) else row['sheet_name']
                    existing_group_map[(group_name, sheet_name)] = group_id
                
                # Get rename mapping
                group_renames = data.get("_group_renames", {})
                reverseRenameMap = {}  # Maps new_name -> old_name for lookup
                for old_name, new_name in group_renames.items():
                    reverseRenameMap[new_name] = old_name
                
                # Clear existing data
                cursor.execute("DELETE FROM connections")
                cursor.execute("DELETE FROM group_members")
                cursor.execute("DELETE FROM nodes")
                cursor.execute("DELETE FROM node_groups")
                cursor.execute("DELETE FROM sheets")

                # re‑enable safe updates once the destructive operations are done
                cursor.execute("SET SQL_SAFE_UPDATES = 1")
                
                # Insert sheets
                sheets = data.get("sheets", ["Main"])
                for sheet_name in sheets:
                    cursor.execute(
                        "INSERT INTO sheets (name, zoom) VALUES (%s, %s)",
                        (sheet_name, 1.0)
                    )
                
                # Insert nodes and track IDs
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
                
                # Handle groups with smart ID preservation for renames
                groups_data = data.get("groups", [])
                group_name_to_id = {}
                
                # Deduplicate groups by (name, sheet_name)
                unique_groups = {}
                for group in groups_data:
                    group_name = group.get("name")
                    sheet_name = group.get("sheet_name", "Main")
                    composite_key = (group_name, sheet_name)
                    if composite_key not in unique_groups:
                        unique_groups[composite_key] = group
                
                # Insert groups with ID preservation for renames
                for (group_name, sheet_name), group in unique_groups.items():
                    old_name = reverseRenameMap.get(group_name)
                    old_group_id = None
                    
                    if old_name:
                        # This group was renamed, try to get its old ID
                        old_group_id = existing_group_map.get((old_name, sheet_name))
                    
                    if old_group_id:
                        # Re-insert with the same ID to preserve database references
                        cursor.execute(
                            "INSERT INTO node_groups (id, name, sheet_name) VALUES (%s, %s, %s)",
                            (old_group_id, group_name, sheet_name)
                        )
                        group_name_to_id[(group_name, sheet_name)] = old_group_id
                    else:
                        # Normal insert for new groups
                        cursor.execute(
                            "INSERT INTO node_groups (name, sheet_name) VALUES (%s, %s)",
                            (group_name, sheet_name)
                        )
                        group_name_to_id[(group_name, sheet_name)] = cursor.lastrowid

                # Insert group memberships
                for group in groups_data:
                    group_name = group.get("name")
                    sheet_name = group.get("sheet_name", "Main")
                    group_id = group_name_to_id.get((group_name, sheet_name))
                    if group_id:
                        for node_name in group.get("nodes", []):
                            node_id = node_name_to_id.get(node_name)
                            if node_id:
                                cursor.execute(
                                    "INSERT INTO group_members (group_id, node_id) VALUES (%s, %s)",
                                    (group_id, node_id)
                                )

                # Insert connections
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
                import traceback
                traceback.print_exc()
                return False
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            print(f"Error connecting to MySQL: {e}")
            if attempt == 0 and "Already closed" in str(e):
                # retry once
                continue
            return False
    # if we exhausted retries without returning, indicate failure
    return False


def save_network_delta(data: Dict[str, Any]) -> bool:
    """Faster import: apply delta between DB and provided data.

    - Deletes nodes that are not present in the incoming data
    - Inserts or updates nodes using multi-row INSERT ... ON DUPLICATE KEY UPDATE
    - Inserts/updates groups and group memberships for groups present in the payload
    - Replaces connections for source nodes present in the payload
    This avoids dropping all tables and is much faster for large datasets.
    """
    # retry once if initial attempt fails due to closed connection
    for attempt in range(2):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")

                # temporarily disable safe updates so we can run blind deletes during
                # the delta calculations.  this mirrors the behaviour in
                # ``save_network_to_mysql``.
                cursor.execute("SET SQL_SAFE_UPDATES = 0")

                # Ensure tables exist
                create_tables_if_not_exist()

                incoming_nodes = data.get('nodes', []) or []
                incoming_groups = data.get('groups', []) or []

                incoming_names = [n.get('name') for n in incoming_nodes if n.get('name')]

                # Find existing nodes
                if incoming_names:
                    placeholder = ','.join(['%s'] * len(incoming_names))
                    # Delete nodes not present in incoming
                    cursor.execute(f"SELECT name FROM nodes")
                    existing = [r[0] for r in cursor.fetchall()]
                    to_delete = [n for n in existing if n not in incoming_names]
                    if to_delete:
                        ph = ','.join(['%s'] * len(to_delete))
                        # get ids to cascade-delete relationships
                        cursor.execute(f"SELECT id FROM nodes WHERE name IN ({ph})", tuple(to_delete))
                        del_ids = [r[0] for r in cursor.fetchall()]
                        if del_ids:
                            ph_ids = ','.join(['%s'] * len(del_ids))
                            cursor.execute(f"DELETE FROM group_members WHERE node_id IN ({ph_ids})", tuple(del_ids))
                            cursor.execute(f"DELETE FROM connections WHERE node_id IN ({ph_ids}) OR target_node_id IN ({ph_ids})", tuple(del_ids + del_ids))
                            cursor.execute(f"DELETE FROM nodes WHERE id IN ({ph_ids})", tuple(del_ids))

                # Upsert nodes in bulk
                if incoming_nodes:
                    insert_cols = (
                        "name, ip1, ip2, shape, sheet_name, base_x, base_y, size, starttime, total_down_time, "
                        "last_active, color, alive, previous_alive, last_status_change, disabled, last_down_time, notes"
                    )
                    placeholders = ','.join(['%s'] * len(insert_cols.split(', ')))
                    update_expr = (
                        "ip1=VALUES(ip1), ip2=VALUES(ip2), shape=VALUES(shape), sheet_name=VALUES(sheet_name), "
                        "base_x=VALUES(base_x), base_y=VALUES(base_y), size=VALUES(size), starttime=VALUES(starttime), "
                        "total_down_time=VALUES(total_down_time), last_active=VALUES(last_active), color=VALUES(color), "
                        "alive=VALUES(alive), previous_alive=VALUES(previous_alive), last_status_change=VALUES(last_status_change), "
                        "disabled=VALUES(disabled), last_down_time=VALUES(last_down_time), notes=VALUES(notes)"
                    )
                    sql = f"INSERT INTO nodes ({insert_cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_expr}"
                    rows = []
                    for n in incoming_nodes:
                        rows.append((
                            n.get('name'),
                            n.get('ip1', ''),
                            n.get('ip2', ''),
                            n.get('shape', 'circle'),
                            n.get('sheet_name', 'Main'),
                            n.get('base_x', n.get('x', 100)),
                            n.get('base_y', n.get('y', 100)),
                            n.get('size', 30),
                            n.get('starttime', 0),
                            n.get('total_down_time', 0),
                            n.get('last_active', ''),
                            n.get('color', 'red'),
                            int(n.get('alive', 0)),
                            int(n.get('previous_alive', 0)),
                            n.get('last_status_change', 0),
                            int(n.get('disabled', 0)),
                            n.get('last_down_time', 0),
                            n.get('notes', ''),
                        ))
                    # executemany will perform many-row insert efficiently
                    cursor.executemany(sql, rows)

                # Refresh mapping name -> id for incoming nodes
                node_name_to_id = {}
                if incoming_nodes:
                    names = [n.get('name') for n in incoming_nodes if n.get('name')]
                    ph = ','.join(['%s'] * len(names))
                    cursor.execute(f"SELECT id, name FROM nodes WHERE name IN ({ph})", tuple(names))
                    for r in cursor.fetchall():
                        node_name_to_id[r[1]] = r[0]

                # Groups: deduplicate by (name, sheet_name) and upsert
                unique_groups = {}
                for g in incoming_groups:
                    name = g.get('name')
                    sheet = g.get('sheet_name', 'Main')
                    if name:
                        unique_groups[(name, sheet)] = g

                group_name_to_id = {}
                if unique_groups:
                    for (gname, sheet), g in unique_groups.items():
                        cursor.execute("INSERT IGNORE INTO node_groups (name, sheet_name) VALUES (%s, %s)", (gname, sheet))
                        cursor.execute("SELECT id FROM node_groups WHERE name=%s AND sheet_name=%s", (gname, sheet))
                        r = cursor.fetchone()
                        if r:
                            group_name_to_id[(gname, sheet)] = r[0]

                # Replace group memberships for incoming groups
                if group_name_to_id:
                    gids = list(group_name_to_id.values())
                    ph = ','.join(['%s'] * len(gids))
                    cursor.execute(f"DELETE FROM group_members WHERE group_id IN ({ph})", tuple(gids))
                    gm_rows = []
                    for g in incoming_groups:
                        gname = g.get('name')
                        sheet = g.get('sheet_name', 'Main')
                        if not gname:
                            continue
                        gid = group_name_to_id.get((gname, sheet))
                        if not gid:
                            # try matching by composite key if needed
                            gid = group_name_to_id.get((gname, sheet))
                        for member in g.get('nodes', []):
                            nid = node_name_to_id.get(member)
                            if nid:
                                gm_rows.append((gid, nid))
                    if gm_rows:
                        cursor.executemany("INSERT INTO group_members (group_id, node_id) VALUES (%s, %s)", gm_rows)

                # Connections: for each source node in incoming, delete existing connections and reinsert
                if incoming_nodes and node_name_to_id:
                    src_ids = [node_name_to_id[n.get('name')] for n in incoming_nodes if n.get('name') in node_name_to_id]
                    if src_ids:
                        ph = ','.join(['%s'] * len(src_ids))
                        cursor.execute(f"DELETE FROM connections WHERE node_id IN ({ph})", tuple(src_ids))
                        conn_rows = []
                        for n in incoming_nodes:
                            src = n.get('name')
                            sid = node_name_to_id.get(src)
                            if not sid:
                                continue
                            for tgt in n.get('connections', []):
                                tid = node_name_to_id.get(tgt)
                                if tid:
                                    conn_rows.append((sid, tid))
                        if conn_rows:
                            cursor.executemany("INSERT INTO connections (node_id, target_node_id) VALUES (%s, %s)", conn_rows)

                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
                cursor.execute("SET SQL_SAFE_UPDATES = 1")
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                print(f"Error saving network delta to MySQL: {e}")
                return False
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            print(f"Error connecting to MySQL: {e}")
            if attempt == 0 and "Already closed" in str(e):
                continue
            return False
    return False
