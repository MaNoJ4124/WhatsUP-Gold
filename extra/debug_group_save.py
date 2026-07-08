#!/usr/bin/env python3
"""
Debug test to see what's happening with group inserts.
"""

import pymysql
from mysql_driver import MYSQL_CONFIG, save_network_to_mysql

def debug_group_save():
    """Debug what's in the database"""
    
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("=" * 70)
        print("DEBUG - GROUP SAVE")
        print("=" * 70)
        
        # Clear all tables
        cursor.execute("DELETE FROM connections")
        cursor.execute("DELETE FROM group_members")
        cursor.execute("DELETE FROM nodes")
        cursor.execute("DELETE FROM node_groups")
        cursor.execute("DELETE FROM sheets")
        conn.commit()
        print("\n1. Cleared all tables")
        
        # Step 1: Save initial data
        initial_data = {
            "sheets": ["ALL SCADA"],
            "current_sheet": "ALL SCADA",
            "nodes": [],
            "groups": [
                {
                    "name": "ANAY",
                    "sheet_name": "ALL SCADA",
                    "nodes": []
                }
            ],
            "sheet_zoom": {},
            "_group_renames": {}
        }
        
        print("\n2. Saving initial data...")
        result = save_network_to_mysql(initial_data)
        print(f"   Result: {result}")
        
        cursor.execute("SELECT id, name, sheet_name FROM node_groups")
        rows = cursor.fetchall()
        print(f"   Groups in DB after save 1:")
        initial_id = None
        for row in rows:
            group_id = row[0] if isinstance(row, tuple) else row['id']
            name = row[1] if isinstance(row, tuple) else row['name']
            sheet = row[2] if isinstance(row, tuple) else row['sheet_name']
            print(f"      ID={group_id}, Name='{name}', Sheet='{sheet}'")
            if name == "ANAY":
                initial_id = group_id
        
        # Step 2: Rename with tracking
        renamed_data = {
            "sheets": ["ALL SCADA"],
            "current_sheet": "ALL SCADA",
            "nodes": [],
            "groups": [
                {
                    "name": "ANAY_URJA",
                    "sheet_name": "ALL SCADA",
                    "nodes": []
                }
            ],
            "sheet_zoom": {},
            "_group_renames": {
                "ANAY": "ANAY_URJA"
            }
        }
        
        print("\n3. Saving renamed data with tracking...")
        print(f"   _group_renames = {renamed_data['_group_renames']}")
        result = save_network_to_mysql(renamed_data)
        print(f"   Result: {result}")
        
        cursor.execute("SELECT id, name, sheet_name FROM node_groups")
        rows = cursor.fetchall()
        print(f"   Groups in DB after rename:")
        for row in rows:
            group_id = row[0] if isinstance(row, tuple) else row['id']
            name = row[1] if isinstance(row, tuple) else row['name']
            sheet = row[2] if isinstance(row, tuple) else row['sheet_name']
            print(f"      ID={group_id}, Name='{name}', Sheet='{sheet}'")
            
        print(f"\n4. Analysis:")
        print(f"   Initial ID was: {initial_id}")
        print(f"   Did ID preserve? {initial_id if initial_id else 'NO'}")
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    debug_group_save()
