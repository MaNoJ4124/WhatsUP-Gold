#!/usr/bin/env python3
"""
Test to verify group rename preserves database IDs using the new temp111.py tracking approach.
"""

import pymysql
from mysql_driver import MYSQL_CONFIG, save_network_to_mysql

def test_group_rename_with_tracking():
    """Test that group rename with temp111 tracking preserves ID"""
    
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("=" * 70)
        print("GROUP RENAME TEST - WITH TRACKING (temp111.py approach)")
        print("=" * 70)
        
        # Clear all tables
        cursor.execute("DELETE FROM connections")
        cursor.execute("DELETE FROM group_members")
        cursor.execute("DELETE FROM nodes")
        cursor.execute("DELETE FROM node_groups")
        cursor.execute("DELETE FROM sheets")
        conn.commit()
        print("\n1. Cleared all tables")
        
        # Step 1: Save initial data with ANAY group
        initial_data = {
            "sheets": ["ALL SCADA"],
            "current_sheet": "ALL SCADA",
            "nodes": [
                {
                    "name": "Node1",
                    "ip1": "192.168.1.1",
                    "ip2": "192.168.1.1",
                    "shape": "circle",
                    "sheet_name": "ALL SCADA",
                    "base_x": 100,
                    "base_y": 100,
                    "size": 35,
                    "starttime": 0,
                    "total_down_time": 0,
                    "last_active": "",
                    "color": "blue",
                    "alive": 1,
                    "previous_alive": 1,
                    "last_status_change": 0,
                    "disabled": False,
                    "last_down_time": 0,
                    "notes": "",
                    "connections": []
                }
            ],
            "groups": [
                {
                    "name": "ANAY",
                    "sheet_name": "ALL SCADA",
                    "nodes": ["Node1"]
                }
            ],
            "sheet_zoom": {},
            "_group_renames": {}  # No renames initially
        }
        
        print("\n2. Saving initial data with 'ANAY' group...")
        result = save_network_to_mysql(initial_data)
        print(f"   Save result: {result}")
        
        # Get the group ID
        cursor.execute("SELECT id, name, sheet_name FROM node_groups WHERE name='ANAY'")
        row = cursor.fetchone()
        if row:
            initial_id = row[0]
            print(f"   Initial ANAY group ID: {initial_id}")
        else:
            print("   ✗ FAILED: Could not find ANAY group!")
            return
        
        # Step 2: Rename ANAY to ANAY_URJA WITH tracking
        renamed_data = {
            "sheets": ["ALL SCADA"],
            "current_sheet": "ALL SCADA",
            "nodes": initial_data["nodes"],  # Same nodes
            "groups": [
                {
                    "name": "ANAY_URJA",  # <-- RENAMED!
                    "sheet_name": "ALL SCADA",
                    "nodes": ["Node1"]  # Same nodes as before
                }
            ],
            "sheet_zoom": {},
            "_group_renames": {
                "ANAY": "ANAY_URJA"  # <-- CRITICAL: Tell database about the rename!
            }
        }
        
        print("\n3. Saving renamed data with tracking:")
        print("   _group_renames = {'ANAY': 'ANAY_URJA'}")
        result = save_network_to_mysql(renamed_data)
        print(f"   Save result: {result}")
        
        # Get the new group ID
        cursor.execute("SELECT id, name, sheet_name FROM node_groups WHERE name='ANAY_URJA'")
        row = cursor.fetchone()
        if row:
            final_id = row[0]
            print(f"   Final ANAY_URJA group ID: {final_id}")
        else:
            print("   ✗ FAILED: Could not find ANAY_URJA group!")
            return
        
        # Check if old name still exists
        cursor.execute("SELECT id FROM node_groups WHERE name='ANAY'")
        old_exists = cursor.fetchone()
        
        # Verification
        print("\n4. VERIFICATION:")
        print(f"   Initial ID: {initial_id}")
        print(f"   Final ID:   {final_id}")
        
        if old_exists:
            print(f"   ✗ Old name 'ANAY' still in DB - This is wrong!")
        elif initial_id == final_id:
            print(f"   ✓ SUCCESS! ID {initial_id} PRESERVED during rename!")
            print(f"   ✓ Group was UPDATED in-place")
            print(f"   ✓ Old name removed, new name is '{row[1]}'")
        else:
            print(f"   ✗ FAILED: IDs changed! ({initial_id} → {final_id})")
            print(f"   This means a NEW group was created instead of updating")
            
    finally:
        # Cleanup
        cursor.execute("DELETE FROM connections")
        cursor.execute("DELETE FROM group_members")
        cursor.execute("DELETE FROM nodes")
        cursor.execute("DELETE FROM node_groups")
        cursor.execute("DELETE FROM sheets")
        conn.commit()
        cursor.close()
        conn.close()
        print("\n5. Test cleanup completed")
        print("=" * 70)

if __name__ == "__main__":
    test_group_rename_with_tracking()
