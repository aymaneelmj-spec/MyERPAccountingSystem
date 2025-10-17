#!/usr/bin/env python3
"""
Database Migration Script - Add missing last_login column
Run this script to fix the database schema issue
"""
import sqlite3
import os
from datetime import datetime

def migrate_database():
    """Add last_login column to users table"""
    
    # Database path
    db_path = 'instance/erp_database.db'
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("Please run the main app.py first to create the database")
        return False
    
    try:
        print("🔄 Starting database migration...")
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if column already exists
        cursor.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'last_login' in columns:
            print("✅ Column 'last_login' already exists. No migration needed.")
            conn.close()
            return True
        
        # Add the missing column
        print("📝 Adding 'last_login' column to users table...")
        cursor.execute("ALTER TABLE users ADD COLUMN last_login DATETIME")
        
        conn.commit()
        conn.close()
        
        print("✅ Migration completed successfully!")
        print("✅ Database schema updated.")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

if __name__ == '__main__':
    print("="*60)
    print("Happy Deal Transit ERP - Database Migration")
    print("="*60)
    
    success = migrate_database()
    
    if success:
        print("\n✅ You can now restart the Flask application")
        print("Run: python app.py")
    else:
        print("\n❌ Migration failed. Please check the errors above.")
    
    print("="*60)