# cart/dao.py
import json
import os.path
import sqlite3

def connect(path):
    exists = os.path.exists(path)
    conn = sqlite3.connect(path)
    if not exists:
        create_tables(conn)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS carts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            contents TEXT NOT NULL,
            cost REAL DEFAULT 0
        )
    ''')
    conn.commit()

def get_cart(username: str) -> list:
    conn = connect('carts.db')
    cursor = conn.cursor()
    
    # Get cart with a single query
    cursor.execute('SELECT * FROM carts WHERE username = ?', (username,))
    cart = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return cart

def add_to_cart(username: str, product_id: int):
    conn = connect('carts.db')
    cursor = conn.cursor()
    
    # Get existing cart directly
    cursor.execute('SELECT contents FROM carts WHERE username = ?', (username,))
    result = cursor.fetchone()
    
    try:
        # Process contents directly without multiple conversions
        contents = json.loads(result['contents']) if result else []
        if product_id not in contents:  # Avoid duplicates
            contents.append(product_id)
            
        # Update in a single operation
        cursor.execute('INSERT OR REPLACE INTO carts (username, contents, cost) VALUES (?, ?, ?)',
                      (username, json.dumps(contents), 0))
        conn.commit()
    except (json.JSONDecodeError, TypeError):
        # Handle new cart creation
        contents = [product_id]
        cursor.execute('INSERT INTO carts (username, contents, cost) VALUES (?, ?, ?)',
                      (username, json.dumps(contents), 0))
        conn.commit()
    
    cursor.close()
    conn.close()

def remove_from_cart(username: str, product_id: int):
    conn = connect('carts.db')
    cursor = conn.cursor()
    
    # Get and update cart in single operations
    cursor.execute('SELECT contents FROM carts WHERE username = ?', (username,))
    result = cursor.fetchone()
    
    if result:
        try:
            contents = json.loads(result['contents'])
            if product_id in contents:
                contents.remove(product_id)
                cursor.execute('UPDATE carts SET contents = ? WHERE username = ?',
                           (json.dumps(contents), username))
                conn.commit()
        except json.JSONDecodeError:
            pass
    
    cursor.close()
    conn.close()

def delete_cart(username: str):
    conn = connect('carts.db')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM carts WHERE username = ?', (username,))
    conn.commit()
    cursor.close()
    conn.close()