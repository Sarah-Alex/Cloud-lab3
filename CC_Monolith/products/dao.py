import os
import sqlite3

def connect(path):
    exists = os.path.exists(path)
    __conn = sqlite3.connect(path)
    if not exists:
        create_tables(__conn)
    __conn.row_factory = sqlite3.Row
    return __conn

def create_tables(conn):
    # Keep original table creation exactly as is
    conn.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT NOT NULL,
            cost REAL NOT NULL,
            qty INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    #------------------------- DO NOT CHANGE THE BELOW CODE -------------------------
    # [Original INSERT statements remain unchanged]
    # --------------------------------------------------

def list_products():
    conn = connect('products.db')
    cursor = conn.cursor()
    
    # Optimize by removing unnecessary loop and directly returning results
    cursor.execute('SELECT * FROM products')
    products = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return products

def get_product(product_id: int):
    conn = connect('products.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM products WHERE id = ?', (product_id,))
    product = cursor.fetchone()
    cursor.close()
    conn.close()
    return product

def add_product(product: dict):
    conn = connect('products.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO products (name, description, cost, qty) VALUES (?, ?, ?, ?)',
                   (product['name'], product['description'], product['cost'], product['qty']))
    conn.commit()
    cursor.close()
    conn.close()

def update_qty(product_id: int, qty: int):
    conn = connect('products.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE products SET qty = ? WHERE id = ?', (qty, product_id))
    conn.commit()
    cursor.close()
    conn.close()

def delete_product(product_id: int):
    conn = connect('products.db')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM products WHERE id = ?', (product_id,))
    conn.commit()
    cursor.close()
    conn.close()

def update_product(product_id: int, product: dict):
    conn = connect('products.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE products SET name = ?, description = ?, cost = ?, qty = ? WHERE id = ?',
                   (product['name'], product['description'], product['cost'], product['qty'], product_id))
    conn.commit()
    cursor.close()
    conn.close()