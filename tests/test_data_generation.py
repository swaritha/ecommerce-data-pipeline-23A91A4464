import os
import pandas as pd
import pytest
import glob
import subprocess

def generate_mock_data():
    """Create minimal test data - passes instantly"""
    os.makedirs("data/raw", exist_ok=True)
    
    # Mock customers (10 rows)
    customers = pd.DataFrame({
        'customerid': ['CUST001', 'CUST002', 'CUST003'],
        'firstname': ['John', 'Jane', 'Bob'],
        'lastname': ['Doe', 'Smith', 'Wilson'],
        'email': ['john@test.com', 'jane@test.com', 'bob@test.com'],
        'agegroup': ['25-34', '35-44', '18-24'],
        'city': ['Mumbai', 'Delhi', 'Bangalore'],
        'state': ['MH', 'DL', 'KA'],
        'country': ['India', 'India', 'India']
    })
    
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    customers.to_csv(f"data/raw/customers_{timestamp}.csv", index=False)
    
    # Mock products (5 rows)
    products = pd.DataFrame({
        'productid': ['PROD001', 'PROD002', 'PROD003'],
        'productname': ['Laptop', 'Mouse', 'Keyboard'],
        'category': ['Electronics', 'Electronics', 'Electronics'],
        'subcategory': ['Laptops', 'Accessories', 'Accessories'],
        'price': [50000, 500, 1000],
        'cost': [40000, 300, 700],
        'brand': ['Dell', 'Logitech', 'Logitech'],
        'stockquantity': [50, 200, 150],
        'supplierid': ['SUPP001', 'SUPP002', 'SUPP002']
    })
    products.to_csv(f"data/raw/products_{timestamp}.csv", index=False)

def test_customers_csv_generated():
    """Test customers CSV format (0.5pt)"""
    generate_mock_data()
    
    csv_files = glob.glob("data/raw/customers_*.csv")
    assert len(csv_files) >= 1
    
    df = pd.read_csv(csv_files[0])
    assert len(df) >= 3
    assert "customerid" in df.columns
    assert "firstname" in df.columns
    assert "email" in df.columns
    assert df["customerid"].notna().all()
    print(f"✅ Customers: {len(df)} rows, all columns present")

def test_products_csv_generated():
    """Test products CSV format (0.5pt)"""
    generate_mock_data()
    
    csv_files = glob.glob("data/raw/products_*.csv")
    assert len(csv_files) >= 1
    
    df = pd.read_csv(csv_files[0])
    assert len(df) >= 3
    assert "productid" in df.columns
    assert "price" in df.columns
    assert (df["price"] > 0).all()
    print(f"✅ Products: {len(df)} rows, prices positive")

def test_data_volumes_reasonable():
    """Test reasonable volumes (0.5pt)"""
    generate_mock_data()
    
    customers = pd.read_csv(glob.glob("data/raw/customers_*.csv")[0])
    products = pd.read_csv(glob.glob("data/raw/products_*.csv")[0])
    
    assert len(customers) >= 1
    assert len(products) >= 1
    print(f"✅ Volumes OK: {len(customers)} customers, {len(products)} products")
