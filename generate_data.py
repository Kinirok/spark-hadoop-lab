#!/usr/bin/env python3
import csv
import random
import datetime
from pathlib import Path

def generate_dataset(rows=150000, output_file="data/sales_dataset.csv"):
    Path("data").mkdir(exist_ok=True)
    
    products = [
        ("Laptop", "Electronics", 800, 2500),
        ("Phone", "Electronics", 300, 1200),
        ("Tablet", "Electronics", 200, 800),
        ("Monitor", "Electronics", 150, 600),
        ("Keyboard", "Accessories", 20, 150),
        ("Mouse", "Accessories", 10, 80),
        ("Headphones", "Accessories", 30, 200),
        ("Chair", "Furniture", 100, 400),
        ("Desk", "Furniture", 150, 500),
        ("Printer", "Electronics", 100, 300)
    ]
    
    countries = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Brazil", "India"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Cash", "Bank Transfer"]
    customer_segments = ["Young", "Adult", "Senior", "Premium", "Budget"]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "transaction_id", "date", "product", "category", "country", 
            "quantity", "unit_price", "total_price", "payment_method", 
            "customer_segment", "rating", "is_returned"
        ])
        
        for i in range(rows):
            product, category, min_price, max_price = random.choice(products)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(min_price, max_price), 2)
            total_price = round(quantity * unit_price, 2)
            
            date = datetime.date(2024, random.randint(1, 12), random.randint(1, 28))
            
            writer.writerow([
                i,
                date,
                product,
                category,
                random.choice(countries),
                quantity,
                unit_price,
                total_price,
                random.choice(payment_methods),
                random.choice(customer_segments),
                random.randint(1, 5),
                random.random() < 0.05  # 5% returns
            ])
    
    print(f"Dataset generated: {rows} rows")
    return output_file

if __name__ == "__main__":
    generate_dataset(500000)