# scripts/verify_dw.py
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///data/amazon_dw.db')

tables = ['Dim_Date', 'Dim_Product', 'Dim_Customer',
          'Dim_Location', 'Dim_Seller', 'Fact_Commandes']

print("=== VERIFICATION DW ===\n")
for table in tables:
    n = pd.read_sql(f"SELECT COUNT(*) as n FROM {table}", engine)['n'][0]
    print(f"  {table:<20} : {n:>7} lignes")

print("\n=== TEST REQUETE CA PAR CATEGORIE ===")
q = """
SELECT p.category, 
       COUNT(*) as nb_commandes,
       ROUND(SUM(f.total_amount), 2) as CA_total
FROM Fact_Commandes f
JOIN Dim_Product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY CA_total DESC
"""
print(pd.read_sql(q, engine).to_string(index=False))