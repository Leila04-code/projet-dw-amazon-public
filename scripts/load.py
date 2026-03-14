# scripts/load.py
import pandas as pd
import pyodbc

def get_conn():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=amazon_dw;"
        "Trusted_Connection=yes;"
    )

def insert_df(cursor, df, table_name):
    """Insère un DataFrame ligne par ligne via pyodbc."""
    cols = ', '.join(df.columns)
    placeholders = ', '.join(['?' for _ in df.columns])
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(sql, data)

def load(df):
    conn   = get_conn()
    cursor = conn.cursor()
    print("🔄 Début chargement dans SQL Server...")

    try:
        # --- Dim_Date ---
        dim_date = df[['OrderDate','Year','Month','Quarter',
                       'DayOfWeek','MonthName']].drop_duplicates().copy()
        dim_date.columns = ['date_id','year','month','quarter',
                            'day_of_week','month_name']
        dim_date['date_id'] = dim_date['date_id'].astype(str)
        insert_df(cursor, dim_date, 'Dim_Date')
        conn.commit()
        print(f"  ✅ Dim_Date      : {len(dim_date)} lignes")

        # --- Dim_Product ---
        # --- Dim_Product ---
        dim_product = df[['ProductID','ProductName',
                  'Category','Brand']].copy()
        dim_product.columns = ['product_id','product_name','category','brand']

        # ⚠️ drop_duplicates AVANT tout le reste
        dim_product = dim_product.drop_duplicates(subset=['product_id'], keep='first')

         # Vérification
        print(f"  Nb ProductID uniques : {dim_product['product_id'].nunique()}")
        print(f"  Nb lignes total      : {len(dim_product)}")
        print(f"  Doublons restants    : {dim_product.duplicated(subset=['product_id']).sum()}")

        insert_df(cursor, dim_product, 'Dim_Product')
        conn.commit()
        print(f"  ✅ Dim_Product   : {len(dim_product)} lignes")
    

        # --- Dim_Customer ---
        dim_customer = df[['CustomerID','CustomerName','PaymentMethod']].copy()
        dim_customer.columns = ['customer_id','customer_name','payment_method']
        dim_customer = dim_customer.drop_duplicates(subset=['customer_id'], keep='first')
        insert_df(cursor, dim_customer, 'Dim_Customer')
        conn.commit()
        print(f"  ✅ Dim_Customer  : {len(dim_customer)} lignes")

        # --- Dim_Location ---
        dim_location = df[['City','State','Country']].copy()
        dim_location.columns = ['city','state','country']
        dim_location = dim_location.drop_duplicates(subset=['city','state','country'], keep='first')

        insert_df(cursor, dim_location, 'Dim_Location')
        conn.commit()
        print(f"  ✅ Dim_Location  : {len(dim_location)} lignes")

        # --- Dim_Seller ---
        dim_seller = df[['SellerID']].copy()
        dim_seller.columns = ['seller_id']
        dim_seller = dim_seller.drop_duplicates(subset=['seller_id'], keep='first')
        insert_df(cursor, dim_seller, 'Dim_Seller')
        conn.commit()
        print(f"  ✅ Dim_Seller    : {len(dim_seller)} lignes")

        # --- Récupérer location_id générés par SQL Server ---
        cursor.execute("SELECT location_id, city, state, country FROM Dim_Location")
        rows = cursor.fetchall()
        dim_loc_db = pd.DataFrame(
            [tuple(r) for r in rows],
            columns=['location_id','city','state','country']
        )

        # --- Fact_Commandes ---
        df_merged = df.merge(
            dim_loc_db,
            left_on  = ['City','State','Country'],
            right_on = ['city','state','country'],
            how='left'
        )

        fact = df_merged[[
            'OrderID','OrderDate','ProductID','CustomerID',
            'location_id','SellerID',
            'Quantity','UnitPrice','Discount','Tax',
            'ShippingCost','TotalAmount','Marge','CA_Net',
            'OrderStatus'
        ]].copy()

        fact.columns = [
            'order_id','date_id','product_id','customer_id',
            'location_id','seller_id',
            'quantity','unit_price','discount','tax',
            'shipping_cost','total_amount','marge','ca_net',
            'order_status'
        ]

        fact['date_id'] = fact['date_id'].astype(str)

        

        insert_df(cursor, fact, 'Fact_Commandes')
        conn.commit()
        print(f"  ✅ Fact_Commandes: {len(fact)} lignes")

        print("✅ Chargement SQL Server terminé !")

    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur : {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    from extract   import extract
    from transform import transform
    df_raw   = extract()
    df_clean = transform(df_raw)
    load(df_clean)