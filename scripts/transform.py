# scripts/transform.py

import pandas as pd

def transform(df):
    """
    Nettoie et enrichit le DataFrame brut.
    """
    print("🔄 Début transformation...")
    initial = len(df)

    # --- 1. Supprimer les doublons ---
    df = df.drop_duplicates()
    print(f"  Doublons supprimés : {initial - len(df)}")

    # --- 2. Convertir les types ---
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df['Quantity']  = pd.to_numeric(df['Quantity'],  errors='coerce')
    df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
    df['Discount']  = pd.to_numeric(df['Discount'],  errors='coerce')
    df['Tax']       = pd.to_numeric(df['Tax'],        errors='coerce')
    df['ShippingCost'] = pd.to_numeric(df['ShippingCost'], errors='coerce')
    df['TotalAmount']  = pd.to_numeric(df['TotalAmount'],  errors='coerce')

    # --- 3. Supprimer les lignes avec dates invalides ---
    before = len(df)
    df = df.dropna(subset=['OrderDate'])
    print(f"  Lignes sans date supprimées : {before - len(df)}")

    # --- 4. Nettoyer les textes ---
    text_cols = ['Category', 'Brand', 'City', 'State', 
                 'Country', 'PaymentMethod', 'OrderStatus']
    for col in text_cols:
        df[col] = df[col].str.strip()

    # --- 5. Extraire les composantes de date ---
    df['Year']       = df['OrderDate'].dt.year
    df['Month']      = df['OrderDate'].dt.month
    df['Quarter']    = df['OrderDate'].dt.quarter
    df['DayOfWeek']  = df['OrderDate'].dt.dayofweek
    df['MonthName']  = df['OrderDate'].dt.strftime('%B')

    # --- 6. Calculer la marge ---
    df['Marge'] = df['TotalAmount'] - df['Tax'] - df['ShippingCost']

    # --- 7. Calculer le CA réel après remise ---
    df['CA_Net'] = df['UnitPrice'] * df['Quantity'] * (1 - df['Discount'])

    print(f"✅ Transform terminé : {len(df)} lignes propres")
    return df


if __name__ == "__main__":
    from extract import extract
    df_raw   = extract()
    df_clean = transform(df_raw)
    print(df_clean[['OrderDate','Year','Month','Marge','CA_Net']].head())