# scripts/extract.py
import sys
import pandas as pd
import os

WATERMARK_FILE = 'data/last_order_id.txt'

def read_watermark():
    """Retourne le dernier OrderID traité (0 si premier run)."""
    if not os.path.exists(WATERMARK_FILE):
        return 0
    with open(WATERMARK_FILE, 'r') as f:
        return int(f.read().strip())

def write_watermark(last_order_id):
    """Sauvegarde le dernier OrderID traité."""
    with open(WATERMARK_FILE, 'w') as f:
        f.write(str(last_order_id))

def extract(filepath='data/Amazon-test.csv'):
    """
    Lit uniquement les nouvelles lignes depuis le dernier run.
    Utilise le watermark (dernier OrderID traité) comme point de reprise.

    En test  → filepath='data/Amazon_test.csv'
    En prod  → filepath='data/Amazon_enrichi.csv'  (défaut)
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df = pd.read_csv(filepath)

    # Extraire la partie numérique des OrderID pour comparaison
    df['_order_num'] = df['OrderID'].str.replace('ORD', '', regex=False).str.strip().astype(int)

    # Lire le watermark
    last_id = read_watermark()

    # ✅ Ne garder que les nouvelles lignes
    df_new = df[df['_order_num'] > last_id].drop(columns=['_order_num'])

    if df_new.empty:
        print(f"ℹ️  Aucune nouvelle ligne depuis ORD{last_id:07d} — pipeline ignorée.")
        return df_new

    # ✅ Mettre à jour le watermark avec le dernier OrderID traité
    new_last_id = int(
        df_new['OrderID'].str.replace('ORD', '', regex=False).str.strip().astype(int).max()
    )
    write_watermark(new_last_id)

    print(f"✅ Extract : {len(df_new)} nouvelles lignes")
    print(f"   Watermark : ORD{last_id:07d} → ORD{new_last_id:07d}")
    return df_new


if __name__ == "__main__":
    df = extract()
    print(df.head())