# scripts/extract.py
import sys
import pandas as pd
import os

def extract(filepath='data/Amazon.csv'):
    """
    Lit le fichier CSV source et retourne un DataFrame brut.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df = pd.read_csv(filepath)
    print(f"✅ Extract : {len(df)} lignes, {len(df.columns)} colonnes")
    return df


if __name__ == "__main__":
    df = extract()
    print(df.head())