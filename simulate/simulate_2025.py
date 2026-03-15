import pandas as pd
import numpy as np
import random
from datetime import date, timedelta

def generer_annee_2025():

    # ─────────────────────────────────────
    # Charger Amazon.csv
    # ─────────────────────────────────────
    df_base = pd.read_csv('Amazon.csv')
    print(f"✅ Amazon.csv chargé : {len(df_base)} lignes")

    # ─────────────────────────────────────
    # Vérifier si 2025 déjà présent
    # ─────────────────────────────────────
    df_base['OrderDate'] = pd.to_datetime(df_base['OrderDate'])
    if 2025 in df_base['OrderDate'].dt.year.unique():
        print("⚠️  2025 déjà présent dans Amazon.csv — script ignoré !")
        return

    # ─────────────────────────────────────
    # Extraire prénoms et noms réels
    # ─────────────────────────────────────
    noms_complets = df_base['CustomerName'].dropna().unique()
    prenoms = list(set([
        n.split()[0] for n in noms_complets if len(n.split()) >= 1
    ]))
    noms = list(set([
        n.split()[1] for n in noms_complets if len(n.split()) >= 2
    ]))
    print(f"✅ {len(prenoms)} prénoms | {len(noms)} noms trouvés")

    # ─────────────────────────────────────
    # Statistiques réelles depuis Amazon.csv
    # ─────────────────────────────────────
    produits = df_base.groupby(
        ['ProductID', 'ProductName', 'Category', 'Brand']
    ).agg(
        prix_min   = ('UnitPrice', 'min'),
        prix_max   = ('UnitPrice', 'max'),
        qty_min    = ('Quantity',  'min'),
        qty_max    = ('Quantity',  'max'),
        remise_min = ('Discount',  'min'),
        remise_max = ('Discount',  'max'),
    ).reset_index()

    villes          = df_base[['City', 'State', 'Country']].drop_duplicates()
    paiements       = df_base.groupby('PaymentMethod').size().reset_index(
                          name='frequence')
    paiements_liste = paiements['PaymentMethod'].tolist()
    paiements_poids = paiements['frequence'].tolist()

    # ─────────────────────────────────────
    # Dernier OrderID et CustomerID
    # ─────────────────────────────────────
    last_order = int(
        df_base['OrderID']
        .str.replace('ORD', '', regex=False)
        .str.strip()
        .astype(int)
        .max()
    )
    last_customer = int(
        df_base['CustomerID']
        .str.replace('CUST', '', regex=False)
        .str.strip()
        .astype(int)
        .max()
    )
    print(f"✅ Dernier OrderID    : ORD{last_order:07d}")
    print(f"✅ Dernier CustomerID : CUST{last_customer:06d}")

    # ─────────────────────────────────────
    # Toutes les dates 2025 aléatoires
    # ─────────────────────────────────────
    toutes_dates_2025 = [
        date(2025, 1, 1) + timedelta(days=i)
        for i in range(365)
    ]

    nb_ventes = len(df_base) // 5  # ~20 000 ventes
    print(f"\n🔄 Génération de {nb_ventes} ventes 2025...")

    ventes = []
    for i in range(nb_ventes):

        produit  = produits.sample(1).iloc[0]
        prix     = round(random.uniform(
                       produit['prix_min'],
                       produit['prix_max']), 2)
        qty      = random.randint(
                       int(produit['qty_min']),
                       int(produit['qty_max']))
        remise   = round(random.uniform(
                       produit['remise_min'],
                       produit['remise_max']), 2)
        tax      = round(prix * qty * 0.08, 2)
        shipping = round(random.uniform(0, 15), 2)
        total    = round(
                       prix * qty * (1 - remise) + tax + shipping, 2)
        ville    = villes.sample(1).iloc[0]
        paiement = random.choices(
                       paiements_liste,
                       weights=paiements_poids)[0]

        last_order    += 1
        last_customer += random.randint(1, 50)

        ventes.append({
            'OrderID'      : f"ORD{last_order:07d}",
            'OrderDate'    : random.choice(toutes_dates_2025).strftime('%Y-%m-%d'),
            'CustomerID'   : f"CUST{last_customer:06d}",
            'CustomerName' : f"{random.choice(prenoms)} {random.choice(noms)}",
            'ProductID'    : produit['ProductID'],
            'ProductName'  : produit['ProductName'],
            'Category'     : produit['Category'],
            'Brand'        : produit['Brand'],
            'Quantity'     : qty,
            'UnitPrice'    : prix,
            'Discount'     : remise,
            'Tax'          : tax,
            'ShippingCost' : shipping,
            'TotalAmount'  : total,
            'PaymentMethod': paiement,
            'OrderStatus'  : random.choice(
                ['Delivered', 'Shipped', 'Processing']),
            'City'         : ville['City'],
            'State'        : ville['State'],
            'Country'      : ville['Country'],
            'SellerID'     : f"SELL{random.randint(1000,99999):05d}",
        })

    df_2025 = pd.DataFrame(ventes)

    # ─────────────────────────────────────
    # ✅ Ajouter à la fin de Amazon.csv
    # ✅ Sans toucher aux données existantes
    # ✅ Sans réassigner les OrderID
    # ✅ Sans mélanger les lignes
    # ✅ Sans corrompre les dates
    # ─────────────────────────────────────
    df_2025.to_csv(
        'Amazon.csv',
        mode='a',       # ajoute à la fin
        header=False,   # pas de header
        index=False
    )

    print(f"\n✅ {len(df_2025)} ventes 2025 ajoutées dans Amazon.csv !")
    print(f"   Avant : {len(df_base):,} lignes")
    print(f"   Après : {len(df_base) + len(df_2025):,} lignes")
    print(f"   OrderID : {df_2025['OrderID'].iloc[0]}"
          f" → {df_2025['OrderID'].iloc[-1]}")

    # ─────────────────────────────────────
    # Vérification finale
    # ─────────────────────────────────────
    df_final = pd.read_csv('Amazon.csv')
    df_final['OrderDate'] = pd.to_datetime(df_final['OrderDate'])

    print(f"\n📊 Distribution par année :")
    print(df_final.groupby(
        df_final['OrderDate'].dt.year
    )['TotalAmount'].agg(['count', 'sum']).rename(
        columns={'count': 'Nb Ventes', 'sum': 'CA Total ($)'}
    ).round(2).to_string())

    print(f"\n📊 Aperçu des 5 nouvelles lignes 2025 :")
    print(df_2025[['OrderID', 'OrderDate', 'CustomerName',
                   'ProductName', 'UnitPrice',
                   'TotalAmount']].head(5).to_string())


# ── EXÉCUTION ──
generer_annee_2025()

