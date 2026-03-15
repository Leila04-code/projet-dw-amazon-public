import pandas as pd
import numpy as np
import random
import os
from datetime import datetime

def explorer_donnees_existantes():

    df_base = pd.read_csv('Amazon.csv')

    # ─────────────────────────────────────
    # Extraire les vrais prénoms et noms
    # depuis le CSV existant
    # ─────────────────────────────────────
    noms_complets = df_base['CustomerName'].dropna().unique().tolist()

    # Séparer prénom et nom
    prenoms = list(set([
        n.split()[0] for n in noms_complets if len(n.split()) >= 1
    ]))
    noms    = list(set([
        n.split()[1] for n in noms_complets if len(n.split()) >= 2
    ]))

    print(f"✅ {len(prenoms)} prénoms uniques trouvés")
    print(f"✅ {len(noms)} noms de famille uniques trouvés")

    # Charger ventes simulées précédentes si existent
    if os.path.exists('nouvelles_ventes_test.csv'):
        df_new     = pd.read_csv('nouvelles_ventes_test.csv')
        df_total   = pd.concat([df_base, df_new], ignore_index=True)
        print(f"✅ Ventes simulées existantes : {len(df_new)} lignes")
    else:
        df_total   = df_base

    # Dernier OrderID et CustomerID
    last_order    = int(
        df_total['OrderID'].str.replace('ORD','').astype(int).max()
    )
    last_customer = int(
        df_total['CustomerID'].str.replace('CUST','').astype(int).max()
    )

    print(f"✅ Dernier OrderID    : ORD{last_order:07d}")
    print(f"✅ Dernier CustomerID : CUST{last_customer:06d}")

    # Statistiques produits
    produits_reels = df_base.groupby(
        ['ProductID','ProductName','Category','Brand']
    ).agg(
        prix_min   = ('UnitPrice', 'min'),
        prix_max   = ('UnitPrice', 'max'),
        qty_min    = ('Quantity',  'min'),
        qty_max    = ('Quantity',  'max'),
        remise_min = ('Discount',  'min'),
        remise_max = ('Discount',  'max'),
    ).reset_index()

    villes    = df_base[['City','State','Country']].drop_duplicates()
    paiements = df_base.groupby('PaymentMethod').size().reset_index(
                    name='frequence')

    return produits_reels, villes, paiements, \
           last_order, last_customer, prenoms, noms


def simulate_daily_sales():

    produits, villes, paiements, \
    last_order, last_customer, prenoms, noms = \
        explorer_donnees_existantes()

    paiements_liste = paiements['PaymentMethod'].tolist()
    paiements_poids = paiements['frequence'].tolist()

    today  = datetime.today().date()
    nb     = random.randint(50, 150)
    ventes = []

    for i in range(nb):

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

        new_order    = last_order + i + 1
        new_customer = last_customer + random.randint(1, 50)

        # ✅ Nom réaliste combiné depuis le CSV
        nom_complet = f"{random.choice(prenoms)} {random.choice(noms)}"

        ventes.append({
            'OrderID'      : f"ORD{new_order:07d}",
            'OrderDate'    : today,
            'CustomerID'   : f"CUST{new_customer:06d}",
            'CustomerName' : nom_complet,        # ✅ vrai format
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
                ['Delivered','Shipped','Processing']),
            'City'         : ville['City'],
            'State'        : ville['State'],
            'Country'      : ville['Country'],
            'SellerID'     : f"SELL{random.randint(1000,99999):05d}",
        })

    df_new = pd.DataFrame(ventes)

    if os.path.exists('nouvelles_ventes_test.csv'):
        df_new.to_csv('nouvelles_ventes_test.csv',
                      mode='a', header=False, index=False)
        print(f"\n✅ {len(df_new)} ventes AJOUTÉES au fichier existant")
    else:
        df_new.to_csv('nouvelles_ventes_test.csv', index=False)
        print(f"\n✅ {len(df_new)} ventes créées dans nouveau fichier")

    print(f"📅 Date    : {today}")
    print(f"🔢 OrderID : ORD{last_order+1:07d}"
          f" → ORD{last_order+nb:07d}")
    print(f"\n📊 Aperçu :")
    print(df_new[['OrderID','CustomerName',
                  'ProductName','UnitPrice',
                  'TotalAmount']].head(10).to_string())

    return df_new


# ── EXÉCUTION ──
simulate_daily_sales()

