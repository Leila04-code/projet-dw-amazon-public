import pandas as pd
import random
import os
from datetime import datetime

LOG_FILE = 'simulation_log.txt'


def lire_log():
    """Retourne la liste des runs déjà effectués."""
    if not os.path.exists(LOG_FILE):
        return []
    with open(LOG_FILE, 'r') as f:
        return [line.strip() for line in f.readlines()]


def ecrire_log(today_str, nb_ventes):
    """Enregistre chaque run avec timestamp et nb de ventes."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a') as f:
        f.write(f"{timestamp} | date_simulee={today_str} | nb_ventes={nb_ventes}\n")


def simulate_daily_sales(
    nb_ventes=None,
    date_cible=None,
    force=False,
    filepath='data/Amazon_enrichi.csv'  # ✅ FIX 3 : paramètre filepath
):
    """
    Simule des ventes journalières et les ajoute au fichier CSV cible.

    Params:
        nb_ventes  : nombre de ventes à générer (défaut : aléatoire 50-150)
        date_cible : date des ventes 'YYYY-MM-DD' (défaut : aujourd'hui)
        force      : True = bypasse le check doublon (tests locaux uniquement)
        filepath   : fichier CSV cible
    """

    # ─────────────────────────────────────
    # Charger le fichier CSV cible
    # ─────────────────────────────────────
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Fichier introuvable : {filepath}")

    df_base = pd.read_csv(filepath)
    df_base['OrderDate'] = pd.to_datetime(df_base['OrderDate'])
    print(f"✅ {filepath} chargé : {len(df_base):,} lignes")

    # ─────────────────────────────────────
    # Extraire prénoms / noms réels
    # ─────────────────────────────────────
    noms_complets = df_base['CustomerName'].dropna().unique().tolist()
    prenoms = list(set([n.split()[0] for n in noms_complets if len(n.split()) >= 1]))
    noms    = list(set([n.split()[1] for n in noms_complets if len(n.split()) >= 2]))

    # ─────────────────────────────────────
    # Statistiques produits
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
    paiements       = df_base.groupby('PaymentMethod').size().reset_index(name='frequence')
    paiements_liste = paiements['PaymentMethod'].tolist()
    paiements_poids = paiements['frequence'].tolist()

    # ─────────────────────────────────────
    # Dernier OrderID et CustomerID
    # ─────────────────────────────────────
    last_order = int(
        df_base['OrderID'].str.replace('ORD', '', regex=False).str.strip().astype(int).max()
    )
    last_customer = int(
        df_base['CustomerID'].str.replace('CUST', '', regex=False).str.strip().astype(int).max()
    )
    print(f"✅ Dernier OrderID    : ORD{last_order:07d}")
    print(f"✅ Dernier CustomerID : CUST{last_customer:06d}")

    # ─────────────────────────────────────
    # Date cible
    # ─────────────────────────────────────
    today = datetime.strptime(date_cible, '%Y-%m-%d').date() \
            if isinstance(date_cible, str) \
            else (date_cible or datetime.today().date())
    today_str = today.strftime('%Y-%m-%d')

    # ─────────────────────────────────────
    # ✅ FIX 1 : Check doublon SANS input()
    # Airflow n'a pas de terminal interactif
    # ─────────────────────────────────────
    historique = lire_log()
    runs_today = [l for l in historique if f"date_simulee={today_str}" in l]

    if runs_today and not force:
        print(f"⚠️  Déjà simulé {len(runs_today)}x pour le {today_str} — simulation ignorée.")
        return None  # ✅ Stop propre, pas de crash, pas de doublon

    elif runs_today and force:
        print(f"⚡ Mode force=True — {len(runs_today)} run(s) déjà effectué(s), on continue.")

    # ─────────────────────────────────────
    # Génération des ventes
    # ─────────────────────────────────────
    nb = nb_ventes or random.randint(50, 150)
    ventes = []
    current_customer = last_customer

    for i in range(nb):

        produit  = produits.sample(1).iloc[0]
        prix     = round(random.uniform(produit['prix_min'], produit['prix_max']), 2)
        qty      = random.randint(int(produit['qty_min']), int(produit['qty_max']))
        remise   = round(random.uniform(produit['remise_min'], produit['remise_max']), 2)
        tax      = round(prix * qty * 0.08, 2)
        shipping = round(random.uniform(0, 15), 2)
        total    = round(prix * qty * (1 - remise) + tax + shipping, 2)
        ville    = villes.sample(1).iloc[0]
        paiement = random.choices(paiements_liste, weights=paiements_poids)[0]

        new_order         = last_order + i + 1
        current_customer += random.randint(0, 3)

        ventes.append({
            'OrderID'      : f"ORD{new_order:07d}",
            'OrderDate'    : today_str,
            'CustomerID'   : f"CUST{current_customer:06d}",
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
            'OrderStatus'  : random.choice(['Delivered', 'Shipped', 'Processing']),
            'City'         : ville['City'],
            'State'        : ville['State'],
            'Country'      : ville['Country'],
            'SellerID'     : f"SELL{random.randint(1000, 99999):05d}",
        })

    df_new = pd.DataFrame(ventes)

    # ─────────────────────────────────────
    # Écriture dans le fichier CSV cible
    # ─────────────────────────────────────
    df_new.to_csv(filepath, mode='a', header=False, index=False)
    ecrire_log(today_str, nb)

    print(f"\n✅ {nb} ventes ajoutées dans {filepath}")
    print(f"📅 Date    : {today_str}")
    print(f"🔢 OrderID : ORD{last_order+1:07d} → ORD{last_order+nb:07d}")
    print(f"\n📊 Aperçu :")
    print(df_new[['OrderID', 'CustomerID', 'CustomerName',
                  'ProductName', 'UnitPrice', 'TotalAmount']].head(5).to_string())

    return df_new


# ✅ FIX 2 : ligne d'exécution commentée
# Airflow importe ce fichier → ne doit rien exécuter à l'import
# simulate_daily_sales()