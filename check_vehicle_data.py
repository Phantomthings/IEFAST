import pandas as pd
from db import query_df, table_exists

print("=" * 60)
print("DIAGNOSTIC DES DONNÉES DE VÉHICULES")
print("=" * 60)

print("\n1. Vérification de l'existence de la table kpi_charges_mac")
if table_exists("kpi_charges_mac"):
    print("   ✅ La table kpi_charges_mac existe")

    # 2. Vérifier les colonnes disponibles
    print("\n2. Colonnes disponibles dans kpi_charges_mac")
    df_sample = query_df("SELECT * FROM kpi_charges_mac LIMIT 1")
    print(f"   Colonnes : {', '.join(df_sample.columns.tolist())}")

    # 3. Vérifier si la colonne Vehicle existe
    if "Vehicle" in df_sample.columns:
        print("   ✅ La colonne 'Vehicle' existe")

        # 4. Statistiques sur les données Vehicle
        print("\n3. Statistiques sur les données Vehicle")
        df_vehicles = query_df("""
            SELECT
                Vehicle,
                COUNT(*) as count
            FROM kpi_charges_mac
            GROUP BY Vehicle
            ORDER BY count DESC
            LIMIT 20
        """)

        print(f"   Total de lignes : {df_vehicles['count'].sum()}")
        print(f"   Nombre de types de véhicules distincts : {len(df_vehicles)}")
        print("\n   Répartition des véhicules (Top 20) :")
        for _, row in df_vehicles.iterrows():
            vehicle = str(row['Vehicle']).strip() if pd.notna(row['Vehicle']) else 'NULL'
            if vehicle in ['', 'nan', 'none', 'None', 'NULL']:
                vehicle = '❌ NULL/Empty'
            print(f"      {vehicle}: {row['count']}")

        # 5. Compter les valeurs valides vs invalides
        print("\n4. Analyse des valeurs valides")
        df_valid = query_df("""
            SELECT
                SUM(CASE
                    WHEN Vehicle IS NULL
                        OR TRIM(Vehicle) = ''
                        OR TRIM(Vehicle) IN ('nan', 'none', 'None', 'NULL')
                    THEN 1
                    ELSE 0
                END) as invalid_count,
                SUM(CASE
                    WHEN Vehicle IS NOT NULL
                        AND TRIM(Vehicle) != ''
                        AND TRIM(Vehicle) NOT IN ('nan', 'none', 'None', 'NULL')
                    THEN 1
                    ELSE 0
                END) as valid_count,
                COUNT(*) as total
            FROM kpi_charges_mac
        """)

        total = df_valid['total'].iloc[0]
        invalid = df_valid['invalid_count'].iloc[0]
        valid = df_valid['valid_count'].iloc[0]

        print(f"   Total de lignes : {total}")
        print(f"   Valeurs invalides (NULL/Empty) : {invalid} ({invalid/total*100:.1f}%)")
        print(f"   Valeurs valides : {valid} ({valid/total*100:.1f}%)")

        # 6. Exemple de données valides
        if valid > 0:
            print("\n5. Exemples de données valides")
            df_examples = query_df("""
                SELECT Vehicle, COUNT(*) as count
                FROM kpi_charges_mac
                WHERE Vehicle IS NOT NULL
                    AND TRIM(Vehicle) != ''
                    AND TRIM(Vehicle) NOT IN ('nan', 'none', 'None', 'NULL')
                GROUP BY Vehicle
                ORDER BY count DESC
                LIMIT 10
            """)
            for _, row in df_examples.iterrows():
                print(f"      {row['Vehicle']}: {row['count']}")

    else:
        print("   ❌ La colonne 'Vehicle' n'existe PAS dans kpi_charges_mac")

else:
    print("   ❌ La table kpi_charges_mac n'existe PAS")

print("\n" + "=" * 60)
print("FIN DU DIAGNOSTIC")
print("=" * 60)
