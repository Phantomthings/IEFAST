"""
Script de diagnostic rapide pour comprendre le problème des véhicules
"""
import sys
sys.path.insert(0, '/home/user/ie-charges')

try:
    from db import query_df, table_exists

    print("=" * 70)
    print("DIAGNOSTIC DES DONNÉES VEHICLE")
    print("=" * 70)

    # 1. Vérifier kpi_charges_mac
    print("\n1. Analyse de kpi_charges_mac")
    print("-" * 70)

    if table_exists("kpi_charges_mac"):
        print("   ✅ La table kpi_charges_mac existe")

        # Vérifier si la colonne Vehicle existe
        sample = query_df("SELECT * FROM kpi_charges_mac LIMIT 1")

        if "Vehicle" in sample.columns:
            print("   ✅ La colonne Vehicle existe")

            result2 = query_df("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN Vehicle IS NULL THEN 1 ELSE 0 END) as null_count,
                    SUM(CASE WHEN Vehicle = 'Unknown' THEN 1 ELSE 0 END) as unknown_count,
                    SUM(CASE
                        WHEN Vehicle IS NOT NULL
                        AND Vehicle != 'Unknown'
                        AND TRIM(Vehicle) != ''
                        THEN 1 ELSE 0
                    END) as valid_count
                FROM kpi_charges_mac
                LIMIT 1
            """)

            if not result2.empty:
                row2 = result2.iloc[0]
                print(f"   Total de lignes: {row2['total']}")
                print(f"   NULL: {row2['null_count']}")
                print(f"   'Unknown': {row2['unknown_count']}")
                print(f"   Valeurs valides: {row2['valid_count']}")

            # Exemples de véhicules valides
            if result2.iloc[0]['valid_count'] > 0:
                print("\n   Exemples de véhicules valides:")
                examples = query_df("""
                    SELECT Vehicle, COUNT(*) as count
                    FROM kpi_charges_mac
                    WHERE Vehicle IS NOT NULL
                        AND Vehicle != 'Unknown'
                        AND TRIM(Vehicle) != ''
                    GROUP BY Vehicle
                    ORDER BY count DESC
                    LIMIT 5
                """)
                for _, ex in examples.iterrows():
                    print(f"      - {ex['Vehicle']}: {ex['count']} occurrences")
        else:
            print("   ❌ La colonne Vehicle n'existe PAS dans kpi_charges_mac")
    else:
        print("   ❌ La table kpi_charges_mac n'existe PAS")

    # 2. Tester le JOIN actuel
    print("\n2. Test du JOIN entre kpi_sessions et kpi_charges_mac")
    print("-" * 70)

    if table_exists("kpi_charges_mac"):
        test_join = query_df("""
            SELECT
                COUNT(*) as total_sessions,
                SUM(CASE WHEN c.Vehicle IS NOT NULL THEN 1 ELSE 0 END) as matched,
                SUM(CASE
                    WHEN c.Vehicle IS NOT NULL
                    AND c.Vehicle != 'Unknown'
                    AND TRIM(c.Vehicle) != ''
                    THEN 1 ELSE 0
                END) as valid_matched
            FROM kpi_sessions k
            LEFT JOIN kpi_charges_mac c
                ON TRIM(UPPER(k.`MAC Address`)) = TRIM(UPPER(c.`MAC Address`))
                AND ABS(TIMESTAMPDIFF(SECOND, k.`Datetime start`, c.`Datetime start`)) <= 5
            LIMIT 1
        """)

        if not test_join.empty:
            tj = test_join.iloc[0]
            print(f"   Total sessions: {tj['total_sessions']}")
            print(f"   Sessions matchées (avec Vehicle non-NULL): {tj['matched']}")
            print(f"   Sessions avec Vehicle valide: {tj['valid_matched']}")

            if tj['total_sessions'] > 0:
                match_rate = (tj['valid_matched'] / tj['total_sessions']) * 100
                print(f"   Taux de correspondance: {match_rate:.2f}%")

    print("\n" + "=" * 70)
    print("FIN DU DIAGNOSTIC")
    print("=" * 70)

except Exception as e:
    print(f"Erreur: {e}")
    import traceback
    traceback.print_exc()
