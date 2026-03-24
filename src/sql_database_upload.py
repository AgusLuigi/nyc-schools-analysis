# SQL PUSH DATA UPLOAD

def sql_run_db_pipeline(df, link_path="src/SQL_LINK_.txt"):
    """
    Führt die Datenbank-Infrastruktur-Vorbereitung und den Daten-Upload aus.
    Inklusive dynamischer Ermittlung der benötigten Spaltenbreiten.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    import os
    
    # --- CONFIGURATION ---
    UPLOAD_DATA   = df
    DATABASE      = 'nyc_schools'
    MASTER_TABLE  = 'schools_directory'
    CONNECTION_ID = 'dbn'
    TARGET_TABLE  = 'agus_sat_results'

    # --- DYNAMISCHE ANALYSE (Automatisierung der Spaltengrößen) ---
    # Wir messen die max. Länge im DF und addieren einen Puffer von 5 Zeichen
    # Das verhindert den 'StringDataRightTruncation' Fehler bei DBN (13 Zeichen)
    def get_max_len(col_name, default=20):
        if col_name in df.columns:
            actual_max = df[col_name].astype(str).replace('nan', '').str.len().max()
            return int(max(actual_max, default) + 5)
        return default

    dynamic_dbn_limit = get_max_len(CONNECTION_ID)
    dynamic_sid_limit = get_max_len('internal_school_id')

    # 1. PFAD-LOGIK
    if not os.path.exists(link_path):
        alternative_path = os.path.join("..", "..", link_path)
        if os.path.exists(alternative_path):
            link_path = alternative_path
        else:
            print(f"❌ Fehler: '{link_path}' nicht gefunden.")
            return False

    try:
        # 2. Verbindung herstellen
        with open(link_path, "r") as f:
            db_link = f.read().strip()
        
        engine = create_engine(db_link)

        # 3. SQL-Setup mit dynamischen Werten
        # Wir nutzen DROP TABLE, um die alten, zu kleinen Spaltenlimits (10) zu überschreiben
        sql_setup = f"""
        CREATE SCHEMA IF NOT EXISTS {DATABASE};

        DROP TABLE IF EXISTS {DATABASE}.{TARGET_TABLE};

        CREATE TABLE {DATABASE}.{TARGET_TABLE} (
            {CONNECTION_ID} VARCHAR({dynamic_dbn_limit}) PRIMARY KEY,
            school_name TEXT,
            num_of_sat_test_takers INTEGER,
            sat_critical_reading_avg_score INTEGER,
            sat_math_avg_score INTEGER,
            sat_writing_avg_score INTEGER,
            pct_students_tested NUMERIC(5,2),
            academic_tier_rating INTEGER,
            internal_school_id VARCHAR({dynamic_sid_limit}),
            contact_extension TEXT
        );
        """

        # 4. Infrastruktur-Befehle ausführen
        with engine.connect() as conn:
            conn.execute(text(sql_setup))
            conn.commit()
            print(f"✅ Infrastruktur bereit: {TARGET_TABLE} nutzt jetzt VARCHAR({dynamic_dbn_limit}) für {CONNECTION_ID}.")

        # 5. Daten-Upload
        # Vorher Leerzeichen strippen für saubere IDs
        df[CONNECTION_ID] = df[CONNECTION_ID].astype(str).str.strip()
        
        UPLOAD_DATA.to_sql(
            name=TARGET_TABLE, 
            con=engine, 
            schema=DATABASE, 
            if_exists='append', 
            index=False
        )
        
        # 6. CSV Export
        os.makedirs("day_4_task", exist_ok=True)
        UPLOAD_DATA.to_csv("day_4_task/cleaned_sat_results.csv", index=False)
        
        print(f"🚀 Erfolg: {len(UPLOAD_DATA)} Zeilen übertragen (Max-Länge DBN: {dynamic_dbn_limit-5}).")
        return

    except Exception as e:
        print(f"❌ Fehler bei der Datenbank-Pipeline: {e}")
        return False

# Aufruf
#sql_run_db_pipeline(df)