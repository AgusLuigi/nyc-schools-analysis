# --- SCHRITT 0 & 1: Namen normalisieren & Aufräumen ---
def clean_column_names(df):
    import pandas as pd
    import numpy as np
    """Normalisiert Spalten-Titel für SQL (klein, keine Punkte, Unterstriche)."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('.', '', regex=False)
    )
    print("✅ Neue Spalten-Titel für SQL:", df.columns.tolist())
    return df

# --- SCHRITT 2: Stochastische Typ-Synchronisation & NaN-Feilen ---
def clean_type_stochastic_flote_int(df_input):
    import pandas as pd
    import numpy as np
    """
    Stochastic Type Synchronization with Keyword-Boost:
    - IDENTITY PROTECTION: DBN, IDs, names, and coordinates remain 'object' (String).
    - MEASUREMENT BOOST: If 'avg', 'max', 'min', 'score', or currency is in the title, 
      the column is prioritized for numeric conversion, overriding 'object' status.
    - SURGICAL CLEANING: 's' or symbols are converted to NaN for SQL compatibility.
    """
    # --- UNIVERSELLER IMPORT CHECK ---
    # Wir versuchen die Analyse-Funktion dynamisch zu greifen, 
    # falls sie nicht global verfügbar ist.
    try:
        from pre_eda_data import analyze_semantic_type_v3
    except ImportError:
        # Falls das Modul anders heißt oder im Notebook direkt definiert wurde
        if 'analyze_semantic_type_v3' not in globals():
            print("⚠️ Warnung: analyze_semantic_type_v3 nicht gefunden. Nutze Fallback-Logik.")
            def analyze_semantic_type_v3(d): return pd.DataFrame(columns=['Spalte', 'Semantischer_Typ'])
    
    # Work on a copy to preserve the original state
    df = df_input.copy()
    
    # 1. Execute semantic analysis
    df_semantics = analyze_semantic_type_v3(df)
    
    # 2. Calculatory Keywords (The "Boost" list)
    calc_keywords = ['avg', 'max', 'min', 'score', 'value', 'amount', 'total', 'count', 'num', 'pct', 'rate', '€', '$', '£', '%']
    protected_names = ['dbn', 'name', 'lat', 'lon', 'coord', 'address', 'phone', 'fax', 'id']

    for _, row in df_semantics.iterrows():
        col = row['Spalte']
        sem_type = row['Semantischer_Typ']
        col_lower = col.lower()
        
        # --- A) IDENTITY PROTECTION (Veto Logic) ---
        is_identity_content = any(t in sem_type for t in ['ID', 'Geometrisch', 'Text', 'Freitext', 'Datum'])
        is_boosted = any(k in col_lower for k in calc_keywords)
        is_protected_name = any(k in col_lower for k in protected_names)

        if (is_identity_content or is_protected_name) and not is_boosted:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'NaN', 'nan.0'], np.nan)
            continue 
            
        # --- B) MEASUREMENT SYNCHRONIZATION (Numeric Casting) ---
        is_numeric_target = any(t in sem_type for t in ['Integer', 'Float', 'Waehrung', 'Prozentsatz'])
        
        if is_numeric_target or is_boosted:
            if df[col].dtype == 'object':
                # Surgical removal of symbols to allow numeric casting
                for symbol in ['%', '$', '€', '¥']:
                    df[col] = df[col].astype(str).str.replace(symbol, '', regex=False)
                df[col] = df[col].str.strip()
            
            # The 's' or blanks become NaN, enabling float64/int64 for SQL.
            temp_numeric = pd.to_numeric(df[col], errors='coerce')
            
            # Stochastische Entscheidung für den finalen Typ
            if any(k in col_lower for k in ['avg', 'pct', 'score', 'rate','float']):
                df[col] = temp_numeric.astype('float64')
            else:
                df[col] = temp_numeric.astype('Int64')

        # Kontrolle der Ergebnisse
    print(f"✅ Stochastic Type-Sync complete.")
    print(f"📊 Anzahl der NaN Werte (ehemals 's' oder leer) in Zahlen-Spalten:")
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    print(df[numeric_cols].isna().sum())
    return df

def clean_rows_remove_duplicates(df):
    import pandas as pd
    import numpy as np
    """Zusatz für Day 4: Echte Zeilen-Duplikate entfernen"""
    initial = len(df)
    df = df.drop_duplicates()
    print(f"✅ Duplikate entfernt: {initial - len(df)}")
    return df
# --- ANWENDUNG IM NOTEBOOK ---
#df = clean_column_names(df)
#df = full_stochastic_type_flote_int_cleaning(df)