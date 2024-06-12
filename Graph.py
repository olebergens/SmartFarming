import pandas as pd
import matplotlib.pyplot as plt

# Funktion zum Laden von CSV-Dateien mit Kodierungserkennung und Fehlerbehandlung
def load_csv_with_encoding(file_path, sep=';'):
    try:
        df = pd.read_csv(file_path, encoding='utf-8', sep=sep, low_memory=False)
        df.columns = df.columns.str.strip().str.replace('"', '').str.replace(' ', '')
        df = df.reset_index(drop=True)  # Entferne den Index und setze ihn neu
        print(f"Loaded {file_path} with 'utf-8' encoding.")
        return df
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=sep, low_memory=False)
        df.columns = df.columns.str.strip().str.replace('"', '').str.replace(' ', '')
        df = df.reset_index(drop=True)  # Entferne den Index und setze ihn neu
        print(f"Loaded {file_path} with 'ISO-8859-1' encoding.")
        return df
    except Exception as e:
        print(f"Failed to load {file_path}: {e}")
        return None

# Funktion zur Bereinigung und Konvertierung von numerischen Daten
def clean_and_convert_numeric(df, columns):
    for column in columns:
        df[column] = df[column].astype(str).str.replace(',', '.').astype(float)
    return df

# Dateien laden und überprüfen
file_paths = {
    'futterbesuche': 'csv/futterbesuche_3171_20210601-20230301_2023-04-19.csv',
    'melkdaten': 'csv/melkdaten_3170_20210601-20230331_2023-04-19.csv',
    'stammdaten': 'csv/stammdaten_3175_20230420-20230419_2023-04-19.csv',
    'tagesdaten': 'csv/tagesdaten_3169_20210601-20230331_2023-04-19.csv',
    'wasserbesuche_1': 'csv/wasserbesuche_3172_20210601-20230331_2023-04-19.csv',
    'wasserbesuche_2': 'csv/wasserbesuche_3173_20210601-20230331_2023-04-19.csv'
}

dfs = {}
for key, path in file_paths.items():
    df = load_csv_with_encoding(path)
    if df is not None:
        dfs[key] = df
        print(f"First 5 rows of {path}:")
        print(df.head())
        print(f"Columns in {path}: {df.columns.tolist()}")

# Überprüfen, ob alle Dateien erfolgreich geladen wurden
if len(dfs) != len(file_paths):
    print(f"Error: Expected {len(file_paths)} files, but only {len(dfs)} were loaded.")
else:
    futterbesuche_df = dfs['futterbesuche']
    melkdaten_df = dfs['melkdaten']
    stammdaten_df = dfs['stammdaten']
    tagesdaten_df = dfs['tagesdaten']
    wasserbesuche_df_1 = dfs['wasserbesuche_1']
    wasserbesuche_df_2 = dfs['wasserbesuche_2']

    # Wasserbesuche-Daten zusammenführen
    wasserbesuche_df = pd.concat([wasserbesuche_df_1, wasserbesuche_df_2])

    # Angenommene Spaltennamen basierend auf dem Bild
    lom_column = 'lom'  # Eineindeutige Tieridentifikation
    date_column = 'datum'  # aktuelles Datum

    # Überprüfen, ob die Spalten in den DataFrames vorhanden sind
    required_columns = {
        'futterbesuche_df': [lom_column, date_column, 'ration'],  # TMR-Aufnahme frisch kg
        'melkdaten_df': [lom_column, date_column, 'mkg'],  # Milchmenge kg
        'tagesdaten_df': [lom_column, date_column],
        'wasserbesuche_df': [lom_column, date_column, 'h2o-aufn']  # Wasseraufnahmekg
    }

    for df_name, cols in required_columns.items():
        df = globals()[df_name]
        print(f"\nChecking columns in {df_name}:")
        for col in cols:
            if col in df.columns:
                print(f"  Column '{col}' is present.")
            else:
                print(f"  Column '{col}' is MISSING!")

    # Wenn alle erforderlichen Spalten vorhanden sind, den Rest des Codes ausführen
    if all(all(col in globals()[df_name].columns for col in cols) for df_name, cols in required_columns.items()):
        # Konvertiere relevante Spalten in numerische Werte
        futterbesuche_df = clean_and_convert_numeric(futterbesuche_df, ['ration'])
        melkdaten_df = clean_and_convert_numeric(melkdaten_df, ['mkg'])
        wasserbesuche_df = clean_and_convert_numeric(wasserbesuche_df, ['h2o-aufn'])

        # Loms extrahieren
        loms = futterbesuche_df[lom_column].unique()

        # Durch alle Loms iterieren
        for lom in loms:
            # Daten für die spezifische Lom filtern
            futterbesuche_lom = futterbesuche_df[futterbesuche_df[lom_column] == lom].sort_values(by=date_column)
            melkdaten_lom = melkdaten_df[melkdaten_df[lom_column] == lom].sort_values(by=date_column)
            tagesdaten_lom = tagesdaten_df[tagesdaten_df[lom_column] == lom].sort_values(by=date_column)
            wasserbesuche_lom = wasserbesuche_df[wasserbesuche_df[lom_column] == lom].sort_values(by=date_column)

            # Konvertiere Datumsspalte in datetime
            futterbesuche_lom[date_column] = pd.to_datetime(futterbesuche_lom[date_column], errors='coerce')
            melkdaten_lom[date_column] = pd.to_datetime(melkdaten_lom[date_column], errors='coerce')
            tagesdaten_lom[date_column] = pd.to_datetime(tagesdaten_lom[date_column], errors='coerce')
            wasserbesuche_lom[date_column] = pd.to_datetime(wasserbesuche_lom[date_column], errors='coerce')

            # Entferne Zeilen mit ungültigen Datumswerten
            futterbesuche_lom = futterbesuche_lom.dropna(subset=[date_column])
            melkdaten_lom = melkdaten_lom.dropna(subset=[date_column])
            tagesdaten_lom = tagesdaten_lom.dropna(subset=[date_column])
            wasserbesuche_lom = wasserbesuche_lom.dropna(subset=[date_column])

            # Entferne Zeilen mit NaN-Werten in den interessierenden Spalten
            futterbesuche_lom = futterbesuche_lom.dropna(subset=['ration'])
            melkdaten_lom = melkdaten_lom.dropna(subset=['mkg'])
            wasserbesuche_lom = wasserbesuche_lom.dropna(subset=['h2o-aufn'])

            # Zeitachsen erstellen und plotten
            plt.figure(figsize=(15, 10))
            plt.suptitle(f'Lom ID: {lom}', fontsize=16)

            # Futterbesuche über die Zeit
            plt.subplot(2, 3, 1)
            plt.plot(futterbesuche_lom[date_column], futterbesuche_lom['ration'], marker='o')
            plt.title('Futterbesuche über die Zeit')
            plt.xlabel('Datum')
            plt.ylabel('Frischmasse (kg)')
            plt.xticks(rotation=45)

            # Melkdaten über die Zeit
            plt.subplot(2, 3, 2)
            plt.plot(melkdaten_lom[date_column], melkdaten_lom['mkg'], marker='o')
            plt.title('Melkdaten über die Zeit')
            plt.xlabel('Datum')
            plt.ylabel('Milchmenge (kg)')
            plt.xticks(rotation=45)

            # Wasserbesuche über die Zeit
            plt.subplot(2, 3, 3)
            plt.plot(wasserbesuche_lom[date_column], wasserbesuche_lom['h2o-aufn'], marker='o')
            plt.title('Wasserbesuche über die Zeit')
            plt.xlabel('Datum')
            plt.ylabel('Wasseraufnahme (kg)')
            plt.xticks(rotation=45)

            plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            plt.show()

        # Durchschnittswerte berechnen
        average_futterbesuche = futterbesuche_df.groupby(date_column)['ration'].mean()
        average_melkdaten = melkdaten_df.groupby(date_column)['mkg'].mean()
        average_wasserbesuche = wasserbesuche_df.groupby(date_column)['h2o-aufn'].mean()

        # Durchschnittswerte plotten
        plt.figure(figsize=(15, 5))
        plt.subplot(1, 3, 1)
        plt.plot(average_futterbesuche.index, average_futterbesuche.values, marker='o')
        plt.title('Durchschnittliche Futterbesuche über die Zeit')
        plt.xlabel('Datum')
        plt.ylabel('Durchschnittliche Frischmasse (kg)')
        plt.xticks(rotation=45)

        plt.subplot(1, 3, 2)
        plt.plot(average_melkdaten.index, average_melkdaten.values, marker='o')
        plt.title('Durchschnittliche Melkdaten über die Zeit')
        plt.xlabel('Datum')
        plt.ylabel('Durchschnittliche Milchmenge (kg)')
        plt.xticks(rotation=45)

        plt.subplot(1, 3, 3)
        plt.plot(average_wasserbesuche.index, average_wasserbesuche.values, marker='o')
        plt.title('Durchschnittliche Wasserbesuche über die Zeit')
        plt.xlabel('Datum')
        plt.ylabel('Durchschnittliche Wasseraufnahme (kg)')
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.show()


