import psycopg2
import pandas as pd

# Verbindung zur PostgreSQL-Datenbank herstellen
try:
    conn = psycopg2.connect(
        host="localhost",  # oder die IP-Adresse des Docker-Containers
        port="5432",
        database="postgres",  # Name deiner Datenbank
        user="postgres",  # Benutzername
        password="root"  # Passwort
    )
    cursor = conn.cursor()
    print("Verbindung zur Datenbank erfolgreich hergestellt.")
except Exception as e:
    print(f"Fehler bei der Verbindung zur Datenbank: {e}")
    exit()

# CSV-Dateien einlesen
files = {
    "Rind": 'csv/stammdaten_3175_20230420-20230419_2023-04-19.csv',
    "Futteraufnahme": 'csv/futterbesuche_3171_20210601-20230301_2023-04-19.csv',
    "Milchleistung": 'csv/melkdaten_3170_20210601-20230331_2023-04-19.csv',
    "Aktivitaetsdaten": 'csv/wasserbesuche_3173_20210601-20230331_2023-04-19.csv',
    "BCS_Daten": 'csv/wasserbesuche_3172_20210601-20230331_2023-04-19.csv',
    "Gesundheitsdaten": 'csv/diagnosen_3174_20210601-20230331_2023-04-19.csv',
    "Tagesdaten": 'csv/tagesdaten_3169_20210601-20230331_2023-04-19.csv',
}

# Tabellen für die Daten vorbereiten
table_columns = {
    "Rind": ["lom", "tiernr", "name", "geschlecht", "gebdat", "rasse"],
    "Futteraufnahme": ["tiernr", "lom", "datum", "ration", "menge"],
    "Milchleistung": ["tiernr", "lom", "datum", "mkg", "fett_prozent", "eiweiss_prozent"],
    "Aktivitaetsdaten": ["tiernr", "lom", "datum", "akti"],
    "BCS_Daten": ["tiernr", "lom", "datum", "bcs_m", "bcs_a"],
    "Gesundheitsdaten": ["tiernr", "lom", "datum", "kategorie", "diagnose"],
    "Tagesdaten": ["tiernr", "lom", "datum", "mkg", "gewicht", "bcs_m", "bcs_a"],
}

# Spalten in den Datenframes umbenennen
rename_columns = {
    "Rind": {"lom": "lom", "tiernr": "tiernr", "name": "name", "geschlecht": "geschlecht", "gebdat": "gebdat", "rasse": "rasse"},
    "Futteraufnahme": {"tiernr": "tiernr", "lom": "lom", "datum": "datum", "ration": "ration", "menge": "menge"},
    "Milchleistung": {"tiernr": "tiernr", "lom": "lom", "datum": "datum", "mkg": "mkg", "fett-%": "fett_prozent", "eiweiss-%": "eiweiss_prozent"},
    "Aktivitaetsdaten": {"tiernr": "tiernr", "lom": "lom", "datum": "datum", "akti": "akti"},
    "BCS_Daten": {"tiernr": "tiernr", "lom": "lom", "datum": "datum", "bcs_m": "bcs_m", "bcs_a": "bcs_a"},
    "Gesundheitsdaten": {"tiernr": "tiernr", "lom": "lom", "datum": "datum", "kategorie": "kategorie", "diagnose": "diagnose"},
    "Tagesdaten": {"tiernr": "tiernr", "lom": "lom", "datum": "datum", "mkg": "mkg", "gewicht": "gewicht", "bcs_m": "bcs_m", "bcs_a": "bcs_a"},
}

# Überprüfen, ob die lom-Werte in der Tabelle "Rind" vorhanden sind
def check_lom_exists(lom_value):
    cursor.execute("SELECT EXISTS(SELECT 1 FROM Rind WHERE lom=%s)", (str(lom_value),))
    return cursor.fetchone()[0]

# CSV-Dateien in die Datenbank laden
def load_csv_to_db(table, file, rename_cols, table_cols, lom_position=0):
    try:
        # CSV-Datei einlesen
        df = pd.read_csv(file, encoding='latin1', delimiter=';', low_memory=False)

        # Tatsächliche Spaltennamen anzeigen
        print(f"Spaltennamen der Datei {file}: {df.columns.tolist()}")

        # Spalten umbenennen
        if table in rename_cols:
            df = df.rename(columns=rename_cols[table])

        # Nur die relevanten Spalten behalten
        df = df[table_cols[table]]

        # Überprüfen, ob die Spalten in der DataFrame vorhanden sind
        missing_columns = [col for col in table_cols[table] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Fehlende Spalten in der CSV-Datei für die Tabelle {table}: {missing_columns}")

        # Daten in die entsprechende Tabelle einfügen
        for index, row in df.iterrows():
            lom_value = row[df.columns[lom_position]]
            if table != "Rind" and not check_lom_exists(lom_value):
                print(f"Überspringe Zeile mit ungültigem lom: {lom_value}")
                continue

            cols = ','.join(table_cols[table])
            vals = ','.join(["%s"] * len(row))
            sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
            cursor.execute(sql, tuple(row))
        print(f"Daten erfolgreich in die Tabelle {table} geladen.")
    except Exception as e:
        print(f"Fehler beim Laden der Daten in die Tabelle {table}: {e}")

# Zuerst die Daten für "Rind" laden
load_csv_to_db("Rind", files["Rind"], rename_columns, table_columns, lom_position=1)

# Dann die restlichen Tabellen laden
for table in files:
    if table != "Rind":
        load_csv_to_db(table, files[table], rename_columns, table_columns, lom_position=5)

# Änderungen speichern und Verbindung schließen
conn.commit()
cursor.close()
conn.close()
