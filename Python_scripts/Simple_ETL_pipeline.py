# Pipeline ETL com Python
import pandas as pd
import sqlite3
import os

DB_FILE = "etl_pipeline.db"

def extract(file_path):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    elif file_path.endswith(".json"):
        return pd.read_json(file_path)
    else:
        raise ValueError("Formato não suportado!")

def transform(df):
    # Exemplo de transformação: renomear colunas e remover duplicados
    df = df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"))
    df = df.drop_duplicates()
    # Adicionar coluna de exemplo
    if "valor" in df.columns:
        df["valor_normalizado"] = df["valor"] / df["valor"].max()
    return df

def load(df, table_name, db_file=DB_FILE):
    conn = sqlite3.connect(db_file)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Tabela '{table_name}' carregada com sucesso no banco {db_file}.")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)  # garante pasta data
    file_path = "data/exemplo.csv"      # altere para seu arquivo
    table_name = "exemplo"

    df = extract(file_path)
    df = transform(df)
    load(df, table_name)
