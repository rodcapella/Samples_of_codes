# -*- coding: utf-8 -*-
import unicodedata, re
from rapidfuzz import process, fuzz
from tqdm import tqdm
import pandas as pd

# === 1. Ler ficheiros ===
df_erradas = pd.read_csv("citys_distincts_erradas.csv", encoding="latin1")
df_ref = pd.read_csv("lista_localidades_exatas.csv", encoding="latin1")

# === 2. Extrair lista oficial ===
freguesias_ref = df_ref["desig_postal"].dropna().astype(str).unique().tolist()

def limpar_texto(nome):
    if pd.isna(nome) or not isinstance(nome, str):
        return ""
    nome = unicodedata.normalize("NFKD", nome)
    nome = nome.encode("ASCII","ignore").decode("utf-8","ignore")
    nome = re.sub(r"[^A-Za-z\s\-]", " ", nome)
    nome = re.sub(r"[-]", " ", nome)
    nome = re.sub(r"\s+", " ", nome).strip()
    nome = re.sub(r"\b(Uni[oã]o|das|dos|de|da|do|freguesia|freguesias|municipio)\b",
                  "", nome, flags=re.IGNORECASE)
    return nome.title().strip()

# Limpa e atualiza bases
df_erradas["city_clean"] = df_erradas["city"].apply(limpar_texto)
df_ref["desig_clean"] = df_ref["desig_postal"].apply(limpar_texto)
freguesias_ref = df_ref["desig_clean"].dropna().astype(str).unique().tolist()

def fuzzy_match_robusto(nome, lista_ref):
    if not nome:
        return ""
    resultado = process.extractOne(
        nome,
        lista_ref,
        scorer=fuzz.partial_ratio,   # tolera fragmentos
        score_cutoff=60
    )
    if resultado is None:
        return ""
    match, score, _ = resultado
    match = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", match)
    return match.upper().strip()

tqdm.pandas(desc=" Corrigindo localidades (modo robusto)...")
df_erradas["city_corrected"] = df_erradas["city_clean"].progress_apply(
    lambda x: fuzzy_match_robusto(x, freguesias_ref)
)

import openpyxl

def limpar_illegal(val):
    if pd.isna(val):
        return ""
    # remove caracteres ilegais e de controle
    val = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", str(val))
    # remove encoding corrompido (ex: Ã, Â, etc.)
    val = val.encode("latin1", "ignore").decode("utf-8", "ignore")
    return val.strip()

for col in ["city", "city_corrected"]:
    df_erradas[col] = df_erradas[col].apply(limpar_illegal)

df_final = df_erradas[["city", "city_corrected"]].copy()

# garantir que tudo é string segura
df_final = df_final.astype(str).applymap(lambda x: limpar_illegal(x))

df_final.to_excel("citys_distincts_corrigidas.xlsx", index=False, engine="openpyxl")

from rapidfuzz import fuzz
import numpy as np

# calcular média de similaridade entre amostras
amostra_erradas = df_erradas["city_clean"].sample(50, random_state=1).tolist()
amostra_ref = freguesias_ref[:50]

scores = []
for a in amostra_erradas:
    melhores = [fuzz.token_sort_ratio(a, r) for r in amostra_ref]
    scores.append(max(melhores))

print(f"Média de similaridade (amostra): {np.mean(scores):.2f}%")
print(f"Maior score encontrado: {np.max(scores):.2f}%")

print("citys_distincts_corrigidas.xlsx criado com sucesso!")
print(f"Total de linhas: {len(df_final)}")
print(f"Total corrigidas: {(df_final['city_corrected'] != '').sum()}")
print(f"Total sem correspondência: {(df_final['city_corrected'] == '').sum()}")
