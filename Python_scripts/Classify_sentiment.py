# Um script Python que lê textos (por exemplo, reviews ou tweets) e classifica como positivo, negativo ou neutro usando a biblioteca TextBlob.
import pandas as pd
from textblob import TextBlob

def classify_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0.1:
        return "positivo"
    elif analysis.sentiment.polarity < -0.1:
        return "negativo"
    else:
        return "neutro"

def analyze_file(file_path):
    df = pd.read_csv(file_path)
    if "texto" not in df.columns:
        raise ValueError("O CSV deve ter uma coluna chamada 'texto'.")
    
    df["sentimento"] = df["texto"].apply(classify_sentiment)
    df.to_csv("data/resultados.csv", index=False)
    print("Análise concluída! Resultados salvos em data/resultados.csv")
    print(df.head())

if __name__ == "__main__":
    analyze_file("data/exemplos.csv")
