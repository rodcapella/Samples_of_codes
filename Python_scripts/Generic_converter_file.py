# Um script Python que lê um arquivo CSV, JSON ou Excel, converte para outro formato e salva.
import pandas as pd
import argparse

def convert_file(input_file, output_file):
    # Detectar extensão de entrada
    if input_file.endswith(".csv"):
        df = pd.read_csv(input_file)
    elif input_file.endswith(".json"):
        df = pd.read_json(input_file)
    elif input_file.endswith(".xlsx"):
        df = pd.read_excel(input_file)
    else:
        raise ValueError("Formato de arquivo não suportado!")

    # Salvar em formato de saída
    if output_file.endswith(".csv"):
        df.to_csv(output_file, index=False)
    elif output_file.endswith(".json"):
        df.to_json(output_file, orient="records", indent=4)
    elif output_file.endswith(".xlsx"):
        df.to_excel(output_file, index=False)
    else:
        raise ValueError("Formato de saída não suportado!")

    print(f"Arquivo convertido com sucesso: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conversor CSV ↔ JSON ↔ Excel")
    parser.add_argument("input", help="Arquivo de entrada")
    parser.add_argument("output", help="Arquivo de saída")
    args = parser.parse_args()

    convert_file(args.input, args.output)
