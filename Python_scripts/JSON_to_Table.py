# Pega qualquer JSON arbitrário e gera automaticamente tabelas SQL normalizadas, incluindo relações para arrays e objetos aninhados
import json

# Função para inferir tipos SQL básicos
def infer_sql_type(values):
    types = set()
    for v in values:
        if v is None:
            continue
        elif isinstance(v, bool):
            types.add("BOOLEAN")
        elif isinstance(v, int):
            types.add("INT")
        elif isinstance(v, float):
            types.add("FLOAT")
        else:
            types.add("VARCHAR(255)")
    if len(types) == 0:
        return "VARCHAR(255)"
    elif len(types) == 1:
        return types.pop()
    else:
        # conflito de tipos → usar genérico
        return "VARCHAR(255)"

# Função recursiva para gerar schema
def generate_schema(data, table_name="root", parent=None):
    columns = {}
    subtables = {}

    if isinstance(data, list):
        for item in data:
            c, s = generate_schema(item, table_name, parent)
            for k, v in c.items():
                if k not in columns:
                    columns[k] = [v]
                else:
                    columns[k].append(v)
            subtables.update(s)
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                subtables[key] = generate_schema(value, key, table_name)
            elif isinstance(value, list):
                subtables[key] = generate_schema(value, key, table_name)
            else:
                columns[key] = [infer_sql_type([value])]
    # resolver conflitos de tipos
    final_columns = {k: infer_sql_type(v) for k, v in columns.items()}
    return final_columns, subtables

# Função para gerar SQL a partir do schema inferido
def schema_to_sql(schema, table_name="root", parent=None):
    columns, subtables = schema
    sql = f"CREATE TABLE {table_name} (\n"
    if parent:
        sql += f"    {parent}_id INT,\n"
    for col, col_type in columns.items():
        sql += f"    {col} {col_type},\n"
    sql = sql.rstrip(",\n") + "\n);\n\n"
    for subname, subschema in subtables.items():
        sql += schema_to_sql(subschema, subname, table_name)
    return sql

# --- Exemplo de uso ---
json_data = '''
[
  {
    "id": 1,
    "nome": "Alice",
    "endereco": {
      "rua": "Rua A",
      "cidade": "Cidade X"
    },
    "telefones": ["1234-5678", "9876-5432"]
  },
  {
    "id": 2,
    "nome": "Bob",
    "endereco": {
      "rua": "Rua B",
      "cidade": "Cidade Y",
      "complemento": "Apto 101"
    }
  }
]
'''

data = json.loads(json_data)
schema = generate_schema(data)
sql_code = schema_to_sql(schema)
print(sql_code)
