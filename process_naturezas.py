import os
import csv
from mongo_connection import get_collection

def process_naturezas(category_folder_path, db):
    collection = get_collection(db, "naturezas")
    
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    csv_files = sorted([f for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    for csv_file in csv_files:
        csv_file_path = os.path.join(category_folder_path, csv_file)
        print(f"Lendo arquivo {csv_file_path}...")

        # Abrir o CSV sem cabeçalho e mapear campos por posição
        with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
            reader = csv.reader(file, delimiter=';')
            
            for row in reader:
                codigo = row[0]  # Exemplo: Código da natureza jurídica (posição 0)
                descricao = row[1]  # Exemplo: Descrição da natureza jurídica (posição 1)

                # Verificar se os campos não estão vazios ou nulos
                if codigo and descricao:
                    # Verificar se o código já existe no MongoDB
                    if collection.find_one({"codigo": codigo.strip()}) is None:
                        document = {
                            "codigo": codigo.strip(),
                            "descricao": descricao.strip()
                        }
                        collection.insert_one(document)
                        print(f"Documento inserido: {document}")
                    else:
                        print(f"Código {codigo.strip()} já existe. Registro ignorado.")
                else:
                    print(f"Registro ignorado por conter campos vazios: {row}")
