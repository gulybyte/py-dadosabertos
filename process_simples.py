import os
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from mongo_connection import get_collection

def process_single_csv_simples(csv_file_path, db):
    collection = get_collection(db, "simples")
    print(f"Lendo arquivo {csv_file_path}...")

    # Contar o número de linhas no arquivo para a barra de progresso
    total_lines = 0
    with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
        total_lines = sum(1 for _ in file)
    
    # Processar o arquivo CSV
    with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
        reader = csv.reader((line.replace('\0', '') for line in file), delimiter=';')
        progress_bar = tqdm(total=total_lines, desc=f"Processando {os.path.basename(csv_file_path)}", position=csv_files.index(csv_file_path))

        documents_to_insert = []
        for row in reader:
            if not row:
                continue

            # Mapear os campos conforme as posições especificadas no CSV para "Simples"
            cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)
            opcao_pelo_simples = row[1].strip()  # Opção pelo Simples (posição 1)
            data_opcao_simples = row[2].strip()  # Data da Opção pelo Simples (posição 2)
            data_exclusao_simples = row[3].strip() if row[3].strip() else None  # Data da Exclusão do Simples (posição 3)
            opcao_mei = row[4].strip()  # Opção pelo MEI (posição 4)
            data_opcao_mei = row[5].strip()  # Data da Opção pelo MEI (posição 5)
            data_exclusao_mei = row[6].strip() if row[6].strip() else None  # Data da Exclusão do MEI (posição 6)

            # Verificar se os campos principais não estão vazios ou nulos
            if cnpj_basico:
                # Adicionar documento para inserção em massa
                documents_to_insert.append({
                    "cnpj_basico": cnpj_basico,
                    "opcao_pelo_simples": opcao_pelo_simples,
                    "data_opcao_simples": data_opcao_simples,
                    "data_exclusao_simples": data_exclusao_simples,
                    "opcao_mei": opcao_mei,
                    "data_opcao_mei": data_opcao_mei,
                    "data_exclusao_mei": data_exclusao_mei,
                })

                # Inserir documentos em massa quando atingir o limite
                if len(documents_to_insert) >= 1000:
                    collection.insert_many(documents_to_insert)
                    documents_to_insert.clear()
            
            progress_bar.update(1)
        
        # Inserir quaisquer documentos restantes
        if documents_to_insert:
            collection.insert_many(documents_to_insert)
        
        progress_bar.close()

def process_simples(category_folder_path, db):
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    global csv_files
    csv_files = sorted([os.path.join(category_folder_path, f) for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    # Processar arquivos CSV em paralelo com um número específico de threads
    num_threads = min(32, len(csv_files))  # Ajuste o número de threads conforme necessário
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(executor.map(lambda csv_file: process_single_csv_simples(csv_file, db), csv_files))

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_simples("/caminho/para/a/pasta/categoria", db)
