import os
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from mongo_connection import get_collection

def process_single_csv(csv_file_path, db):
    collection = get_collection(db, "empresas")
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

            cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)
            razao_social = row[1].strip()  # Razão Social (posição 1)
            natureza_juridica = row[2].strip()  # Natureza Jurídica (posição 2)
            qualificacao_responsavel = row[3].strip()  # Qualificação do Responsável (posição 3)
            capital_social = row[4].replace(",", ".").strip()  # Capital Social (posição 4), substitui vírgula por ponto
            porte = row[5].strip()  # Porte da Empresa (posição 5)
            ente_federativo_responsavel = row[6].strip() if row[6].strip() else None  # Ente Federativo (posição 6), pode ser vazio

            # Verificar se os campos principais não estão vazios ou nulos
            if cnpj_basico and razao_social:
                # Adicionar documento para inserção em massa
                documents_to_insert.append({
                    "cnpj_basico": cnpj_basico,
                    "razao_social": razao_social,
                    "natureza_juridica": natureza_juridica,
                    "qualificacao_responsavel": qualificacao_responsavel,
                    "capital_social": float(capital_social),  # Converter capital social para número
                    "porte": porte,
                    "ente_federativo_responsavel": ente_federativo_responsavel
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

def process_empresas(category_folder_path, db):
    global csv_files
    csv_files = sorted([os.path.join(category_folder_path, f) for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    # Processar arquivos CSV em paralelo com um número específico de threads
    num_threads = min(32, len(csv_files))  # Ajuste o número de threads conforme necessário
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(executor.map(lambda csv_file: process_single_csv(csv_file, db), csv_files))

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_empresas("/caminho/para/a/pasta/categoria", db)
