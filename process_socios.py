import os
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from mongo_connection import get_collection

def process_single_csv_socios(csv_file_path, db):
    collection = get_collection(db, "socios")
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

            # Mapear os campos conforme as posições especificadas no CSV para "Socios"
            cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)
            identificador_socio = row[1].strip()  # Identificador do Sócio (posição 1)
            nome_socio = row[2].strip()  # Nome do Sócio (posição 2)
            cpf_cnpj_socio = row[3].strip() if row[3].strip() else None  # CPF/CNPJ do Sócio (posição 3)
            qualificacao_socio = row[4].strip()  # Qualificação do Sócio (posição 4)
            data_entrada_sociedade = row[5].strip()  # Data de Entrada na Sociedade (posição 5)
            pais_socio = row[6].strip() if row[6].strip() else None  # País do Sócio (posição 6)
            representante_legal = row[7].strip() if row[7].strip() else None  # Representante Legal (posição 7)
            qualificacao_representante = row[8].strip() if row[8].strip() else None  # Qualificação do Representante (posição 8)
            faixa_etaria = row[9].strip() if row[9].strip() else None  # Faixa Etária (posição 9)

            # Verificar se os campos principais não estão vazios ou nulos
            if cnpj_basico and identificador_socio:
                # Criar a chave primária única usando a combinação de cnpj_basico e identificador_socio
                primary_key = f"{cnpj_basico}_{identificador_socio}"

                # Adicionar documento para inserção em massa
                documents_to_insert.append({
                    "_id": primary_key,  # Definir a chave primária
                    "cnpj_basico": cnpj_basico,
                    "identificador_socio": identificador_socio,
                    "nome_socio": nome_socio,
                    "cpf_cnpj_socio": cpf_cnpj_socio,
                    "qualificacao_socio": qualificacao_socio,
                    "data_entrada_sociedade": data_entrada_sociedade,
                    "pais_socio": pais_socio,
                    "representante_legal": representante_legal,
                    "qualificacao_representante": qualificacao_representante,
                    "faixa_etaria": faixa_etaria,
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

def process_socios(category_folder_path, db):
    global csv_files
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    csv_files = sorted([os.path.join(category_folder_path, f) for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    # Processar arquivos CSV em paralelo com um número específico de threads
    num_threads = min(8, len(csv_files))  # Ajuste o número de threads conforme necessário
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(executor.map(lambda csv_file: process_single_csv_socios(csv_file, db), csv_files))

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_socios("/caminho/para/a/pasta/categoria", db)
