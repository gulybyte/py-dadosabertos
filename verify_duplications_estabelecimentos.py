import os
import csv
from tqdm import tqdm

def process_single_csv(csv_file_path):
    print(f"Lendo arquivo {csv_file_path}...")

    # Contar o número de linhas no arquivo para a barra de progresso
    total_lines = 0
    with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
        total_lines = sum(1 for _ in file)
    
    # Set para rastrear CNPJs e detectar duplicatas
    cnpj_seen = set()
    duplications = []

    # Processar o arquivo CSV
    with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
        reader = csv.reader((line.replace('\0', '') for line in file), delimiter=';')
        progress_bar = tqdm(total=total_lines, desc=f"Processando {os.path.basename(csv_file_path)}")

        for row in reader:
            if not row:
                continue

            cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)

            # Verificar duplicação
            if cnpj_basico in cnpj_seen:
                duplications.append(cnpj_basico)
            else:
                cnpj_seen.add(cnpj_basico)

            progress_bar.update(1)
        
        progress_bar.close()

        # Exibir CNPJs duplicados, se houver
        if duplications:
            print(f"Foram encontrados {len(duplications)} CNPJs duplicados no arquivo {os.path.basename(csv_file_path)}.")
            for cnpj in duplications:
                print(f"Duplicado: {cnpj}")
        else:
            print(f"Não foram encontradas duplicações no arquivo {os.path.basename(csv_file_path)}.")

def verify_duplications_estabelecimentos(category_folder_path):
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    csv_files = sorted([os.path.join(category_folder_path, f) for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    # Processar arquivos CSV em paralelo, se necessário
    for csv_file in csv_files:
        process_single_csv(csv_file)

# Exemplo de uso:
# verify_duplications_estabelecimentos("/caminho/para/a/pasta/categoria")
