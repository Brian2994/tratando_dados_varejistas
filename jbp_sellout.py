# Importar bibliotecas
import pandas as pd
import gcsfs

# Variáveis para o mês e ano de referência
mes = 10  # Filtrar por mês
ano = 2024  # Filtrar por ano

def load_data_from_gcs(mes, ano):
    # Construir o caminho do bucket de forma dinâmica com base no ano e mês
    bucket_path = f'gs://ype-sellout/raw/jbp/{ano}/{mes:02d}/'
    # Criar um sistema de arquivos GCS
    fs = gcsfs.GCSFileSystem()    
    # Listar todos os arquivos CSV no bucket
    all_files = fs.ls(bucket_path)  # lista de arquivos
    csv_files = [f for f in all_files if f.endswith('.csv')]  # filtra arquivos CSV

    print("Arquivos CSV encontrados:", csv_files)  # Imprimir arquivos

    # Inicializa uma lista para armazenar todos os DataFrames
    df_list = []

    for file in csv_files:
        print(f'Lendo o arquivo: {file}')  # Mensagem de depuração
        try:
            arquivo = 'gs://' + file
            df = pd.read_csv(arquivo, sep=';', encoding='utf-8')  # Lê o CSV

            # Verifica se a coluna de data existe
            if 'DATA' in df.columns:
                # Tenta converter a coluna 'DATA' para datetime, preservando erros
                df['Data'] = pd.to_datetime(df['DATA'],format='%d/%m/%Y', errors='coerce')
                df['Data'] = pd.to_datetime(df['Data'].fillna(df['Data']), format='%Y-%m-%d', errors='coerce')
            df_list.append(df)  # Adiciona o DataFrame à lista
            print(f'O arquivo {file} foi lido com sucesso, número de linhas: {len(df)}')
        except Exception as e:
            print(f'Erro ao ler o arquivo {file}: {e}')

    # Verifica se a lista está vazia
    if not df_list:
        print('Nenhum DataFrame foi adicionado à lista.')
        return None
    
    # Concatena todos os DataFrames em um único DataFrame
    all_data = pd.concat(df_list, ignore_index=True)
    
    return all_data

def process_data_from_gcs(df, mes, ano):
    if 'Varejista' in df.columns:
        # Substituindo Amigao por Amigão na coluna Varejista
        df['Varejista'] = df['Varejista'].str.replace('Amigao', 'Amigão', regex=False)
        # Substituindo Apoio Entrega por Apoio Mineiro na coluna Varejista
        df['Varejista'] = df['Varejista'].str.replace('Apoio Entrega', 'Apoio Mineiro', regex=False)
        # Substituindo Sams por Sams Club na coluna Varejista
        df['Varejista'] = df['Varejista'].str.replace('SAMS', 'Sams Club', regex=False)

    if 'EAN' in df.columns:
        df['EAN'] = df['EAN'].replace(['#N/D', ''], 0)
        df['EAN'] = pd.to_numeric(df['EAN'], errors='coerce').fillna(0).astype(int)

    if 'Quantidade' in df.columns:
        # Substituir vírgulas por pontos e converter para float
        df['Quantidade'] = df['Quantidade'].replace(',', '.', regex=True)  # Troca vírgulas por pontos
        df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce')  # Converte para numérico
        # Preencher NaN com 0 antes da conversão para inteiro
        df['Quantidade'] = df['Quantidade'].fillna(0).astype(int)

    if 'Pedidos' in df.columns:
        # Substituir vírgulas por pontos e converter para float
        df['Pedidos'] = df['Pedidos'].replace(',', '.', regex=True)  # Troca vírgulas por pontos
        df['Pedidos'] = pd.to_numeric(df['Pedidos'], errors='coerce')  # Converte para numérico
        # Preencher NaN com 0 antes da conversão para inteiro
        df['Pedidos'] = df['Pedidos'].fillna(0).astype(int)

    if 'Cod_loja' in df.columns:
        df['Cod_loja'] = df['Cod_loja'].fillna(0).astype(int)

    if 'Canal de Venda' in df.columns:
        df.rename(columns={'Canal de Venda': 'Canal_de_Venda'}, inplace=True)

    if 'Descrição' in df.columns:
        df.rename(columns={'Descrição': 'Descricao'}, inplace=True)

    # Lista de columnas esperadas
    colunas_esperadas = [
        'Data',
        'Varejista',
        'Canal_de_Venda',
        'EAN',
        'Descricao',
        'Receita',
        'Quantidade',
        'Pedidos',
        'UF',
        'Cidade',
        'Cod_loja',
        'Loja'
    ]

    for coluna in colunas_esperadas:
        if coluna not in df.columns:
            print(f'{coluna} não esta presente no arquivo!')

    # Filtrar os dados para o mês e ano de referência
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
    df_filtrado = df[(df['Data'].dt.month == mes) & (df['Data'].dt.year == ano)].copy()

    # Formatando a coluna 'Data' para o formato 'DD/MM/YYYY'
    df_filtrado['Data'] = df_filtrado['Data'].dt.strftime('%d/%m/%Y')

    # Organizar o DataFrame com base em colunas_esperadas
    df_filtrado = df_filtrado[colunas_esperadas]
    
    print(df_filtrado.head())
    print(df_filtrado.info())

    return df_filtrado

def save_data_from_gcs(df, mes, ano):
    # Caminho do GCS onde sera salvo o arquivo já tratado
    arquivo_processado = f'gs://ype-sellout/trusted/jbp/{ano}/{mes:02d}/compilados_sellout_{mes:02d}_{ano}.csv'
    df.to_csv(arquivo_processado, sep=';', encoding='utf-8', index=False) # Usamos ponto e virgula como separador 
    print('Arquivo tratado e salvo com sucesso!')

# Carregar os dados
df = load_data_from_gcs(mes, ano)
df_processado = process_data_from_gcs(df, mes, ano)
save_data_from_gcs(df_processado, mes, ano)

# Finalizado!