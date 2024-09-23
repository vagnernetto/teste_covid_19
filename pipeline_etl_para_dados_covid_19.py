# Importando a biblioteca
import pandas as pd
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit
import logging
import requests
import time

__author__='vagner.rnetto'
__contact__='https://www.linkedin.com/in/vagnerrnetto/'
__version__='1.0'

# Configuração do logger
logging.basicConfig(filename='etl_activity.log', level=logging.INFO)

class covide_19:
    # Função para Extração
    def extracao_csv(self):
        start_time = time.time()  # Início da contagem do tempo
        url = "https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv"
        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
        
        # Fazendo a requisição
        response = requests.get(url)
        if response.status_code == 200:
            # Lê o CSV apenas se a requisição for bem-sucedida
            data = pd.read_csv(url)
            num_linhas, num_colunas = data.shape
            print(f"Extração concluída com sucesso, existem {num_colunas} colunas e {num_linhas} linhas.")
        else:
            raise Exception(f"Erro ao acessar a URL: {response.status_code}")

        elapsed_time = time.time() - start_time  # Tempo decorrido
        print(f"Tempo de extração: {elapsed_time:.2f} segundos")
        return data

    # Função para validar dados
    def validar_dados(self, data):
        required_columns = ['location_key', 'date']  # Adicione mais colunas conforme necessário
        for col in required_columns:
            if col not in data.columns:
                print(f"Atenção: Coluna ausente: {col}")

        if data.isnull().any().any():
            print("Atenção: Dados contêm valores nulos.")
            print(data[data.isnull().any(axis=1)])  # Imprime as linhas com valores nulos
        else:
            print("Validação de dados concluída com sucesso.")

    # Função para transformação
    def transformacao_csv(self, data):
        start_time = time.time()  # Início da contagem do tempo
        self.validar_dados(data)  # Valida os dados antes da transformação

        # Normalização de dados
        data['location_key'] = data['location_key'].str.strip().str.lower()
        
        # Pivotar o DataFrame
        melted_data = pd.melt(data, id_vars=['location_key', 'date', 'latitude', 'longitude'], var_name='variable', value_name='value')
        
        # Converter valores para numérico
        melted_data['value'] = pd.to_numeric(melted_data['value'], errors='coerce')
        
        # Remover duplicatas
        melted_data = melted_data.drop_duplicates()
        
        # Renomear coluna
        melted_data.rename(columns={'variable': 'indicator'}, inplace=True)
        
        num_linhas, num_colunas = melted_data.shape  
        print(f"Transformação concluída com sucesso, existem {num_colunas} colunas e um total geral de {num_linhas} após pivot.")
        elapsed_time = time.time() - start_time  # Tempo decorrido
        print(f"Tempo de transformação: {elapsed_time:.2f} segundos")
        return melted_data

    # Função de controle de acesso
    def verificar_acesso(self, user_id):
        permitted_users = ['user_123', 'admin']  # Exemplo de usuários permitidos
        if user_id not in permitted_users:
            raise PermissionError("Acesso negado para o usuário.")
        print(f"Acesso concedido para o usuário: {user_id}")

    # Função para carga
    def carga_load_df(self, df_melted_data, user_id, source):
        self.verificar_acesso(user_id)  # Verifica se o usuário tem acesso
        spark = SparkSession.builder.appName("COVID_ETL").getOrCreate()
        
        # Definindo configuração para mesclar esquemas
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        spark_df = spark.createDataFrame(df_melted_data)

        # Adicionar colunas de controle
        spark_df = spark_df.withColumn("created_at", current_timestamp()) \
                        .withColumn("updated_at", current_timestamp()) \
                        .withColumn("record_id", monotonically_increasing_id()) \
                        .withColumn("commit_dt", current_timestamp()) \
                        .withColumn("user_id", lit(user_id)) \
                        .withColumn("source", lit(source))

        start_time = time.time()  # Início da contagem do tempo
        try:
            # Salvar o DataFrame no formato Delta
            spark_df.write.format("delta").mode("overwrite").save("/FileStore/tables/covid-data/delta_table")
            print("Load concluído com sucesso!")
            logging.info(f"Carga realizada com sucesso por {user_id} de {source}.")
        except Exception as e:
            raise Exception(f"Erro ao carregar dados no Databricks: {e}")

        elapsed_time = time.time() - start_time  # Tempo decorrido
        print(f"Tempo de carga: {elapsed_time:.2f} segundos")

    def sumario_dos_dados(self):
        spark = SparkSession.builder.appName("LeituraDelta").getOrCreate()
        delta_table_path = "/FileStore/tables/covid-data/delta_table"
        df_spark = spark.read.format("delta").load(delta_table_path)
        df = df_spark.toPandas()
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        summary = df.describe(include='all', datetime_is_numeric=True)
        grouped = df.groupby('indicator')['value'].agg(['mean', 'sum', 'count']).reset_index()
        print("Análise Sumária:")
        print(summary)
        print("\nAgrupamento por 'indicator':")
        print(grouped)

    def executor_processo(self, processo, user_id, source):
        if processo == 'extracao_csv':
            data = self.extracao_csv()
            return data
        elif processo == 'transformacao_csv':
            data = self.extracao_csv()
            return self.transformacao_csv(data)
        elif processo == 'carga_load_df':
            transformed_data = self.transformacao_csv(self.extracao_csv())
            self.carga_load_df(transformed_data, user_id, source)
            return transformed_data
        elif processo == 'processar_etl':
            data = self.extracao_csv()
            transformed_data = self.transformacao_csv(data)
            self.carga_load_df(transformed_data, user_id, source)
            return data, transformed_data
        elif processo == 'analise_sumaria':
            return self.sumario_dos_dados()
        else:
            raise ValueError("Processo não encontrado.")

# Uso da classe
executor = covide_19()
resultado = executor.executor_processo('processar_etl', user_id='user_123', source='API')

