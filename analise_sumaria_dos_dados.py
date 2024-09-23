import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import round

__author__='vagner.rnetto@gmail.com'
__contact__='https://www.linkedin.com/in/vagnerrnetto/'
__version__='1.0'

def processar_dados_covid(delta_table_path, worldcities_path):
    # Criar uma sessão Spark
    spark = SparkSession.builder.appName("LeituraDelta").getOrCreate()

    # Ler a tabela Delta
    df_spark = spark.read.format("delta").load(delta_table_path)
    df = df_spark.toPandas()

    # Ler o arquivo CSV de cidades
    df2 = spark.read.csv(worldcities_path, inferSchema=True, header=True)

    # Arredondar latitude e longitude
    df2 = df2.withColumn("lat", round(df2["lat"], 2))
    df2 = df2.withColumn("lng", round(df2["lng"], 2))

    # Filtrar os dados
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df_filtered = df[
        (df['latitude'].notnull()) &
        (df['longitude'].notnull()) &
        (df['indicator'].isin(['cumulative_confirmed_male', 'cumulative_confirmed_female', 'cumulative_persons_fully_vaccinated']))
    ].copy()

    # Merge dos dados
    df_filtered_spark = spark.createDataFrame(df_filtered)
    df_merged = df_filtered_spark.join(
        df2,
        (df_filtered_spark['latitude'] == df2['lat']) & 
        (df_filtered_spark['longitude'] == df2['lng']),
        "left"
    )
    df_lat_lon = df_merged.toPandas()

    # Gráfico de comparação de casos confirmados por gênero
    plt.figure(figsize=(10, 6))
    df_lat_lon_g1 = df_lat_lon[
        (df_lat_lon['value'] > 0) &
        (df_lat_lon['indicator'].isin(['cumulative_confirmed_female', 'cumulative_confirmed_male']))
    ].copy()
    df_pivot = df_lat_lon_g1.pivot_table(index='indicator', values='value', aggfunc='sum')
    ax = df_pivot.plot(kind='bar', figsize=(10, 6))
    for container in ax.containers:
        ax.bar_label(container, fmt='%.0f')
    plt.title('Comparação de Casos Confirmados por Gênero')
    plt.xlabel('Indicador')
    plt.ylabel('Número de Casos')
    ax.set_xticklabels(['Cumulativo Confirmado Masculino', 'Cumulativo Confirmado Feminino'], rotation=45)
    ax.legend().set_visible(False)
    plt.tight_layout()
    plt.show()

    # Adiciona um espaço visual
    plt.figure(figsize=(10, 0.5))  # Espaço vazio
    plt.axis('off')  # Não exibir eixo
    plt.show()

    # Gráfico de comparação ao longo do tempo
    plt.figure(figsize=(10, 6))
    df_lat_lon_g1 = df_lat_lon[
        (df_lat_lon['value'] > 0) &
        (df_lat_lon['indicator'].isin(['cumulative_confirmed_female', 'cumulative_confirmed_male']))
    ].copy()
    df_pivot = df_lat_lon_g1.pivot_table(index='date', columns='indicator', values='value', aggfunc='sum').reset_index()
    ax = df_pivot.plot(x='date', kind='bar', figsize=(10, 6), width=0.8)
    for container in ax.containers:
        ax.bar_label(container, fmt='%.0f', label_type='edge')
    plt.title('Comparação de Casos Confirmados por Gênero ao Longo do Tempo')
    plt.xlabel('Data')
    plt.ylabel('Número de Casos Confirmados')
    plt.xticks(rotation=45)
    ax.legend().set_visible(False)
    plt.tight_layout()
    plt.show()

    # Adiciona um espaço visual
    plt.figure(figsize=(10, 0.5))  # Espaço vazio
    plt.axis('off')  # Não exibir eixo
    plt.show()

    # Mapa de calor de média por país
    plt.figure(figsize=(10, 6))
    df_lat_lon['date'] = pd.to_datetime(df_lat_lon['date'])
    df_filtered = df_lat_lon[
        (df_lat_lon['latitude'].notnull()) &
        (df_lat_lon['longitude'].notnull()) &
        (~df_lat_lon['indicator'].isin(['cumulative_persons_fully_vaccinated'])) &
        (df_lat_lon['value'] > 0)
    ].copy()
    country_avg = df_filtered.groupby(['country', 'indicator'])['value'].mean().unstack().fillna(0)
    country_avg = country_avg.sort_values(by=country_avg.columns[-1], ascending=False)
    sns.heatmap(country_avg, annot=True, fmt=".1f", cmap="YlGnBu", cbar_kws={'label': 'Média de Value'})
    plt.title('Mapa de Calor de Cumulative Confirmed por País')
    plt.xlabel('Indicador')
    plt.ylabel('País')
    plt.tight_layout()
    plt.show()

    # Adiciona um espaço visual
    plt.figure(figsize=(10, 0.5))  # Espaço vazio
    plt.axis('off')  # Não exibir eixo
    plt.show()

    # Comparação de vacinados e população
    population_data = {
        'country': ['Singapore', 'United States', 'Germany', 'Poland', 'Argentina', 
                    'Israel', 'Maldives', 'India', 'Malaysia', 'Jersey', 'Colombia'],
        'population': [5850342, 331002651, 83783942, 38386000, 45195777, 
                       8655535, 5156963, 1380004385, 32365999, 108800, 50882891]
    }
    population_df = pd.DataFrame(population_data)

    df_filtered = df_lat_lon[
        (df_lat_lon['indicator'] == 'cumulative_persons_fully_vaccinated') &
        (df_lat_lon['value'] > 0)
    ]
    country_sum = df_filtered.groupby('country')['value'].sum().reset_index()
    country_sum = country_sum.sort_values(by='value', ascending=False)
    comparison_df = pd.merge(country_sum, population_df, on='country', how='inner')

    fig, ax1 = plt.subplots(figsize=(12, 6))
    x = np.arange(len(comparison_df['country']))
    bars = ax1.bar(x, comparison_df['population'], width=0.4, color='lightgreen', label='População')
    for i in range(len(comparison_df)):
        ax1.text(i, comparison_df['population'].iloc[i], f'{comparison_df["population"].iloc[i]:,.0f}', ha='center', va='bottom')

    ax1.set_title('Comparação de Cumulative Persons Fully Vaccinated e População por País')
    ax1.set_xlabel('País')
    ax1.set_ylabel('População')
    ax1.set_xticks(x)
    ax1.set_xticklabels(comparison_df['country'], rotation=45)
    ax1.tick_params(axis='y', labelsize=10)
    ax1.legend(loc='upper right', fontsize=10)

    ax2 = ax1.twinx()
    line = ax2.plot(x, comparison_df['value'], color='skyblue', marker='o', linewidth=2, label='Totalmente Vacinadas')
    for i in range(len(comparison_df)):
        ax2.text(i, comparison_df['value'].iloc[i], f'{comparison_df["value"].iloc[i]:,.0f}', ha='center', va='bottom')

    ax2.set_ylabel('Totalmente Vacinadas')
    ax2.tick_params(axis='y', labelsize=10)

    plt.tight_layout()
    plt.show()

# Exemplo de uso da função
processar_dados_covid("/FileStore/tables/covid-data/delta_table", "/FileStore/tables/worldcities/worldcities.csv")
