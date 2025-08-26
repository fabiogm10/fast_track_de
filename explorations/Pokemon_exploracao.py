# Databricks notebook source
# MAGIC %md
# MAGIC ### Exploração dos dados Pokemons
# MAGIC

# COMMAND ----------

import sys

sys.path.append("/Workspace/Users/fabiogm10@gmail.com/Pipeline-Treinamento-Compass/fast_track_de")

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Executa a query e converte para Pandas DataFrame
df = spark.sql("""
    SELECT HABILIDADE,
           count(*) as qtd_habilidade
    FROM `workspace`.`treinamento-compass`.`pokemon_completo`
    GROUP BY HABILIDADE
    ORDER BY qtd_habilidade DESC 
    LIMIT 10
""").toPandas()

# Configura o estilo do gráfico
plt.style.use('default')
plt.figure(figsize=(12, 8))

# Cria o gráfico de barras
bars = plt.bar(df['HABILIDADE'], df['qtd_habilidade'], 
               color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#F9A602', '#9B59B6', 
                      '#E74C3C', '#3498DB', '#2ECC71', '#F39C12', '#9B59B6'])

# Adiciona valores nas barras
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f'{int(height)}', ha='center', va='bottom', fontweight='bold')

# Configurações do gráfico
plt.title('Top 10 Habilidades Mais Comuns em Pokémon', fontsize=16, fontweight='bold', pad=20)
plt.xlabel('Habilidades', fontsize=12, fontweight='bold')
plt.ylabel('Quantidade de Pokémon', fontsize=12, fontweight='bold')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', alpha=0.3)

# Ajusta layout e mostra o gráfico
plt.tight_layout()
plt.show()

# Mostra os dados em tabela também
display(df)

# COMMAND ----------

import pandas as pd

# Executa a query do Spark e converte para pandas DataFrame
df = spark.sql("""
    SELECT NOME,
           count(*) as qtd_habilidade
    FROM `workspace`.`treinamento-compass`.`pokemon_completo`
    GROUP BY NOME, HABILIDADE
    ORDER BY qtd_habilidade DESC 
    LIMIT 10
""").toPandas()

# Exibe o DataFrame com formatação
print("Top 10 Pokémon com Mais Habilidades:")
print("=" * 50)

# Formata o DataFrame para melhor visualização
df_display = df.copy()
df_display['qtd_habilidade'] = df_display['qtd_habilidade'].astype(int)

# Exibe o DataFrame
display(df_display)



# COMMAND ----------

import pandas as pd

# Executa a query do Spark e converte para pandas DataFrame
df = spark.sql("""
    select GERACAO,
       count(*) as qtd_por_geracao
from `workspace`.`treinamento-compass`.`pokemon_completo`
group by GERACAO
order by qtd_por_geracao desc limit 10;
""").toPandas()


# Exibe o DataFrame
display(df)

# COMMAND ----------

import pandas as pd

# Executa a query do Spark e converte para pandas DataFrame
df = spark.sql("""
   select FORMA,
       count(*) as qtd_por_forma
from `workspace`.`treinamento-compass`.`pokemon_completo`
group by FORMA
order by qtd_por_forma desc limit 10;
""").toPandas()


# Exibe o DataFrame
display(df)

# COMMAND ----------

import pandas as pd

# Executa a query do Spark e converte para pandas DataFrame
df = spark.sql("""
select classe_dano_movimento,
       count(*) as qtd_por_classe_mov
from `workspace`.`treinamento-compass`.`pokemon_completo`
group by classe_dano_movimento
order by qtd_por_classe_mov desc ;
""").toPandas()


# Exibe o DataFrame
display(df)
