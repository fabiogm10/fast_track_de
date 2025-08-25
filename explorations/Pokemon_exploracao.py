# Databricks notebook source
# MAGIC %md
# MAGIC ### Exploração dos dados Pokemons
# MAGIC

# COMMAND ----------

import sys

sys.path.append("/Workspace/Users/fabiogm10@gmail.com/Pipeline-Treinamento-Compass/fast_track_de")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.`treinamento-compass`.pokemon_ids_names "))
