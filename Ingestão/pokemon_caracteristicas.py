import dlt
import requests
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def fetch_pokemon_abilities():
    # Primeiro, buscamos todos os pokémons
    pokemon_list_url = "https://pokeapi.co/api/v2/pokemon?limit=100000"
    pokemon_response = requests.get(pokemon_list_url)
    pokemon_results = pokemon_response.json()["results"]
    
    abilities_data = []
    
    for pokemon in pokemon_results:
        try:
            # Extrai o ID do Pokémon
            pokemon_id = int(pokemon["url"].rstrip('/').split('/')[-1])
            
            # Busca os detalhes completos do Pokémon
            pokemon_detail_url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
            detail_response = requests.get(pokemon_detail_url)
            
            if detail_response.status_code == 200:
                pokemon_data = detail_response.json()
                
                # Extrai todas as habilidades
                for ability in pokemon_data.get("abilities", []):
                    ability_name = ability["ability"]["name"]
                    
                    abilities_data.append({
                        "id": pokemon_id,
                        "habilidade": ability_name
                    })
            
        except Exception as e:
            print(f"Erro ao processar Pokémon ID {pokemon_id}: {e}")
            continue
    
    return abilities_data

@dlt.table(
    name="habilidades_pokemon",
    comment="Todas as habilidades de cada Pokémon"
)
def habilidades_pokemon():
    data = fetch_pokemon_abilities()
    df = pd.DataFrame(data)
    
    # Define o schema explicitamente para melhor performance
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("habilidade", StringType(), True)
    ])
    
    return spark.createDataFrame(df, schema=schema)