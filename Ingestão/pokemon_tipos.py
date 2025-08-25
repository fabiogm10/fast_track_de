import dlt
import requests
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def fetch_pokemon_types():
    # Primeiro, buscamos todos os pokémons
    pokemon_list_url = "https://pokeapi.co/api/v2/pokemon?limit=100000"
    pokemon_response = requests.get(pokemon_list_url)
    pokemon_results = pokemon_response.json()["results"]
    
    types_data = []
    
    for pokemon in pokemon_results:
        try:
            # Extrai o ID do Pokémon
            pokemon_id = int(pokemon["url"].rstrip('/').split('/')[-1])
            
            # Busca os detalhes completos do Pokémon
            pokemon_detail_url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
            detail_response = requests.get(pokemon_detail_url)
            
            if detail_response.status_code == 200:
                pokemon_data = detail_response.json()
                
                # Extrai todos os tipos do Pokémon
                for type_info in pokemon_data.get("types", []):
                    slot = type_info["slot"]
                    type_name = type_info["type"]["name"]
                    
                    types_data.append({
                        "id": pokemon_id,
                        "slot": slot,
                        "tipo": type_name
                    })
            
        except Exception as e:
            print(f"Erro ao processar Pokémon ID {pokemon_id}: {e}")
            continue
    
    return types_data

@dlt.table(
    name="tipos_pokemon",
    comment="Tipos de cada Pokémon com sua posição (slot)"
)
def tipos_pokemon():
    data = fetch_pokemon_types()
    df = pd.DataFrame(data)
    
    # Define o schema explicitamente para melhor performance
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("slot", IntegerType(), True),
        StructField("tipo", StringType(), True)
    ])
    
    return spark.createDataFrame(df, schema=schema)