import dlt
import requests
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def fetch_pokemon_types_with_details_and_pokemon():
    # Busca todos os tipos disponíveis
    type_list_url = "https://pokeapi.co/api/v2/type?limit=100000&offset=0"
    type_response = requests.get(type_list_url)
    type_results = type_response.json()["results"]
    
    types_data = []
    
    for type_item in type_results:
        try:
            # Extrai o ID e nome do tipo
            type_id = int(type_item["url"].rstrip('/').split('/')[-1])
            type_name = type_item["name"]
            
            # Busca os detalhes completos do tipo
            type_detail_url = f"https://pokeapi.co/api/v2/type/{type_id}"
            detail_response = requests.get(type_detail_url)
            
            if detail_response.status_code == 200:
                type_data = detail_response.json()
                
                # Extrai informações da geração
                generation_name = type_data.get("generation", {}).get("name", "unknown")
                
                # Extrai informações da classe de dano de movimento
                move_damage_class = type_data.get("move_damage_class", {})
                move_damage_class_name = move_damage_class.get("name", "unknown") if move_damage_class else "unknown"
                
                # Extrai todos os Pokémon deste tipo
                for pokemon_info in type_data.get("pokemon", []):
                    pokemon_name = pokemon_info["pokemon"]["name"]
                    slot = pokemon_info["slot"]
                    
                    types_data.append({
                        "tipo_id": type_id,
                        "tipo_nome": type_name,
                        "pokemon_nome": pokemon_name,
                        "slot": slot,
                        "geracao": generation_name,
                        "classe_dano_movimento": move_damage_class_name
                    })
            
        except Exception as e:
            print(f"Erro ao processar tipo ID {type_id}: {e}")
            continue
    
    return types_data

@dlt.table(
    name="tipos_pokemon_completos",
    comment="Tipos de Pokémon com todos os Pokémon associados, geração e classe de dano"
)
def tipos_pokemon_completos():
    data = fetch_pokemon_types_with_details_and_pokemon()
    df = pd.DataFrame(data)
    
    # Define o schema explicitamente para melhor performance
    schema = StructType([
        StructField("tipo_id", IntegerType(), True),
        StructField("tipo_nome", StringType(), True),
        StructField("pokemon_nome", StringType(), True),
        StructField("slot", IntegerType(), True),
        StructField("geracao", StringType(), True),
        StructField("classe_dano_movimento", StringType(), True)
    ])
    
    return spark.createDataFrame(df, schema=schema)