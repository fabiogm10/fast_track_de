import dlt
import requests
import pandas as pd

def fetch_all_pokemon():
    url = "https://pokeapi.co/api/v2/pokemon?limit=100000"
    response = requests.get(url)
    results = response.json()["results"]
    # Extract name and ID from the URL
    data = []
    for r in results:
        name = r["name"]
        # The ID is the last part of the URL
        id_ = int(r["url"].rstrip('/').split('/')[-1])
        data.append({"id": id_, "name": name})
    return data

@dlt.table(
    name="pokemon_ids_names",
    comment="Lista de IDs e nomes dos Pokemons"
)
def pokemon_ids_names():
    data = fetch_all_pokemon()
    df = pd.DataFrame(data)
    return spark.createDataFrame(df)