# Treinamento Compass
Engenharia FastTrack

A API escolhida para o treinamento foi a API com dados de POKEMONS.

# APIs utilizadas: 

## Busca todos os pokémons
    pokemon_list_url = "https://pokeapi.co/api/v2/pokemon?limit=100000"

## Busca os detalhes completos do Pokémon
            pokemon_detail_url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"

## Busca os detalhes completos do tipo
            type_detail_url = f"https://pokeapi.co/api/v2/type/{type_id}"

### Os arquivos de ingestão são executas na sequencia em que aparecem para gerar as Delta tables:

- Ingestão_pokemons_id
- pokemon_caracteristicas
- pokemon_classes
- pokemon_tipos

### O arquivo de transformação:
- pokemon_completo

### Gera a tabela final unificando as informações das tabelas geradas:
- workspace.`treinamento-compass`.habilidades_pokemon
- workspace.`treinamento-compass`.pokemon_completo
- workspace.`treinamento-compass`.pokemon_ids_names
- workspace.`treinamento-compass`.tipos_pokemon
- workspace.`treinamento-compass`.tipos_pokemon_completos

### As analises são feitas através do noteBook :
- Pokemon_exploração




