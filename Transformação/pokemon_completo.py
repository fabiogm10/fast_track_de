import dlt

@dlt.table(
    name="pokemon_completo",
    comment="Tabela consolidada com todas as informações dos Pokémon",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def pokemon_completo():
    return spark.sql("""
        select  A.id as ID_POKEMON ,
        A.name as NOME, 
        B.habilidade as HABILIDADE,
        C.tipo as TIPO,
        D.geracao as GERACAO,
        D.classe_dano_movimento,
        D.tipo_nome as FORMA

from `workspace`.`treinamento-compass`.`pokemon_ids_names` as A
inner join workspace.`treinamento-compass`.habilidades_pokemon as B on A.id = B.id
inner join workspace.`treinamento-compass`.tipos_pokemon as C on A.id = C.id
inner join workspace.`treinamento-compass`.tipos_pokemon_completos as D on A.name = D.pokemon_nome
    """)