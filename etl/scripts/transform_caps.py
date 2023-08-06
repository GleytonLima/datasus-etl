from caps import EstabelecimentoSubtipo, Estabelecimento, EstabelecimentoColuna, Subtipo, Combinado, \
    CombinadoEnriquecido, DatamartIndicadorCapsMunicipio, DatamartIndicadorCapsEstado, DatamartIndicadorCapsRegiaoSaude, \
    criar_arquivo_lista_tipo_caps, criar_arquivo_lista_anos, criar_arquivo_lista_anos_leitos


ANOS_CONSIDERADOS = [2022]

PREFIXO_NOME_ANOS = f'{ANOS_CONSIDERADOS[0]}-{ANOS_CONSIDERADOS[-1]}'

ANOS_LEITOS_CONSIDERADOS = [2022]

MES_COMPETENCIA_CONSIDERADO = "12"

criar_arquivo_lista_tipo_caps()
criar_arquivo_lista_anos(anos=ANOS_CONSIDERADOS)
criar_arquivo_lista_anos_leitos(anos=ANOS_LEITOS_CONSIDERADOS)

relacao = EstabelecimentoSubtipo(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO
)
relacao.filtrar_caps()
estabelecimento = Estabelecimento(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO
)
estabelecimento.filtrar_caps()
subtipo = Subtipo(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO
)
subtipo.filtrar_caps()
combinado = Combinado(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO,
    tabela_relacao=relacao,
    tabela_subtipo=subtipo,
    tabela_estabelecimentos=estabelecimento
)
combinado.combinar_estabelecimento_subtipo()
combinado.combinar_arquivos_ano()
combinado_enriquecido = CombinadoEnriquecido(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO,
    combinado=combinado
)
combinado_enriquecido.enriquecer_cnes()

datamart_indicador_caps_municipio = DatamartIndicadorCapsMunicipio(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO
)
datamart_indicador_caps_municipio.criar_datamart_com_indices_cobertura_caps_por_municipio(combinado_enriquecido=combinado_enriquecido)

datamart_indicador_caps_estado = DatamartIndicadorCapsEstado(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO
)
datamart_indicador_caps_estado.criar_datamart_com_indices_cobertura_caps_por_estados(combinado_enriquecido=combinado_enriquecido)

datamart_indicador_caps_regiao_saude = DatamartIndicadorCapsRegiaoSaude(
    anos=ANOS_CONSIDERADOS,
    mes=MES_COMPETENCIA_CONSIDERADO
)
datamart_indicador_caps_regiao_saude.criar_datamart_com_indices_cobertura_caps_por_regiao_saude(combinado_enriquecido=combinado_enriquecido)