from leitos import TransformarCnesLT

transformar_cnes_lt = TransformarCnesLT(
    nome_arquivo_saida="cnes-lt-leitos-saude-mental.csv",
    nome_totalizador="TOTAL_LEITOS_SAUDE_MENTAL",
    codigos_tipo_unidade=[5, 7, 62],
    codigos_tipo_leito=[],
    codigos_especialidade_leito=[5, 47, 73, 87]
)

transformar_cnes_lt.filtrar()
transformar_cnes_lt.gerar_arquivo_auxiliar_tipo_unidade()
transformar_cnes_lt.gerar_arquivo_auxiliar_especializacao_leito()
