from servico_especializado import TransformarCnesSR

transformar_cnes = TransformarCnesSR(
    nome_arquivo_saida="cnes-sr-quantitativo-servicos-hospitalares-saude-mental.csv",
    nome_totalizador="TOTAL_SERVICO_HOSPITALAR_SAUDE_MENTAL",
    codigos_servico_especializado=[115],
    codigos_tipo_unidade=[5, 7, 62],
    codigos_classificacao=[2, 3]
)

transformar_cnes.calcular_quantitativo_sr()
