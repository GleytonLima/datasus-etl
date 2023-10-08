from servico_especializado import TransformarCnesSR

transformar_cnes_sr = TransformarCnesSR(
    nome_arquivo_saida="cnes-sr-numero-servicos-ambulatoriais-em-psiquiatria.csv",
    nome_totalizador="TOTAL_SERVICO_AMBULATORIAL_EM_PSIQUIATRIA",
    codigos_servico_especializado=[115],
    codigos_tipo_unidade=[],
    codigos_classificacao=[4, 36]
)

transformar_cnes_sr.calcular_quantitativo_sr()