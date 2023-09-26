from servico_especializado import TransformarCnesSR

transformar_cnes_sr = TransformarCnesSR(
    nome_arquivo_saida="cnes-sr-quantitativo-ua-adulto-infantil.csv",
    nome_totalizador="TOTAL_UNIDADES_ACOLHIMENTO",
    codigos_servico_especializado=[115],
    codigos_tipo_unidade=[],
    codigos_classificacao=[6, 7]
)

transformar_cnes_sr.calcular_quantitativo_sr()
