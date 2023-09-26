from servico_especializado import TransformarCnesSR

transformar_cnes = TransformarCnesSR(
    nome_arquivo_saida="cnes-sr-quantitativo-servicos-residenciais-terapeuticos.csv",
    nome_totalizador="TOTAL_RESIDENCIAS_TERAPEUTICAS",
    codigos_servico_especializado=[115],
    codigos_tipo_unidade=[],
    codigos_classificacao=[4, 5]
)

transformar_cnes.calcular_quantitativo_sr()
