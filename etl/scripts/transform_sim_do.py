from sim import TransformarSimDO

transformar_cnes_sr = TransformarSimDO(
    nome_arquivo_saida="sim-do.csv",
    causa_basica_list=('F10', 'F11', 'F12', 'F13', 'F14', 'F15', 'F16', 'F18', 'F19')
)

transformar_cnes_sr.sumarizar_por_causa_basica()
