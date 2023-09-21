from equipes import TransformarEquipesCnes

transform = TransformarEquipesCnes(
    nome_arquivo_saida='cnes-ep-esf-nasf.csv',
    classificacao_equipe_esf_eap={6: 'NASF', 7: 'NASF', 45: 'NASF', 72: 'NASF',
                                  1: 'ESF', 12: 'ESF', 14: 'ESF', 24: 'ESF',
                                  27: 'ESF', 30: 'ESF', 33: 'ESF', 36: 'ESF', 70: 'ESF'})
transform.filtrar_tipos_equipes()
transform.enriquecer_tipos_equipes()
