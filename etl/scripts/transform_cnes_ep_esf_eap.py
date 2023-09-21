from equipes import TransformarEquipesCnes

transform = TransformarEquipesCnes(
    nome_arquivo_saida='cnes-ep-esf-eap.csv',
    classificacao_equipe_esf_eap={16: 'EAP', 17: 'EAP', 18: 'EAP', 76: 'EAP', 1: 'ESF', 12: 'ESF', 14: 'ESF',
                                  24: 'ESF', 27: 'ESF', 30: 'ESF', 33: 'ESF', 36: 'ESF', 70: 'ESF'})
transform.filtrar_tipos_equipes()
transform.enriquecer_tipos_equipes()
