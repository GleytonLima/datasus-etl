from equipes import TransformarEquipesCnes

transform = TransformarEquipesCnes(
    nome_arquivo_saida='cnes-ep-equipe-multiprofissional-saude-mental.csv',
    classificacao_equipe_esf_eap={58: 'ECR', 59: 'ECR', 60: 'ECR', 75: 'ECR'})
transform.filtrar_tipos_equipes()
transform.enriquecer_tipos_equipes()
