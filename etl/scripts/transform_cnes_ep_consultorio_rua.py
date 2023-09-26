from equipes import TransformarEquipesCnes

transform = TransformarEquipesCnes(
    nome_arquivo_saida='cnes-ep-consultorio-rua.csv',
    classificacao_equipe_esf_eap={40: 'ECR', 41: 'ECR', 42: 'ECR', 73: 'ECR'})
transform.filtrar_tipos_equipes()
transform.enriquecer_tipos_equipes()
