# Escolha a imagem base do R
FROM r-base:4.3.1

# Defina o diretório de trabalho dentro do container
WORKDIR /app

# Instale o Python e pacotes Python necessários
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    apt-get install nano

# Copie o requirements.txt
ADD requirements.txt requirements.txt

# Instale as dependências
RUN python3 -m venv .venv && \
    . .venv/bin/activate && \
    python3 -m pip install -r requirements.txt

# Instale as bibliotecas R necessárias, incluindo a "read.dbc"
RUN R -e "install.packages(c('devtools', 'read.dbc'))"

# Comando para executar o script definido pela variável de ambiente
CMD python3 $SCRIPT_NAME