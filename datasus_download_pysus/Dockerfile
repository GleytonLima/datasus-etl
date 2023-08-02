# Use uma imagem base do Python
FROM python:3

# Defina o diretório de trabalho dentro do container
WORKDIR /app

# Copie o requirements.txt
ADD ./requirements.txt requirements.txt

RUN apt-get update && apt-get install nano

# Instale as dependências
RUN pip install -r requirements.txt

# Defina uma variável de ambiente com o nome do script (use "seu_script.py" como valor padrão)
ENV SCRIPT_NAME="download_cnes_ep.py"

# Comando para executar o script definido pela variável de ambiente
CMD python $SCRIPT_NAME
