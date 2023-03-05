import pandas as pd
import requests


def obter_detalhes_cnes():
    url = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos"
    params = {"codigo_tipo_unidade": "70", "limit": "20", "offset": "0"}
    dataframes = []

    while True:
        # Faz a requisição HTTP com os parâmetros de busca
        response = requests.get(url, params=params)
        response_json = response.json()

        # Verifica se há mais páginas de resultados
        if not response_json["estabelecimentos"]:
            break

        # Converte os resultados para um dataframe pandas
        dataframe = pd.DataFrame(response_json["estabelecimentos"])
        dataframes.append(dataframe)

        # Define o offset da próxima página de resultados
        params["offset"] = str(int(params["offset"]) + int(params["limit"]))

    # Concatena todos os dataframes em um único dataframe e converte para JSON
    result_dataframe = pd.concat(dataframes)
    result_json = result_dataframe.to_json(orient="records")

    # Salva o resultado em um arquivo JSON
    with open("cnes.json", "w") as f:
        f.write(result_json)


if __name__ == "__main__":
    obter_detalhes_cnes()
