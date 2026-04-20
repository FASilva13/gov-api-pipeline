import zipfile
import io
import requests
import pandas as pd


class GovExtractor:
    def __init__(self):
        # Usando a URL que funcionou para você
        self.url = "https://portaldatransparencia.gov.br/download-de-dados/servidores/202601_Servidores_SIAPE"
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def fetch_data(self, tipo='Remuneracao'):
        """
        tipo pode ser 'Remuneracao' ou 'Cadastro'
        """
        print(f"Iniciando extração do arquivo de {tipo}...")
        try:
            response = requests.get(self.url, headers=self.headers, timeout=120)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Busca o arquivo que contém a palavra chave no nome
                file_name = [f for f in z.namelist() if tipo in f][0]

                with z.open(file_name) as f:
                    print(f"Lendo {file_name}...")
                    # Lendo uma amostra maior para o seu portfólio
                    return pd.read_csv(f, sep=';', encoding='iso-8859-1', nrows=5000)
        except Exception as e:
            print(f"Erro na extração de {tipo}: {e}")
            return pd.DataFrame()