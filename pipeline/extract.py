import requests
import zipfile
import io
import pandas as pd


class GovExtractor:
    def __init__(self):
        # URL oficial de download (ajustada)
        self.url = "https://portaldatransparencia.gov.br/download-de-dados/servidores/202601_Servidores_SIAPE"

        # O "disfarce": fingimos que somos um navegador comum
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://portaldatransparencia.gov.br/'
        }

    def fetch_servidores(self):
        print("Iniciando download do arquivo .zip...")

        try:
            # Baixando com os novos headers
            response = requests.get(self.url, headers=self.headers, timeout=120, stream=True)
            response.raise_for_status()

            print("Download concluído! \nAbrindo ZIP...")

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # O arquivo de remuneração geralmente termina com _Remuneracao.csv
                file_name = [f for f in z.namelist() if 'Remuneracao' in f][0]

                with z.open(file_name) as f:
                    print(f"Lendo as primeiras 1000 linhas de {file_name}...")
                    # O separador do governo é ';' e o encoding 'iso-8859-1'
                    df = pd.read_csv(f, sep=';', encoding='iso-8859-1', nrows=1000)
                    return df

        except Exception as e:
            print(f"Erro ao baixar/extrair: {e}")
            return pd.DataFrame()