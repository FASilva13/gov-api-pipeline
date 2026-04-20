from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()


class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset_id = os.getenv("DATASET_ID")
        self.project_id = os.getenv("GCP_PROJECT_ID")

    def load_dataframe(self, df, table_name):
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        # Configuração da carga: Se a tabela já existir, ele substitui (WRITE_TRUNCATE)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        print(f"Subindo {len(df)} linhas para o BigQuery ({table_id})...")

        try:
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Espera a carga terminar
            print(f"Sucesso! Tabela {table_name} atualizada.")
        except Exception as e:
            print(f"Erro ao carregar para o BigQuery: {e}")