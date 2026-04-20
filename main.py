from pipeline.extract import GovExtractor
from pipeline.transform import GovTransformer
from pipeline.load import BigQueryLoader


def start_pipeline():
    # 1. EXTRAÇÃO (Arquivos locais ou API)
    extractor = GovExtractor()
    df_raw = extractor.fetch_servidores()

    if not df_raw.empty:
        # 2. TRANSFORMAÇÃO (Limpeza de números e nomes)
        transformer = GovTransformer(df_raw)
        df_final = transformer.clean_data()

        # 3. CARGA (Subida para a nuvem)
        loader = BigQueryLoader()
        loader.load_dataframe(df_final, "remuneracao_servidores")
    else:
        print("Pipeline interrompido: Falha na extração.")


if __name__ == "__main__":
    start_pipeline()