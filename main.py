from pipeline.extract import GovExtractor
from pipeline.transform import GovTransformer
from pipeline.load import BigQueryLoader


def start_pipeline():
    extractor = GovExtractor()
    loader = BigQueryLoader()

    # Lista de arquivos para processar
    arquivos_para_processar = [
        {'tipo': 'Remuneracao', 'tabela': 'remuneracao_servidores'},
        {'tipo': 'Cadastro', 'tabela': 'cadastro_servidores'}
    ]

    for item in arquivos_para_processar:
        print(f"\n--- Processando {item['tipo']} ---")

        # 1. Extract
        df_raw = extractor.fetch_data(tipo=item['tipo'])

        if not df_raw.empty:
            # 2. Transform (A sua função clean_data com mapping dinâmico resolve os dois!)
            transformer = GovTransformer(df_raw)
            df_final = transformer.clean_data()

            # 3. Load
            loader.load_dataframe(df_final, item['tabela'])
        else:
            print(f"Pulei {item['tipo']} devido a erro na extração.")


if __name__ == "__main__":
    start_pipeline()