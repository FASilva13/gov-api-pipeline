import pandas as pd


class GovTransformer:
    def __init__(self, df):
        self.df = df

    def clean_data(self):
        print("Colunas originais:", self.df.columns.tolist())
        df_clean = self.df.copy()

        def find_col(keywords):
            for col in df_clean.columns:
                if any(k.upper() in col.upper() for k in keywords):
                    return col
            return None

        # Mapeamento ajustado com base no seu LOG de colunas
        mapping_search = {
            'id_servidor_portal': ['Id_SERVIDOR_PORTAL'],
            'nome': ['NOME'],
            'cpf': ['CPF'],
            'nome_orgao': ['ORG_LOTACAO', 'ORGAO_LOTACAO'], # Nomes exatos do log
            'descricao_cargo': ['DESCRICAO_CARGO'],
            'ano': ['ANO'],
            'mes': ['MES'],
            'remuneracao_basica_bruta': ['REMUNERAÇÃO BÁSICA BRUTA (R$)', 'BRUTA'],
            'remuneracao_apos_deducoes': ['REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)', 'APOS DEDUCOES']
        }

        mapping = {}
        for final_name, keywords in mapping_search.items():
            found = find_col(keywords)
            if found:
                mapping[found] = final_name

        # Seleciona apenas as colunas encontradas
        df_final = df_clean[list(mapping.keys())].copy()
        df_final = df_final.rename(columns=mapping)

        # 1. Tratamento financeiro
        cols_financeiras = ['remuneracao_basica_bruta', 'remuneracao_apos_deducoes']
        for col in cols_financeiras:
            if col in df_final.columns:
                df_final[col] = df_final[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0)

        # 2. Tratamento de Strings (Onde estava dando o erro)
        cols_texto = ['nome', 'nome_orgao', 'descricao_cargo']
        for col in cols_texto:
            if col in df_final.columns:
                # O .astype(str) garante que o .str.title() não quebre
                df_final[col] = df_final[col].astype(str).str.title().str.strip()

        return df_final