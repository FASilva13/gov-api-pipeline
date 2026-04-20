import pandas as pd


class GovTransformer:
    def __init__(self, df):
        self.df = df

    def clean_data(self):
        print("Limpando e transformando os dados...")

        # 1. Selecionar apenas colunas importantes (Exemplos)
        # Verifique os nomes exatos no seu console e ajuste se necessário
        cols_interesse = [
            'NOME', 'CPF', 'NOME ÓRGÃO', 'DESCRICAO_CARGO',
            'REMUNERAÇÃO BÁSICA BRUTA (R$)', 'VALOR TOTAL DA REMUNERAÇÃO APÓS TETO (R$)'
        ]

        # Filtramos o DataFrame (se as colunas existirem)
        df_clean = self.df[[c for c in cols_interesse if c in self.df.columns]].copy()

        # 2. Converter valores financeiros: "1.250,50" -> 1250.50
        cols_financeiras = [c for c in df_clean.columns if '(R$)' in c]

        for col in cols_financeiras:
            df_clean[col] = df_clean[col].str.replace('.', '', regex=False)
            df_clean[col] = df_clean[col].str.replace(',', '.', regex=False)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)

        # 3. Padronizar nomes (Opcional, mas bom para o portfólio)
        df_clean['NOME'] = df_clean['NOME'].str.title()

        return df_clean