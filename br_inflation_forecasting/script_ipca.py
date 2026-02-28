import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

# ─────────────────────────────────────────────
# BACEN SGS API
# Docs: https://www.bcb.gov.br/content/developers/documenta%C3%A7%C3%A3o/api_serie_temporal_v2_2_ptbr.pdf
#
# Series IDs:
#   433  - IPCA (variação % mensal)
#   1    - Taxa de câmbio USD/BRL (PTAX - venda, fim de período, diário)
#
# NOTA: A API do BACEN limita séries diárias a janelas de 10 anos por request.
#       Para o câmbio, o script divide o período em chunks e concatena.
# ─────────────────────────────────────────────

START_DATE = datetime(2000, 1, 1)
END_DATE   = datetime(2024, 12, 31)
CHUNK_YEARS = 9  # usa 9 para garantir margem abaixo do limite de 10 anos

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"


def fetch_chunk(code, start: datetime, end: datetime):
    """Busca um chunk de dados da API do BACEN."""
    url = BASE_URL.format(code=code)
    params = {
        "formato":     "json",
        "dataInicial": start.strftime("%d/%m/%Y"),
        "dataFinal":   end.strftime("%d/%m/%Y"),
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"  Erro no chunk {start.date()} → {end.date()}: {response.text}")
        return None

    data = response.json()
    if not data:
        return None

    df = pd.DataFrame(data)
    df["data"]  = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df.dropna(subset=["valor"], inplace=True)

    return df


def fetch_bacen_series(code, name, daily=False):
    """
    Busca série completa do BACEN.
    Para séries diárias (daily=True), divide em chunks de CHUNK_YEARS anos.
    """
    print(f"\nFetching {name} (código {code})...")

    if not daily:
        # Séries mensais: busca direto sem chunking
        df = fetch_chunk(code, START_DATE, END_DATE)
        if df is None:
            print(f"Nenhuma observação encontrada para a série {code}")
            return None
        print(f"  {len(df)} observações encontradas.")
        return df

    # Séries diárias: divide em chunks de CHUNK_YEARS anos
    chunks = []
    chunk_start = START_DATE

    while chunk_start <= END_DATE:
        chunk_end = min(chunk_start + relativedelta(years=CHUNK_YEARS) - relativedelta(days=1), END_DATE)
        print(f"  Chunk: {chunk_start.date()} → {chunk_end.date()}")

        df_chunk = fetch_chunk(code, chunk_start, chunk_end)
        if df_chunk is not None:
            chunks.append(df_chunk)
            print(f"  {len(df_chunk)} observações.")

        chunk_start = chunk_end + relativedelta(days=1)

    if not chunks:
        print(f"Nenhuma observação encontrada para a série {code}")
        return None

    df = pd.concat(chunks, ignore_index=True)
    df.drop_duplicates(subset=["data"], inplace=True)
    df.sort_values("data", inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"  Total: {len(df)} observações | {df['data'].min().date()} → {df['data'].max().date()}")
    return df


def aggregate_monthly(df, filename):
    """
    Agrega o câmbio diário para mensal pegando a última cotação do mês.
    A data é normalizada para o primeiro dia do mês para facilitar
    merges com IPCA e CPI, que usam a mesma convenção.
    """
    df_monthly = (
        df.groupby(df["data"].dt.to_period("M"))
        .last()
        .reset_index(drop=True)
    )

    df_monthly["data"] = df_monthly["data"].dt.to_period("M").dt.to_timestamp()
    df_monthly.rename(columns={"valor": "valor_ultimo_mes"}, inplace=True)
    df_monthly.to_csv(filename, index=False)

    print(f"Salvo: {filename} ({len(df_monthly)} linhas) | "
          f"{df_monthly['data'].min().date()} → {df_monthly['data'].max().date()}")


# ── Execução ──────────────────────────────────

# IPCA — série mensal, sem chunking
df_ipca = fetch_bacen_series("433", "IPCA (% mensal)", daily=False)
if df_ipca is not None:
    df_ipca.sort_values("data", inplace=True)
    df_ipca.to_csv("ipca.csv", index=False)
    print(f"Salvo: ipca.csv ({len(df_ipca)} linhas) | "
          f"{df_ipca['data'].min().date()} → {df_ipca['data'].max().date()}")

# Câmbio USD/BRL — série diária, com chunking
df_cambio = fetch_bacen_series("1", "Câmbio USD/BRL (PTAX diário)", daily=True)
if df_cambio is not None:
    df_cambio.to_csv("usd_brl_daily.csv", index=False)
    print(f"Salvo: usd_brl_daily.csv ({len(df_cambio)} linhas)")

    print(f"\nAgregando câmbio para mensal (última cotação do mês)...")
    aggregate_monthly(df_cambio, "usd_brl_monthly.csv")
