# Pax Dei Market Analysis Tool

A Python-based toolset for analyzing crafting profitability in the game *Pax Dei*. This project automates the process of fetching market prices, building a recipe catalog, and calculating the profit margins (spread) for various crafting recipes.

## 📁 Project Structure

d:/PaxDei_Tool/
├── src/                # Core analysis logic
│   ├── advisor.py      # Unified CLI Entry Point
│   └── modules/        # Helper libraries (market, crafting, logistics)
├── etl/                # Data extraction and transformation scripts
│   ├── fetch_market_prices.py  # Scrapes/fetches latest market prices
│   └── build_recipe_catalog.py # Builds the JSON catalog of crafting recipes
├── data/               # Data storage (input/output)
│   ├── catalogo_manufatura.json # Generated recipe catalog
│   ├── selene_latest.parquet    # Fetched market prices
│   ├── client_orders.csv        # [NEW] Tracking de pedidos de clientes (Renamed)
│   ├── suppliers.csv            # [NEW] Registro de fornecedores e preços (Manual)
│   ├── history/                 # Daily Snapshots
│   └── analise_disparidade.csv  # Final reports
└── temp/               # Temporary files

## 🚀 Getting Started

### Prerequisites

- **Python 3.x** installed on your system.
- **pandas** library.

### Installation

1.  Clone or download this repository.
2.  Install the required Python dependencies:

    ```bash
    pip install pandas
    ```

## 🛠️ Usage Guide

The workflow consists of two main stages: **Data Collection** (ETL) and **Analysis**.

### Step 1: Data Collection

Before running the analysis, you need to ensure you have the latest data.

1.  **Fetch Market Prices**:
    Run the price fetcher to update `data/selene_latest.parquet` and history snapshots.
    ```bash
    python etl/fetch_market_prices.py
    ```

2.  **Build Recipe Catalog**:
    Run the catalog builder to update `data/catalogo_manufatura.json`.
    ```bash
    python etl/build_recipe_catalog.py
    ```

### Step 2: Consultas e Inteligência (Unified Advisor)

Utilize o novo CLI unificado `src/advisor.py` para todas as análises.

#### 1. Inteligência de Mercado
*   **Histórico de Item (Preço/Estoque):**
    ```bash
    python src/advisor.py market --history "Nome do Item"
    ```
*   **Liquidez (O que vendeu recentemente):**
    ```bash
    python src/advisor.py market --liquidity
    ```
*   **Top Produtores (Quem vende mais):**
    ```bash
    python src/advisor.py market --sellers "Nome do Item"
    ```

#### 2. Crafting e Manufatura
*   **Lucratividade (Spread):**
    ```bash
    python src/advisor.py crafting --top 10
    ```

#### 3. Logística e Arbitragem
*   **Encontrar Rotas:**
    ```bash
    python src/advisor.py logistics --route "Origem" "Destino"
    ```
*   **Oportunidades de Arbitragem:**
    ```bash
    python src/advisor.py logistics --arbitrage
    ```

#### 4. Gestão de Clientes (Brokerage)
*   **Excel de Demandas:** Edite manualmente o arquivo `data/clientes_demandas.csv`.
*   **Verificar Oportunidades:** *(Em Breve)* Cruzamento automático de demandas x ofertas do mercado.

## 🔄 Ciclo Diário de Execução

Para garantir que você (Investidor) tenha sempre a melhor inteligência de mercado:

1.  **Coleta de Dados (Data Gathering)**:
    *   Execute `python etl/fetch_market_prices.py`.
    *   *Ação:* Conecta ao servidor, baixa os preços atuais e salva o snapshot em `data/history`.
    *   *Objetivo:* Apenas garantir que o histórico está sendo alimentado diariamente.

    *   *Objetivo:* Apenas garantir que o histórico está sendo alimentado diariamente.

    *(Nota: A atualização de receitas `build_recipe_catalog.py` só deve ser feita após Patches do jogo).*

**Dica de Ouro:** Execute a coleta em horários de pico (manhã e noite) para capturar a volatilidade e vendas rápidas. Utilize a informação de `Top_Zone` no relatório de liquidez para fazer arbitragem de transporte.

**Output**:
- The script will print the **Top 5 Most Profitable** and **Top 5 Biggest Loss** items to the console.
- A full detailed report is saved to `data/analise_disparidade.csv`.
- Liquidity report with regional data is saved to `data/liquidez_diaria.csv`.

## 📊 Understanding the Output

### Profitability (`analise_disparidade.csv`)
- **Produto**: The crafted item name.
- **Custo_Manufatura**: Total cost of ingredients (based on lowest market prices).
- **Preco_Venda**: Current lowest selling price of the item.
- **Spread**: Profit amount (`Preco_Venda - Custo_Manufatura`).
- **Margem_Perc**: Profit margin percentage.
- **Mercado_Venda**: The specific market/zone where the item is sold.
- **Sourcing_Insumos**: Details on where to buy the cheapest ingredients.

### Liquidity (`liquidez_diaria.csv`)
- **Item**: Item name.
- **Units_Sold**: Number of listings that disappeared (sold/expired).
- **Total_Volume**: Total value of sold listings.
- **Top_Zone**: The zone with the highest volume of sales for this item.
- **Top_Zone_Sales**: Count of sales in that specific zone.

## 🔎 Consultando Histórico de Itens

Você pode usar o histórico de preços para analisar a tendência de qualquer item ao longo do tempo.

**Comando:**
```bash
python src/market_intelligence.py --item "Nome do Item"
```

**Exemplo (Consultar Iron Ingot):**
```bash
python src/market_intelligence.py --item "Iron Ingot"
```

**O relatório exibe:**
- **Min/Avg Price**: Evolução do preço.
- **Stock_Count**: Quantidade de ofertas ativas (oferta x demanda).
- **Units_Sold**: Quantidade estimada de vendas entre os snapshots.
