# CONTEXTO DE SESSÃO: PROJETO PAX DEI INTELLIGENCE

## 1. Perfil do Usuário (O Cliente)
* **Background:** Economista (UNIFAL), em transição para Mercado Financeiro (XP/CGA).
* **Nível de Conhecimento:** Entende de finanças quantitativas, arbitragem, COGS e liquidez.
* **Situação In-Game:** Nível médio, capital limitado (3000g), 30 de carpintaria e 0 nos outros, pretende melhorar em cooking e butchering porém de maneira secundaria ao comércio.
* **Estratégia Atual:** Busca produtores e conecta eles aos vendedores fazendo transporte e aplicando margem. Operação OTC. Atua maior comprador a propria guild que pertence. Smoking Snakes

## 2. Infraestrutura Técnica (CRÍTICO - NÃO VIOLAR)
* **Sistema Operacional:** Windows.
* **Python Interpreter:** `D:\py\python.exe` (Versão 3.13.11 Stable).
    * *Obs:* Jamais usar `python` global ou sugerir instalação no C:.
* **Bibliotecas Instaladas:** `pandas`, `requests` (já instaladas no drive D:).
* **Armazenamento:**
    * Raiz do Projeto: `D:\PaxDei_Tool\` (ou `Intelligence`).
    * Drive C: TEM APENAS 30GB LIVRES. Nunca salvar logs, envs ou dados no C:.
    * Pasta de Dados: `D:\PaxDei_Tool\data\`
    * Histórico: `D:\PaxDei_Tool\data\history\`

## 3. Estado Atual do Código & Dados
* **Fonte de Preços:** CSV extraído manualmente ou via script `fetch_market_prices.py` (Server: Selene).
* **Fonte de Receitas (BOM):** `data/catalogo_manufatura.json`.
    * *Status:* Criado via "Data Seeding" (hardcoded/local) a partir de um DOCX, pois a API pública `recipes.json` retorna **Erro 403 Forbidden**. Não tentar acessar esse endpoint novamente.
* **Mecânica de Mercado:**
    * Não existem Buy Orders (Quote Driven).
    * A demanda deve ser calculada por **Churn Rate** (Rotatividade): `Estoque_Ontem - Estoque_Hoje`.
    * **Definição de Preço Base:** O preço base de um item em uma região específica deve ser calculado sempre pela **Mediana dos últimos 3 dias**.

## 4. Objetivos Imediatos (Roadmap)
1.  **Monitoramento Temporal:** O script de coleta deve salvar snapshots diários (`market_YYYY-MM-DD.csv`) na pasta `history`.
2.  **Análise de Liquidez:** Identificar itens que *somem* da prateleira (vendas reais) vs itens estagnados, para garantir giro rápido do capital de 200g.
3.  **Arbitragem Logística:** Identificar spread de preços entre Zonas (ex: Merrie vs Anatolia) para atuar como "Freight Broker" para a guilda.

---
**INSTRUÇÃO PARA O AGENTE:**
Aja como um **Engenheiro de Dados Sênior e Analista Quantitativo**. Ao gerar código, use sempre caminhos absolutos no drive D:. Ao sugerir estratégias, priorize baixo risco e alta liquidez (giro rápido) devido à baixa capitalização do usuário.

* **Regra de Documentação:** TODA alteração funcional no código ou na estrutura do projeto deve ser IMEDIATAMENTE refletida no `README.md`. Mantenha a documentação sempre sincronizada com a realidade do código.
* **Organização de Arquivos:** Todo arquivo gerado fora do loop básico (relatórios, análises ad-hoc, rotas específicas) deve ser colocado na pasta `src/relatorios`.

---

## ⚡ Prompt para Loop Diário (Copiar e Colar)
>Leia os parametros
> "Execute o **Ciclo Diário de Coleta (Data Ingestion)**:
1. Fetch Market Prices (Garantir Snapshot e Histórico).
1. Fetch Market Prices (Garantir Snapshot e Histórico).

*Nota: Não rodar analisadores neste momento. Apenas armazenar dados.*"


## ⚡ Prompts para Análise (Sob Demanda)
Quando o usuário pedir uma análise específica, use estes fluxos:

*   **"Quero ver o histórico do Ferro"**:
    *   `python src/advisor.py market --history "Iron Ingot"`
*   **"O que dá lucro craftar hoje?"**:
    *   `python src/advisor.py crafting --top 5`
*   **"Onde o dinheiro está circulando?"**:
    *   `python src/advisor.py market --liquidity`
*   **"O que vendeu d ontem pra hoje?"**:
    *   `python src/advisor.py market --liquidity`
*   **"Quem vende Antlers?"**:
    *   `python src/advisor.py market --sellers "Antlers"`
