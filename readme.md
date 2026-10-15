# Projeto Openflow - Ingestão PostgreSQL → Snowflake

## Guia Orientador

Este arquivo é o documento principal de referência do projeto. Deve ser lido no início de cada conversa e enriquecido conforme o projeto evolui.

## Objetivo

Ingerir via carga full algumas tabelas do PostgreSQL para o Snowflake utilizando Openflow com conector custom PostgreSQL.

## Skills Requeridas

- Engenheiro de Dados
- DBA PostgreSQL
- Especialista Snowflake
- Especialista Openflow

## Infraestrutura Snowflake (Criada)

| Objeto | Nome | Status |
|--------|------|--------|
| Role | `OPENFLOW_INGEST_ROLE` | Criado |
| User | `OPENFLOW_INGEST_USER` | Criado |
| Warehouse | `OPENFLOW_INGEST_WH` | XSMALL, 60s autosuspend, standard |
| Database | `OPENFLOW_DB` | Criado |
| Network Rule | `POSTGRES_OPENFLOW_NETWORK_RULE` | Egress 19.58.185.205:5432 |
| EAI | `POSTGRES_OPENFLOW_EAI` | Anexada ao runtime |

## Openflow (Criado)

| Objeto | Nome | Status |
|--------|------|--------|
| Deployment | `OPENFLOW_DEPLOY` | Gen 2, Snowflake, ACTIVE |
| Runtime | `OPENFLOW_DB.PUBLIC.OPENFLOW_INGEST_RT` | Small S1, ACTIVE |
| Custom Flow | PostgreSQL Full Load | 3 processors, VALID |

## Configuração do Conector

- Fonte: PostgreSQL (ver `config_postgres.md` para detalhes de conexão)
- Tipo de carga: Full Load via custom NiFi flow
- Detalhes do flow: ver `openflow_full_load.md`

## Status

- Infraestrutura completa
- Flow criado e validado
- **PENDENTE:** Conexão ao PostgreSQL recusada — verificar servidor PostgreSQL

## Arquivos de Referência

- `config_postgres.md` — Configuração de conexão ao PostgreSQL
- `openflow_full_load.md` — Detalhes do flow, IDs, comandos nipyapi
- `prompts.txt` — Instruções originais do projeto
