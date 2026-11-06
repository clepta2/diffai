# REGRAS DO PROJETO DIFFAI (AGENTIC GUIDELINES)

## 1. IDENTIDADE E PROPÓSITO
- **DiffAI** é o ecossistema autônomo de Inteligência Artificial com auto-aprendizado, orquestração multi-agente hiper-veloz e motor gráfico PhotoCraft em RAM.
- O copilot oficial chama-se **Hermes AI Pro** / **DiffAI**.

## 2. BACKEND E BANCO DE DADOS: SUPABASE EXCLUSIVO
- **O único backend e banco de dados oficial do projeto é o Supabase (PostgreSQL + Realtime + Supabase Auth).**
- Todas as tabelas sensíveis utilizam **Row Level Security (RLS)** ativo com isolamento por `company_id`.
- NUNCA citar, sugerir ou utilizar Firebase/Firestore.

## 3. ORQUESTRAÇÃO & GASTO DE TOKENS
- Sempre que carregar contexto de habilidades (skills), utilize o Lazy Loader [core/load-skills.js](file:///core/load-skills.js).
- NUNCA copie dezenas de arquivos de skills inteiros no prompt. Utilize `loadSkills.getPromptInjection()` bounded por limite de tokens (300 a 600 tokens).
- Para tarefas de alta latência, dispare corridas especulativas (`speculativeRace`) que abortam modelos mais lentos no primeiro token.

## 4. IMAGENS & PHOTOCRAFT
- O motor de edição de imagens oficial é o **PhotoCraft** (Rust + Web Studio Canvas/WebGL).
- Imagens são renderizadas e tratadas com curvas, nitidez e camadas na memória RAM do navegador do usuário, com gravação no **rmount (5TB)** sob a pasta do tenant.

