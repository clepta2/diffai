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

## 5. ARQUITETURA DO CLUSTER DE IA E DIRETRIZ PERMANENTE: PRIORIZAR SEMPRE MODELOS 100% FREE
- **Host Central de IA:** Servidor Linux VM (`ozpro1` @ `100.98.113.39`), usuário `ozpro`.
- **OmniRouter Central:** Roda na porta `20128` da VM (`http://100.98.113.39:20128`).
- **Orquestrador DiffAI Local:** Roda na porta `3030` (`http://127.0.0.1:3030`), repassando as requisições para a VM via rotação rápida.
- **Banco de Dados de Conexões da VM:** `/home/ozpro/.9router/db/data.sqlite` contendo mais de 80 contas em pool ativo.
- **Auto-Cura e Resiliência Nativa (Boot/Shutdown/Cron):**
  - **No Desligamento (`shutdown.target`):** Serviço systemd `ozpro-shutdown.service` executa `pm2 save` e checkpoint WAL no SQLite do OmniRouter, prevenindo qualquer perda ou corrupção de dados ao desligar a VM.
  - **No Início (`@reboot` Cron Nativo):** Script `/home/ozpro/scripts/boot_healing.sh` garante ZRAM ativo (5.7GB), swappiness=10, Tailscale conectado e executa o `provider_guardian.py` para destravamento instantâneo de contas.
  - **A cada 2 horas (`0 */2 * * *`):** Cron nativo executa auto-cura periódica contínua, destravando contas em backoff e atualizando `/home/ozpro/health/best_models.json`.

### MODELOS OFICIAIS TESTADOS E HOMOLOGADOS (STATUS 200 OK):
1. **Inteligência Máxima & Raciocínio (Claude):**
   - `ag/claude-sonnet-4-6` (1.15s – rotação de 10 contas Antigravity)
   - `ag/claude-opus-4-6-thinking` (1.56s – raciocínio encadeado)
   - `kr/claude-sonnet-4.5` (1.92s – backup redundante via Kiro)
2. **Velocidade Pura (Sub-segundo):**
   - `groq/openai/gpt-oss-120b` e `groq/llama-3.3-70b-versatile` (330ms – pool de 6 chaves Groq ativas 100% free)
   - `ag/gpt-oss-120b-medium` (700ms)
3. **Programação & Engenharia de Software:**
   - `qd/qfmodel` (2.1s – pool de 9 contas Qoder ativas na VM)
   - `kr/deepseek-3.2` (4.8s – DeepSeek ativo via Kiro)
   - `kc/deepseek-r1:free` e `kc/qwen/qwen-2.5-coder-32b-instruct:free` (rotas 100% free via Kilo Code)
4. **Volume Contínuo & Flash:**
   - `gemini/gemini-3.5-flash-lite` (790ms – pool de 16 chaves Gemini AI Studio 100% free)
   - `gemini/gemini-2.5-flash` e `gemini/gemini-3.1-flash-lite-preview` (5.2s)
5. **Modelos de Grande Porte na Nuvem (NVIDIA NIM):**
   - `google/gemma-4-31b-it` (1.9s – pool de 6 chaves NVIDIA ativas)
   - `meta/llama-3.2-90b-vision-instruct` (visão computacional de alta resolução)

### REGRAS PERMANENTES OBRIGATÓRIAS (ZERO RISCO DE COBRANÇA OU ERRO):
- **Toda e qualquer rota de IA do ecossistema DEVE priorizar SEMPRE modelos 100% gratuitos.**
- **Kilo Code (`kc/*`):** Chamar SEMPRE e EXCLUSIVAMENTE a rota gratuita com sufixo `:free` (`kc/deepseek-r1:free`, `kc/meta-llama/llama-3.3-70b-instruct:free`, `kc/qwen/qwen-2.5-coder-32b-instruct:free`). NUNCA chamar modelos sem `:free` para evitar HTTP 402 (Saldo Zero). O orquestrador adiciona automaticamente `:free` se omitido.
- **Kiro (`kr/*`):** Utilizar `kr/claude-sonnet-4.5` e `kr/deepseek-3.2` como rotas gratuitas de alta estabilidade e backup prioritário.
- **SambaNova:** Evitar chamadas diretas até liberação sem cartão (retorna HTTP 402 Payment Required).
- **NUNCA chamar `gemini-4-argon`:** Retorna HTTP 429 por cota zero no AI Studio sem faturamento GCP.

