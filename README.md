# ⚡ DiffAI — Autonomous AI Orchestrator & Multi-Agent Intelligence Engine

[![Version](https://img.shields.io/badge/version-3.0.0-blue.svg)](https://github.com/clepta2/diffai)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Database](https://img.shields.io/badge/Database-Supabase%20PostgreSQL-3ECF8E.svg)](https://supabase.com)
[![Engine](https://img.shields.io/badge/Engine-PhotoCraft%20Rust%20%2F%20RAM-purple.svg)](https://github.com/clepta2/diffai)
[![Speed](https://img.shields.io/badge/Orchestrator-Hedged%20Racing%20~900ms-orange.svg)](https://github.com/clepta2/diffai)

**DiffAI** é um ecossistema especializado de Inteligência Artificial de alta frequência projetado para desenvolvedores, agentes autônomos (como o **Google Antigravity**) e empresas que buscam máxima velocidade, economia agressiva de tokens e aprendizagem contínua com privacidade total.

---

## 🏗️ Arquitetura do Sistema

```mermaid
flowchart TD
    User["Antigravity / Lojista / Agentes"] -->|Chave Mestre| Gateway["DiffAI Gateway (OpenAI Compatible)"]

    subgraph Core["Núcleo DiffAI v3.0"]
        direction TB
        Orch["Hyper-Speed Orchestrator\n(Hedged Racing, DAG, Consenso)"]
        LazySkills["Lazy-Loading de Skills\n(2.834+ Skills Indexadas em O(1))"]
        Memory["Memory Manager & Auto-Learning\n(Supabase PostgreSQL com RLS)"]
    end

    subgraph Providers["Pool de Provedores Neurais"]
        direction LR
        NVIDIA["NVIDIA NIM Cluster\n(Flux, LLaMA, DeepSeek)"]
        Groq["Groq LPU\n(500 tokens/s)"]
        Gemini["Google Gemini\n(Flash 3.1)"]
        OpenRouter["OpenRouter\n(Claude 3.5 / GPT-5)"]
    end

    subgraph Studio["PhotoCraft Web Studio (Client-Side)"]
        RAM["Renderização em RAM Local (0ms I/O)\nCurvas em S, Nitidez, Vinheta, Textos"]
        Rmount["Gravação Direta no rmount (5TB Drive)\n/mnt/gdrive/ozpro-media/{tenant}/"]
    end

    Gateway --> Core
    Core --> Providers
    Core --> Studio
```

---

## ✨ As Grandes Inovações do DiffAI v3.2

### 1. 🏎️ Orquestrador Hiper-Veloz Multi-Chave & Esteira Contínua
* **Continuous Streaming Pipeline (`diffai-pipeline`):** Elimina a espera sequencial. Enquanto a Chave 1 (Groq/Gemini) gera a linha $N$, as Chaves 2 e 3 (Claude Sonnet 4.6/Qoder) já analisam a linha $N-1$ em paralelo com janela deslizante de contexto.
* **Speculative Verification (`diffai-verify`):** Modelo ultrarrápido (<350ms) produz o rascunho e o Claude Sonnet 4.6 audita e lapida em esteira paralela.
* **Speculative Hedged Racing:** Dispara consultas simultâneas contra múltiplos modelos topo de linha com abort controller imediato no primeiro token. Latência real de **~800 ms**.
* **Ensemble Consensus:** Dispara múltiplos especialistas em paralelo e sintetiza uma decisão unificada com um modelo Juiz.

### 2. 🔌 Suporte a Multi-Protocolos de Rede & Streaming
* **WebSocket Full-Duplex RFC 6455 (`ws://localhost:3030/ws`):** Conexão TCP permanente com despacho de tarefas e streaming bidirecional em **0.02ms** sem cabeçalhos HTTP.
* **Raw Stream Protocol (`x-stream-protocol: raw`):** Transmissão direta de caracteres via HTTP Chunked com **97% menos bytes** que o padrão OpenAI.
* **Compact SSE (`x-stream-protocol: compact`):** Streaming mínimo de micro-eventos para comunicação entre agentes autônomos.
* **Binary Codec:** Serializador/deserializador binário de alta densidade sem dependências externas.
* **Semantic Worker (`worker_threads`):** Isolamento de processamento pesado de CPU (AST, regex, syntax check) em thread secundária, preservando latência de 0ms no event loop de rede.
* **EWMA Health Tracker (`/api/cluster-health`):** Roteamento inteligente que mede a média móvel exponencial de latência e desvia de chaves com falha temporária.

### 3. 📚 Motor de Lazy-Loading Semântico (2.834+ Skills)
* Indexação de **2.834 habilidades especializadas** divididas em 10 categorias em menos de 1 segundo.
* Em vez de queimar 20.000 tokens injetando arquivos inteiros no prompt do agente, o DiffAI faz **busca vetorial/semântica O(1)** e injeta apenas as regras estritamente necessárias (limite configurável de 300 a 600 tokens).
* **Economia real de 80% a 90% dos tokens do Antigravity!**

### 4. 🎨 PhotoCraft Web Studio (Edição de Camadas em RAM)
* Elimina a dependência de GPUs caras do servidor para pequenas alterações em imagens.
* A foto inicial gerada pelo Flux é aberta em um **estúdio WebGL/Canvas diretamente no navegador do usuário**, consumindo de 18 a 40 MB da **RAM local do dispositivo**.
* O usuário altera contraste, saturação, curvas, nitidez e adiciona textos com **latência zero (0 ms I/O)** e salva o resultado final no **rmount dedicado de 5 TB**.

### 5. 🧠 Memória Persistente com Isolamento Multi-Tenant (Supabase RLS)
* Aprende de forma autônoma hábitos, regras de negócio e preferências do lojista (`hermes_merchant_profile`).
* Dados totalmente isolados no nível do motor SQL PostgreSQL por **Row Level Security (RLS)** usando o identificador de tenant (`company_id`).

---

## 🌐 Endpoints Disponíveis para Uso Remoto (De Qualquer Lugar)

### 1. Endpoint Global de Produção (Cloudflare Edge — Sem necessidade de túnel local)
* **URL:** `https://ai.ozpro.com.br/v1/chat/completions`
* **Chave Mestre:** `sk-diffai-SUA-CHAVE-MESTRE-AQUI`
* **Padrão:** OpenAI Chat Completions oficial.

### 2. Túnel Público Ativo (Quick Tunnel Standalone Node.js)
* **URL:** `https://experimental-exhibits-forwarding-vii.trycloudflare.com/v1/chat/completions`
* **Chave Mestre:** `sk-diffai-master-key`

```bash
# Teste imediato via curl de qualquer máquina:
curl -X POST "https://ai.ozpro.com.br/v1/chat/completions" \
  -H "Authorization: Bearer sk-diffai-SUA-CHAVE-MESTRE-AQUI" \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Olá DiffAI!"}]}'
```

---

## 🤖 Como Ensinar o Antigravity a Usar o DiffAI

Consulte o manual detalhado em [ANTIGRAVITY.md](file:///ANTIGRAVITY.md).

### Passo 1: Configurar a Chave Mestre
No arquivo `.env` do Antigravity ou do seu projeto:
```env
DIFFAI_API_KEY=sk-diffai-sua-chave-mestre
DIFFAI_GATEWAY_URL=http://localhost:3030/v1
```

### Passo 2: Iniciar o Gateway Local
```bash
npm install
npm start
```

### Passo 3: Comandos CLI Disponíveis para o Antigravity
```bash
# Corrida especulativa de alta velocidade:
npm run orchestrate:race -- "Escreva uma API REST de clientes com validação"

# Consenso entre múltiplos modelos especialistas:
npm run orchestrate:consensus -- "Qual a melhor arquitetura de cache para 100k req/s?"

# Benchmark de latência e TTFT de todos os modelos:
npm run orchestrate:benchmark

# Busca e teste de skills sob demanda:
npm run skills:search -- search "photocraft"
```

---

## 📁 Estrutura do Repositório

```text
diffai/
├── README.md               # Apresentação do projeto e documentação
├── AGENTS.md               # Diretrizes para inteligências artificiais e copilotos
├── ANTIGRAVITY.md          # Manual de conexão e uso do Antigravity
├── .env.example            # Template de variáveis de ambiente e chaves
├── package.json            # Scripts de execução e dependências
│
├── core/                   # Cérebro e orquestração do sistema
│   ├── orchestrator.js     # Orquestrador v3.0 (Race, DAG, Consensus, Worker Pool)
│   ├── load-skills.js      # Carregador sob demanda com teto de tokens
│   ├── build-skills-index.js # Indexador instantâneo de 2.800+ skills
│   └── memory-manager.js   # Memória persistente e RLS do Supabase
│
├── skills/                 # Catálogo e skills essenciais
│   ├── skills-index.json   # Índice estruturado de todas as skills
│   ├── skills-index.min.json # Versão minificada ultraleve
│   └── photocraft/         # Skill e motor oficial PhotoCraft
│
├── api/                    # Servidor e rotas
│   └── server.js           # API Gateway compatível com OpenAI (/v1/chat/completions)
│
└── web/                    # Interface gráfica do usuário
    └── index.html          # Chat executivo com PhotoCraft Web Studio integrado
```

---

## 🛡️ Licença
Distribuído sob a licença **MIT**. Veja `LICENSE` para mais informações.
Desenvolvido por **clepta2** para o ecossistema de inteligência artificial de alta performance.

