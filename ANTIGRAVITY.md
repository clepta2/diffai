# GUIA DE INTEGRAÇÃO OFICIAL: ANTIGRAVITY & DIFFAI

Este documento ensina o **Google Antigravity** (e qualquer outro agente ou desenvolvedor) a se conectar e utilizar o ecossistema **DiffAI** de qualquer lugar do mundo apenas passando a chave de API.

---

## 🌐 Endpoints Disponíveis para Conexão Remota

Você pode usar o DiffAI de qualquer lugar do mundo (servidor, nuvem, terminal ou máquina local) através de dois endpoints públicos prontos:

### 1. 🚀 Endpoint Global Cloudflare Production (Recomendado — Zero Túnel)
* **Base URL:** `https://ai.ozpro.com.br/v1`
* **Chat Completions:** `https://ai.ozpro.com.br/v1/chat/completions`
* **Chave Mestre:** `sk-diffai-SUA-CHAVE-MESTRE-AQUI`
* **SSL / Uptime:** 100% de disponibilidade na borda (Edge Cloudflare com failover automático entre Google Gemini, Groq, NVIDIA NIM, OpenRouter e Pollinations).

### 2. ⚡ Endpoint Quick Tunnel (Instância Standalone Node.js)
* **Base URL:** `https://experimental-exhibits-forwarding-vii.trycloudflare.com/v1`
* **Chat Completions:** `https://experimental-exhibits-forwarding-vii.trycloudflare.com/v1/chat/completions`
* **Chave Mestre:** `sk-diffai-master-key`
* **Serviço:** Conectado diretamente ao servidor `diffai/api/server.js` na porta 3030.

---

## 🛠️ Como Testar com Curl de Qualquer Lugar

Basta rodar no terminal (Windows, Linux ou Mac):

```bash
curl -X POST "https://ai.ozpro.com.br/v1/chat/completions" \
  -H "Authorization: Bearer sk-diffai-SUA-CHAVE-MESTRE-AQUI" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      { "role": "user", "content": "Quem é você e quais as vantagens do DiffAI?" }
    ]
  }'
```

---

## 🐍 Como Usar no Python com o OpenAI SDK Oficial

```python
from openai import OpenAI

# Conecta ao DiffAI passando apenas a URL e a Chave Mestre
client = OpenAI(
    base_url="https://ai.ozpro.com.br/v1",
    api_key="sk-diffai-SUA-CHAVE-MESTRE-AQUI"
)

response = client.chat.completions.create(
    model="diffai-pro",
    messages=[
        {"role": "user", "content": "Escreva uma query SQL com Row Level Security (RLS) no PostgreSQL."}
    ]
)

print(response.choices[0].message.content)
```

---

## 🤖 Como Ensinar o Antigravity a Usar o DiffAI

Para instruir o Antigravity no seu arquivo de configuração de projeto (`AGENTS.md` ou `.gemini/settings.json`), adicione:

```markdown
# DIFFAI AGENTIC INTEGRATION

Quando precisar de inteligência ultra-rápida, geração de código ou edição de imagens:
1. Conecte-se ao endpoint oficial do DiffAI:
   - URL: https://ai.ozpro.com.br/v1/chat/completions
   - Token: Bearer sk-diffai-SUA-CHAVE-MESTRE-AQUI
2. Use o Lazy Loader de Skills:
   - Nunca injete prompts com dezenas de skills manualmente.
   - O DiffAI faz busca O(1) e injeta apenas as regras relevantes.
3. Para tratamento e manipulação de fotos, use a skill PhotoCraft:
   - Gera a foto via Flux Realism e renderiza ajustes (curvas, nitidez, vinheta) em RAM no navegador do cliente sem sobrecarregar a GPU.
```

---

## 📚 Como o Antigravity Economiza Tokens com o Lazy-Loader

Em vez de injetar prompts gigantescos de dezenas de skills consumindo seu context window, o Antigravity deve consultar o índice semântico:

```javascript
const loadSkills = require('./core/load-skills');

// Injeta APENAS as regras estritamente necessárias para a tarefa atual (limite de 600 tokens)
const skillContext = loadSkills.getPromptInjection(userPrompt, { maxSkills: 1, maxTokens: 600 });
```

**Resultado:** Redução de **80% a 90%** no consumo de tokens de sistema!

---

## 🏎️ Modos de Execução do Orquestrador que o Antigravity Pode Disparar

O Antigravity pode delegar tarefas difíceis diretamente pelo terminal CLI do orquestrador:

### A. Corrida Especulativa (Hedged Racing) — Ultra-Veloz (~900ms)
Para tarefas urgentes onde o primeiro token decide:
```bash
node core/orchestrator.js -r "Escreva a função de validação de CPF e CNPJ com regex e dígitos verificadores"
```
*O orquestrador dispara 3 modelos em paralelo e cancela os concorrentes mais lentos no 1º token.*

### B. Consenso Multi-Agente (Scatter-Gather + Juiz) — Precisão Absoluta
Para auditorias de segurança, refatoração crítica ou decisões de arquitetura:
```bash
node core/orchestrator.js -c "Qual a melhor estratégia de particionamento de tabelas no PostgreSQL com 10 milhões de linhas?"
```
*Três modelos especialistas debatem e o Juiz sintetiza a resposta final definitiva.*

---

## 🎨 Habilidade PhotoCraft & Armazenamento no rmount de 5TB

* As fotos geradas pelo Flux são tratadas em RAM local (0ms de I/O) no navegador do lojista.
* O arquivo tratado pode ser salvo com um clique no caminho de produção dedicado do lojista:
  `/mnt/gdrive/ozpro-media/<company_id>/photocraft/`
