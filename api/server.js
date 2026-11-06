/**
 * DiffAI — Standalone AI Gateway & Agentic Orchestrator Server (api/server.js)
 * Compatível com o padrão OpenAI (/v1/chat/completions) para Antigravity e outros agentes.
 */

const http = require('http');
const path = require('path');
const fs = require('fs');

// Carrega .env nativamente se existir
const envPath = path.resolve(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx > 0) {
        const k = trimmed.slice(0, eqIdx).trim();
        const v = trimmed.slice(eqIdx + 1).trim().replace(/^['"]|['"]$/g, '');
        if (!process.env[k]) process.env[k] = v;
      }
    }
  }
}
const MemoryManager = require('../core/memory-manager');
const loadSkills = require('../core/load-skills');
const orchestrator = require('../core/orchestrator');
const {
  speculativeRace,
  runSequentialPipeline,
  runContinuousPipeline,
  runSpeculativeVerification,
  runEnsembleConsensus,
  getMemoryReport,
  clearOrchestratorMemory,
  getModelHealthReport
} = orchestrator;
const { setupWebSocketServer } = require('../core/websocket-server');
const BinaryCodec = require('../core/binary-codec');
const KeepWarmEngine = require('../core/keep-warm');

const PORT = parseInt(process.env.PORT || '3030', 10);
const MASTER_KEY = process.env.DIFFAI_API_KEY || process.env.MASTER_API_KEY || 'sk-diffai-master-key';
const memory = new MemoryManager();
const keepWarm = new KeepWarmEngine();
keepWarm.start();

function sendJson(res, statusCode, data) {
  const payload = Buffer.from(JSON.stringify(data), 'utf8');
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': payload.length,
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-company-id, x-tenant-id'
  });
  res.end(payload);
}

function verifyAuth(req) {
  const auth = req.headers['authorization'] || '';
  const token = auth.replace(/^Bearer\s+/i, '').trim();
  if (!token) return false;
  const validKeys = [MASTER_KEY, process.env.DIFFAI_API_KEY, process.env.OMNIROUTER_API_KEY].filter(Boolean);
  return validKeys.includes(token);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        resolve({});
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  // CORS Preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-company-id, x-tenant-id'
    });
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = parsedUrl.pathname;

  // Health & Memória
  if (pathname === '/health' || pathname === '/') {
    sendJson(res, 200, {
      ok: true,
      service: 'DiffAI Intelligence Engine',
      version: '3.1.0',
      totalSkills: loadSkills?.index?.totalSkills || 2834,
      orchestratorModes: ['speculative-racing', 'sequential-dag', 'ensemble-consensus', 'continuous-pipeline'],
      memory: getMemoryReport()
    });
    return;
  }

  // Monitor de Memória & Limpeza de Garbage
  if (pathname === '/api/memory-status') {
    sendJson(res, 200, { ok: true, memory: getMemoryReport() });
    return;
  }
  if (pathname === '/api/memory-clear' && req.method === 'POST') {
    clearOrchestratorMemory();
    sendJson(res, 200, { ok: true, message: 'Cache e memória temporária limpos com sucesso.', memory: getMemoryReport() });
    return;
  }
  // Monitor de Latência EWMA do Cluster
  if (pathname === '/api/cluster-health') {
    sendJson(res, 200, { ok: true, cluster: getModelHealthReport(), memory: getMemoryReport() });
    return;
  }

  // Monitor e Controle do Keep-Warm Engine (Zero Cold-Start)
  if (pathname === '/api/keep-warm') {
    sendJson(res, 200, { ok: true, keepWarm: keepWarm.getStatus() });
    return;
  }
  if (pathname === '/api/keep-warm/start' && req.method === 'POST') {
    keepWarm.start();
    sendJson(res, 200, { ok: true, message: 'Keep-Warm ativado.', keepWarm: keepWarm.getStatus() });
    return;
  }
  if (pathname === '/api/keep-warm/stop' && req.method === 'POST') {
    keepWarm.stop();
    sendJson(res, 200, { ok: true, message: 'Keep-Warm pausado.', keepWarm: keepWarm.getStatus() });
    return;
  }

  // Modelos disponíveis (OpenAI Standard)
  if (pathname === '/v1/models') {
    if (!verifyAuth(req)) {
      sendJson(res, 401, { error: { message: 'Chave de API DiffAI inválida ou ausente.', type: 'invalid_request_error' } });
      return;
    }
    sendJson(res, 200, {
      object: 'list',
      data: [
        { id: 'diffai-pro', object: 'model', created: Date.now(), owned_by: 'diffai' },
        { id: 'diffai-pipeline', object: 'model', created: Date.now(), owned_by: 'diffai' },
        { id: 'diffai-verify', object: 'model', created: Date.now(), owned_by: 'diffai' },
        { id: 'diffai-racing', object: 'model', created: Date.now(), owned_by: 'diffai' },
        { id: 'diffai-consensus', object: 'model', created: Date.now(), owned_by: 'diffai' },
        { id: 'claude-sonnet-4.6', object: 'model', created: Date.now(), owned_by: 'anthropic' },
        { id: 'deepseek-r1', object: 'model', created: Date.now(), owned_by: 'deepseek' },
        { id: 'gemini-3.1-flash', object: 'model', created: Date.now(), owned_by: 'google' }
      ]
    });
    return;
  }

  // Chat Completions (OpenAI Compatible + Antigravity Gateway)
  if (req.method === 'POST' && (pathname === '/v1/chat/completions' || pathname === '/api/diffai-chat')) {
    if (!verifyAuth(req)) {
      sendJson(res, 401, { error: { message: 'Chave de API DiffAI inválida ou ausente.', type: 'invalid_request_error' } });
      return;
    }

    try {
      const body = await readBody(req);
      const tenantId = req.headers['x-company-id'] || req.headers['x-tenant-id'] || body.companyId || 'default_tenant';
      const messages = body.messages || [];
      const userPrompt = messages.length > 0 ? (messages[messages.length - 1].content || '') : (body.prompt || '');

      // 1. Injeção de Contexto & Memória RLS
      const tenantCtx = await memory.getTenantContext(tenantId);
      memory.learn(tenantId, userPrompt).catch(() => {});

      // 2. Injeção Dinâmica de Skills (Lazy Loading economizador de tokens)
      const skillInjection = loadSkills ? loadSkills.getPromptInjection(userPrompt, { maxSkills: 2, maxTokens: 800 }) : '';

      // 3. Execução pelo Orquestrador com Suporte a Multi-Protocolo (Raw, Compact SSE, OpenAI)
      const isStream = Boolean(body.stream);
      const streamProto = (req.headers['x-stream-protocol'] || (req.headers['accept'] === 'text/plain' ? 'raw' : 'openai')).toLowerCase();
      let replyText = '';
      const mode = body.mode || (body.model === 'diffai-consensus' ? 'consensus' : (body.model === 'diffai-verify' ? 'verify' : (body.model === 'diffai-pipeline' ? 'pipeline' : (body.model === 'diffai-racing' ? 'race' : 'fast'))));

      // Inicia Stream de acordo com o protocolo solicitado
      if (isStream) {
        if (streamProto === 'raw') {
          res.writeHead(200, {
            'Content-Type': 'text/plain; charset=utf-8',
            'Transfer-Encoding': 'chunked',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
          });
        } else {
          res.writeHead(200, {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
          });
        }
      }

      const sendChunk = (token) => {
        if (!isStream || !token) return;
        if (streamProto === 'raw') {
          res.write(token);
        } else if (streamProto === 'compact') {
          res.write(`data: ${token}\n\n`);
        } else {
          const chunk = JSON.stringify({
            id: 'chatcmpl-' + Date.now(),
            object: 'chat.completion.chunk',
            created: Math.floor(Date.now() / 1000),
            model: 'DiffAI Pro',
            choices: [{ index: 0, delta: { content: token }, finish_reason: null }]
          });
          res.write(`data: ${chunk}\n\n`);
        }
      };

      const finishStream = () => {
        if (!isStream) return;
        if (streamProto === 'openai' || streamProto === 'compact') {
          res.write('data: [DONE]\n\n');
        }
        res.end();
      };

      if (mode === 'pipeline') {
        // Modo Esteira Contínua: Produtor (Groq) e Consumidor (Claude) simultâneos
        if (isStream) {
          await runContinuousPipeline({
            taskPrompt: userPrompt,
            producerModel: 'groq/openai/gpt-oss-120b',
            consumerModel: 'ag/claude-sonnet-4-6',
            consumerConcurrency: 2,
            onProducerUnit: (idx, line) => {
              sendChunk(`\n[Passo #${idx}]: ${line}\n`);
            },
            onConsumerUnitComplete: (item) => {
              sendChunk(`  -> Análise Concorrente: ${item.processed.trim()}\n`);
            }
          });
          finishStream();
          return;
        } else {
          const pipeRes = await runContinuousPipeline({
            taskPrompt: userPrompt,
            producerModel: 'groq/openai/gpt-oss-120b',
            consumerModel: 'ag/claude-sonnet-4-6',
            consumerConcurrency: 2
          });
          replyText = pipeRes.results.map(r => `[Passo #${r.index}]: ${r.source}\n-> Análise: ${r.processed}`).join('\n\n');
        }
      } else if (mode === 'verify') {
        // Modo Speculative Verification: Groq gera rascunho rápido + Claude audita
        if (isStream) {
          await runSpeculativeVerification(userPrompt, {
            onDraftToken: (t) => sendChunk(t)
          });
          finishStream();
          return;
        } else {
          const verifyRes = await runSpeculativeVerification(userPrompt);
          replyText = verifyRes.reply;
        }
      } else if (mode === 'race') {
        if (isStream) {
          await speculativeRace(userPrompt, ['ag/claude-sonnet-4-6', 'ag/claude-opus-4-6-thinking', 'ag/gpt-oss-120b-medium'], sendChunk);
          finishStream();
          return;
        } else {
          const raceRes = await speculativeRace(userPrompt, ['ag/claude-sonnet-4-6', 'ag/claude-opus-4-6-thinking', 'ag/gpt-oss-120b-medium']);
          replyText = raceRes.reply || raceRes.content || 'Resposta concluída via Hedged Race.';
        }
      } else if (mode === 'consensus') {
        const conRes = await runEnsembleConsensus(userPrompt);
        replyText = conRes.synthesis || conRes.consensusReply || 'Consenso multimodelo concluído.';
        if (isStream) {
          sendChunk(replyText);
          finishStream();
          return;
        }
      } else {
        // Modo Padrão: Prompt Enriquecido com Memória e Skill
        const enrichedPrompt = `${skillInjection ? `[Skills Ativas]\n${skillInjection}\n\n` : ''}${tenantCtx.textBlock}\n\nPergunta do Lojista: ${userPrompt}`;
        if (isStream) {
          await speculativeRace(enrichedPrompt, ['ag/claude-sonnet-4-6', 'ag/gpt-oss-120b-medium'], sendChunk);
          finishStream();
          return;
        } else {
          const defaultRace = await speculativeRace(enrichedPrompt, ['ag/claude-sonnet-4-6', 'ag/gpt-oss-120b-medium']);
          replyText = defaultRace.reply || defaultRace.content || 'Resposta gerada com sucesso.';
        }
      }

      // Resposta no formato compatível OpenAI
      sendJson(res, 200, {
        id: 'chatcmpl-' + Date.now(),
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: 'DiffAI Pro (Hermes AI)',
        choices: [{
          index: 0,
          message: { role: 'assistant', content: replyText },
          finish_reason: 'stop'
        }],
        usage: {
          prompt_tokens: 180,
          completion_tokens: Math.round(replyText.length / 4),
          total_tokens: 180 + Math.round(replyText.length / 4)
        }
      });
    } catch (err) {
      console.error('[DiffAI Server Error]', err);
      sendJson(res, 500, { error: { message: err.message || 'Erro interno no servidor DiffAI.' } });
    }
    return;
  }

  // Frontend estático
  if (req.method === 'GET' && (pathname === '/chat' || pathname === '/app')) {
    const htmlPath = path.resolve(__dirname, '../web/index.html');
    if (fs.existsSync(htmlPath)) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      fs.createReadStream(htmlPath).pipe(res);
      return;
    }
  }

  sendJson(res, 404, { error: 'Rota não encontrada.' });
});

// Anexa o Servidor WebSocket RFC 6455 Nativo
setupWebSocketServer(server, orchestrator, { masterKey: MASTER_KEY });

server.listen(PORT, () => {
  console.log(`\n🚀 DiffAI Intelligence Engine v3.2 iniciado na porta ${PORT}`);
  console.log(`📡 Endpoint OpenAI Gateway: http://localhost:${PORT}/v1/chat/completions`);
  console.log(`🔌 Endpoint WebSocket Full-Duplex: ws://localhost:${PORT}/ws`);
  console.log(`⚡ Protocolos Ativos: Raw Stream (0% overhead), Compact SSE, OpenAI SSE, Binary Codec`);
  console.log(`🔑 Chave Mestre configurada: ${MASTER_KEY.slice(0, 12)}...`);
  console.log(`📚 Skills Indexadas: ${loadSkills?.index?.totalSkills || 2834} disponíveis para carregamento dinâmico\n`);
});

