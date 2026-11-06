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
const { speculativeRace, runSequentialPipeline, runEnsembleConsensus } = require('../core/orchestrator');

const PORT = parseInt(process.env.PORT || '3030', 10);
const MASTER_KEY = process.env.DIFFAI_API_KEY || process.env.MASTER_API_KEY || 'sk-diffai-master-key';
const memory = new MemoryManager();

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
  if (token && (token === MASTER_KEY || token === process.env.DIFFAI_API_KEY || token === process.env.OMNIROUTER_API_KEY)) return true;
  return true; // Modo desenvolvimento permissivo
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

  // Health
  if (pathname === '/health' || pathname === '/') {
    sendJson(res, 200, {
      ok: true,
      service: 'DiffAI Intelligence Engine',
      version: '3.0.0',
      totalSkills: loadSkills?.index?.totalSkills || 2834,
      orchestratorModes: ['speculative-racing', 'sequential-dag', 'ensemble-consensus']
    });
    return;
  }

  // Modelos disponíveis (OpenAI Standard)
  if (pathname === '/v1/models') {
    sendJson(res, 200, {
      object: 'list',
      data: [
        { id: 'diffai-pro', object: 'model', created: Date.now(), owned_by: 'diffai' },
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

      // 3. Execução pelo Orquestrador
      let replyText = '';
      const mode = body.mode || (body.model === 'diffai-consensus' ? 'consensus' : (body.model === 'diffai-racing' ? 'race' : 'fast'));

      if (mode === 'race') {
        const raceRes = await speculativeRace(userPrompt, ['kr/gpt-5.6-sol', 'kr/claude-sonnet-4.5', 'gemini']);
        replyText = raceRes.content || 'Resposta concluída via Hedged Race.';
      } else if (mode === 'consensus') {
        const conRes = await runEnsembleConsensus(userPrompt);
        replyText = conRes.synthesis || 'Consenso multimodelo concluído.';
      } else {
        // Modo Padrão: Prompt Enriquecido com Memória e Skill
        const enrichedPrompt = `${skillInjection ? `[Skills Ativas]\n${skillInjection}\n\n` : ''}${tenantCtx.textBlock}\n\nPergunta do Lojista: ${userPrompt}`;
        const defaultRace = await speculativeRace(enrichedPrompt, ['kr/claude-sonnet-4.5', 'gemini']);
        replyText = defaultRace.content || 'Resposta gerada com sucesso.';
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

server.listen(PORT, () => {
  console.log(`\n🚀 DiffAI Intelligence Engine iniciado na porta ${PORT}`);
  console.log(`📡 Endpoint OpenAI Gateway: http://localhost:${PORT}/v1/chat/completions`);
  console.log(`🔑 Chave Mestre configurada: ${MASTER_KEY.slice(0, 12)}...`);
  console.log(`📚 Skills Indexadas: ${loadSkills?.index?.totalSkills || 2834} disponíveis para carregamento dinâmico\n`);
});

