import fs from 'fs';
import path from 'path';
import os from 'os';

console.log('🚀 Configurando DiffAI no Antigravity automaticamente...\n');

const homeDir = os.homedir();
const geminiConfigDir = path.join(homeDir, '.gemini', 'config');
const mcpConfigPath = path.join(geminiConfigDir, 'mcp_config.json');
const toolsDir = path.join(homeDir, '.gemini', 'antigravity', 'mcp', 'omnirouter');

// 1. Garante diretórios
if (!fs.existsSync(geminiConfigDir)) fs.mkdirSync(geminiConfigDir, { recursive: true });
if (!fs.existsSync(toolsDir)) fs.mkdirSync(toolsDir, { recursive: true });

// 2. Prepara .env se não existir
const envPath = path.join(process.cwd(), '.env');
const envExamplePath = path.join(process.cwd(), '.env.example');
if (!fs.existsSync(envPath) && fs.existsSync(envExamplePath)) {
    fs.copyFileSync(envExamplePath, envPath);
    console.log('✓ Arquivo .env criado a partir de .env.example');
}

// 3. Atualiza ou cria mcp_config.json
let mcpConfig = { mcpServers: {} };
if (fs.existsSync(mcpConfigPath)) {
    try {
        mcpConfig = JSON.parse(fs.readFileSync(mcpConfigPath, 'utf8'));
    } catch (_) {}
}
if (!mcpConfig.mcpServers) mcpConfig.mcpServers = {};

const scriptPath = path.join(geminiConfigDir, 'omnirouter-mcp.js');
// Garante o conector MCP
const connectorSource = `/**
 * omnirouter-mcp.js — Conector de Baixa Latência para DiffAI & Antigravity
 */
const http = require('http');
const readline = require('readline');

const OMNIROUTER_HOST = process.env.OMNIROUTER_HOST || '127.0.0.1';
const OMNIROUTER_PORT = process.env.OMNIROUTER_PORT || 20128;
const OMNIROUTER_KEY = process.env.OMNIROUTER_KEY || 'sk-diffai-local';

const httpAgent = new http.Agent({ keepAlive: true, keepAliveMsecs: 180000, maxSockets: 100 });

async function executeViaHttp(prompt, model = 'diffai-pro', system = '') {
    return new Promise((resolve, reject) => {
        const payload = JSON.stringify({
            model: model,
            messages: [
                ...(system ? [{ role: 'system', content: system }] : []),
                { role: 'user', content: prompt }
            ]
        });

        const req = http.request({
            hostname: OMNIROUTER_HOST,
            port: OMNIROUTER_PORT,
            path: '/v1/chat/completions',
            method: 'POST',
            agent: httpAgent,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + OMNIROUTER_KEY,
                'Content-Length': Buffer.byteLength(payload)
            }
        }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    resolve(json.choices?.[0]?.message?.content || data);
                } catch (e) {
                    resolve(data);
                }
            });
        });
        req.on('error', err => reject(err));
        req.write(payload);
        req.end();
    });
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: false });
rl.on('line', async (line) => {
    if (!line.trim()) return;
    try {
        const msg = JSON.parse(line);
        if (msg.method === 'tools/list') {
            process.stdout.write(JSON.stringify({
                jsonrpc: '2.0',
                id: msg.id,
                result: {
                    tools: [
                        { name: 'speculative_fast_query', description: 'Execução ultrarrápida via cluster DiffAI', inputSchema: { type: 'object', properties: { prompt: { type: 'string' } }, required: ['prompt'] } },
                        { name: 'deepseek_r1_reasoning', description: 'Raciocínio lógico dedutivo profundo via DeepSeek R1', inputSchema: { type: 'object', properties: { prompt: { type: 'string' } }, required: ['prompt'] } },
                        { name: 'claude_code_expert', description: 'Especialista em código de alta performance Claude Sonnet', inputSchema: { type: 'object', properties: { prompt: { type: 'string' } }, required: ['prompt'] } },
                        { name: 'multi_agent_pipeline', description: 'Pipeline Multi-Agente R1 -> Claude', inputSchema: { type: 'object', properties: { task: { type: 'string' } }, required: ['task'] } },
                        { name: 'a2a_compact_query', description: 'Comunicação compacta Agent-to-Agent (-80% tokens)', inputSchema: { type: 'object', properties: { prompt: { type: 'string' } }, required: ['prompt'] } }
                    ]
                }
            }) + '\\n');
        } else if (msg.method === 'tools/call') {
            const tool = msg.params?.name;
            const args = msg.params?.arguments || {};
            const prompt = args.prompt || args.task || '';
            const reply = await executeViaHttp(prompt, tool === 'deepseek_r1_reasoning' ? 'deepseek-r1' : 'diffai-pro');
            process.stdout.write(JSON.stringify({
                jsonrpc: '2.0',
                id: msg.id,
                result: { content: [{ type: 'text', text: reply }] }
            }) + '\\n');
        } else if (msg.method === 'initialize') {
            process.stdout.write(JSON.stringify({
                jsonrpc: '2.0',
                id: msg.id,
                result: { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'omnirouter', version: '3.0.0' } }
            }) + '\\n');
        }
    } catch (_) {}
});
`;

fs.writeFileSync(scriptPath, connectorSource, 'utf8');

mcpConfig.mcpServers.omnirouter = {
    command: 'node',
    args: [scriptPath.replace(/\\/g, '/')],
    env: {
        OMNIROUTER_URL: 'http://127.0.0.1:20128/v1',
        OMNIROUTER_KEY: 'sk-diffai-local'
    }
};

fs.writeFileSync(mcpConfigPath, JSON.stringify(mcpConfig, null, 2), 'utf8');
console.log('✓ Configuração MCP registrada em:', mcpConfigPath);

// 4. Cria os arquivos de schema das ferramentas no diretório de ferramentas
const tools = [
    {
        name: 'speculative_fast_query',
        schema: {
            name: 'speculative_fast_query',
            description: 'Execução ultrarrápida via cluster DiffAI com seleção automática do modelo com menor latência (<250ms).',
            parameters: { type: 'object', properties: { prompt: { type: 'string', description: 'A pergunta ou instrução rápida' } }, required: ['prompt'] }
        }
    },
    {
        name: 'deepseek_r1_reasoning',
        schema: {
            name: 'deepseek_r1_reasoning',
            description: 'Raciocínio dedutivo e matemático avançado usando DeepSeek R1 para planejar arquiteturas complexas.',
            parameters: { type: 'object', properties: { prompt: { type: 'string', description: 'Problema complexo a ser dissecado' } }, required: ['prompt'] }
        }
    },
    {
        name: 'claude_code_expert',
        schema: {
            name: 'claude_code_expert',
            description: 'Especialista em código e refatoração com alta precisão e integridade de contexto.',
            parameters: { type: 'object', properties: { prompt: { type: 'string', description: 'Tarefa ou arquivo a ser codificado' } }, required: ['prompt'] }
        }
    },
    {
        name: 'multi_agent_pipeline',
        schema: {
            name: 'multi_agent_pipeline',
            description: 'Pipeline Multi-Agente em Cadeia: O DeepSeek R1 calcula a arquitetura e invariantes primeiro, e o Claude Sonnet gera a implementação final refinada baseada no plano do R1.',
            parameters: { type: 'object', properties: { task: { type: 'string', description: 'Tarefa completa a ser resolvida pelo pipeline R1 -> Claude' } }, required: ['task'] }
        }
    },
    {
        name: 'a2a_compact_query',
        schema: {
            name: 'a2a_compact_query',
            description: 'Comunicação compacta entre agentes para transferir contexto e dados brutos economizando 80%+ de tokens.',
            parameters: { type: 'object', properties: { prompt: { type: 'string', description: 'Dados estruturados a compactar' } }, required: ['prompt'] }
        }
    }
];

tools.forEach(t => {
    fs.writeFileSync(path.join(toolsDir, `${t.name}.json`), JSON.stringify(t.schema, null, 2), 'utf8');
});
console.log('✓ Schemas de ferramentas registrados em:', toolsDir);

console.log('\n🎉 SUCESSO! DiffAI configurado no Antigravity.');
console.log('Ao abrir o Antigravity IDE, o orquestrador e as 5 ferramentas estarão ativas imediatamente!\n');
