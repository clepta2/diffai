import test from 'node:test';
import assert from 'node:assert/strict';
import {
  GATEWAYS,
  selectGatewaysForModel,
  resolveFullSwarmRoster,
  resolveDynamicRoleModel,
  MODEL_ALIASES,
  BASE_MODEL_LATENCIES,
  A2A_SYSTEM_HEADER,
  getMemoryReport
} from '../core/orchestrator.js';

test('DiffAI Autonomous Orchestrator v3.2 Test Suite (Padrão Tier-1)', async (t) => {

  await t.test('1. Malha Multi-Gateway e Roteamento por Prefixo de Provedor', () => {
    assert.ok(Array.isArray(GATEWAYS), 'GATEWAYS deve ser um array');
    assert.ok(GATEWAYS.length >= 2, 'Deve conter pelo menos gateway VM e Local');

    const vmGw = GATEWAYS.find(g => g.name === 'vm-tailscale');
    const localGw = GATEWAYS.find(g => g.name === 'local-omnirouter');

    assert.ok(vmGw, 'Gateway vm-tailscale deve estar registrado');
    assert.ok(localGw, 'Gateway local-omnirouter deve estar registrado');

    // Roteamento de Grok (7 contas ativas locais) deve priorizar local-omnirouter
    const grokGateways = selectGatewaysForModel('grok-cli/grok-4.7-medium');
    assert.equal(grokGateways[0].name, 'local-omnirouter', 'Grok deve priorizar gateway local');

    // Roteamento de Groq / Antigravity deve priorizar vm-tailscale
    const groqGateways = selectGatewaysForModel('groq/openai/gpt-oss-120b');
    assert.equal(groqGateways[0].name, 'vm-tailscale', 'Groq deve priorizar gateway VM Tailscale');

    const claudeGateways = selectGatewaysForModel('ag/claude-sonnet-4-6');
    assert.equal(claudeGateways[0].name, 'vm-tailscale', 'Claude Sonnet deve priorizar gateway VM');
  });

  await t.test('2. Conselho dos 6 Cargos (Swarm Roster Completo com Chaos QA)', () => {
    const roster = resolveFullSwarmRoster();

    // Validação dos 6 cargos mandatórios
    assert.ok(roster.ceo, 'Roster deve conter cargo CEO');
    assert.ok(roster.cto, 'Roster deve conter cargo CTO');
    assert.ok(roster.programmer, 'Roster deve conter cargo Programmer');
    assert.ok(roster.reviewer, 'Roster deve conter cargo Reviewer');
    assert.ok(roster.tester, 'Roster deve conter cargo Tester');
    assert.ok(roster.chaos_qa, 'Roster deve conter cargo Chaos QA & Oracle');

    // Verificação de modelos eleitos válidos
    assert.ok(roster.ceo.model, 'CEO deve ter modelo eleito');
    assert.ok(roster.cto.model, 'CTO deve ter modelo eleito');
    assert.ok(roster.programmer.model, 'Programmer deve ter modelo eleito');
    assert.ok(roster.reviewer.model, 'Reviewer deve ter modelo eleito');
    assert.ok(roster.tester.model, 'Tester deve ter modelo eleito');
    assert.ok(roster.chaos_qa.model, 'Chaos QA deve ter modelo eleito');

    // Sub-operários de micro-tarefas
    assert.ok(roster.workers.scraper, 'Worker Scraper deve estar configurado');
    assert.ok(roster.workers.copywriter, 'Worker Copywriter deve estar configurado');
    assert.ok(roster.workers.ddl, 'Worker DDL Architect deve estar configurado');
  });

  await t.test('3. Mapeamento de Aliases e Modelos de Alta Frequência', () => {
    assert.equal(MODEL_ALIASES['fast'], 'groq/openai/gpt-oss-120b', 'Alias fast deve mapear para Groq 120B');
    assert.equal(MODEL_ALIASES['speed'], 'groq/openai/gpt-oss-120b', 'Alias speed deve mapear para Groq 120B');
    assert.equal(MODEL_ALIASES['grok'], 'grok-cli/grok-4.7-medium', 'Alias grok deve mapear para Grok 4.7 Medium');
    assert.equal(MODEL_ALIASES['grok-large'], 'grok-cli/grok-4.7', 'Alias grok-large deve mapear para Grok 4.7');
    assert.equal(MODEL_ALIASES['claude'], 'ag/claude-sonnet-4-6', 'Alias claude deve mapear para Sonnet 4.6');
    assert.equal(MODEL_ALIASES['opus'], 'ag/claude-opus-4-6-thinking', 'Alias opus deve mapear para Opus Thinking');

    // Latência do campeão de velocidade
    assert.equal(BASE_MODEL_LATENCIES['groq/openai/gpt-oss-120b'], 153, 'Groq 120B benchmark latency deve ser 153ms');
  });

  await t.test('4. Protocolo A2A e Telemetria de Memória', () => {
    assert.ok(A2A_SYSTEM_HEADER.includes('[A2A_PROTOCOL_ACTIVE]'), 'A2A header deve conter tag de ativação');
    assert.ok(A2A_SYSTEM_HEADER.includes('Zero cordialidades'), 'A2A deve exigir zero cordialidades');

    const mem = getMemoryReport();
    assert.ok(mem.rssMb, 'Relatório de memória deve conter rssMb');
    assert.ok(mem.heapUsedMb, 'Relatório de memória deve conter heapUsedMb');
  });

});
