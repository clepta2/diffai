import test from 'node:test';
import assert from 'node:assert/strict';
import {
  GATEWAYS,
  selectGatewaysForModel,
  resolveFullSwarmRoster,
  resolveDynamicRoleModel,
  SWARM_16_SPECS,
  A2AMessageBus,
  A2A_MESSAGE_TYPES,
  A2A_PRIORITY,
  AutonomousSwarmAgent,
  SwarmCouncilRuntime16,
  RealtimeSwarmMatrix16,
  runRealtimeSwarm16,
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

  await t.test('2. Swarm Roster Completo dos 16 Agentes (8 Criadores + 8 Revisores Dedicados)', () => {
    const roster = resolveFullSwarmRoster();

    // 8 Agentes Criadores
    assert.ok(roster.ceo, 'Roster deve conter cargo CEO');
    assert.ok(roster.cto, 'Roster deve conter cargo CTO');
    assert.ok(roster.programmer, 'Roster deve conter cargo Programmer');
    assert.ok(roster.reviewer, 'Roster deve conter cargo Reviewer');
    assert.ok(roster.tester, 'Roster deve conter cargo Tester');
    assert.ok(roster.chaos_qa, 'Roster deve conter cargo Chaos QA & Oracle');
    assert.ok(roster.red_team, 'Roster deve conter cargo Red Team Adversarial');
    assert.ok(roster.blue_team, 'Roster deve conter cargo Blue Team Sentinel');

    // 8 Agentes Revisores Dedicados
    assert.ok(roster.ceo_reviewer, 'Roster deve conter ceo_reviewer');
    assert.ok(roster.cto_reviewer, 'Roster deve conter cto_reviewer');
    assert.ok(roster.programmer_reviewer, 'Roster deve conter programmer_reviewer');
    assert.ok(roster.reviewer_reviewer, 'Roster deve conter reviewer_reviewer');
    assert.ok(roster.tester_reviewer, 'Roster deve conter tester_reviewer');
    assert.ok(roster.chaos_qa_reviewer, 'Roster deve conter chaos_qa_reviewer');
    assert.ok(roster.red_team_reviewer, 'Roster deve conter red_team_reviewer');
    assert.ok(roster.blue_team_reviewer, 'Roster deve conter blue_team_reviewer');

    // Coleções agrupadas
    assert.equal(Object.keys(roster.creators).length, 8, 'Devem existir 8 criadores em roster.creators');
    assert.equal(Object.keys(roster.reviewers).length, 8, 'Devem existir 8 revisores em roster.reviewers');
    assert.equal(Object.keys(roster.all16).length, 16, 'Devem existir 16 agentes em roster.all16');

    // Sub-operários de micro-tarefas
    assert.ok(roster.workers.scraper, 'Worker Scraper deve estar configurado');
    assert.ok(roster.workers.copywriter, 'Worker Copywriter deve estar configurado');
    assert.ok(roster.workers.ddl, 'Worker DDL Architect deve estar configurado');
  });

  await t.test('3. Matriz Canônica SWARM_16_SPECS (8 Pares Especialistas)', () => {
    assert.equal(SWARM_16_SPECS.length, 8, 'Devem existir 8 pares especialistas na matriz');

    for (const spec of SWARM_16_SPECS) {
      assert.ok(spec.pairIndex >= 1 && spec.pairIndex <= 8, 'pairIndex deve estar entre 1 e 8');
      assert.ok(spec.creator.id, 'Criador deve ter id');
      assert.ok(spec.creator.role, 'Criador deve ter role');
      assert.ok(spec.creator.name, 'Criador deve ter name');
      assert.ok(spec.creator.defaultModel, 'Criador deve ter defaultModel');
      assert.ok(spec.reviewer.id, 'Revisor deve ter id');
      assert.ok(spec.reviewer.role, 'Revisor deve ter role');
      assert.ok(spec.reviewer.name, 'Revisor deve ter name');
      assert.ok(spec.reviewer.defaultModel, 'Revisor deve ter defaultModel');
      assert.ok(Array.isArray(spec.reviewer.criteria), 'Revisor deve ter lista de critérios de auditoria');
    }
  });

  await t.test('4. Execução Concorrente Estrita das 16 IAs no Segundo Zero (Zero Cascata)', async () => {
    const runtime = new SwarmCouncilRuntime16();
    const telemetry = runtime.getLiveTelemetry();

    assert.equal(telemetry.totalAgents, 16, 'Telemetria deve reportar 16 agentes');
    assert.equal(telemetry.creatorsCount, 8, 'Devem existir 8 criadores');
    assert.equal(telemetry.reviewersCount, 8, 'Devem existir 8 revisores');

    const result = await runtime.executeParallelSwarm({
      niche: 'SaaS Financeiro Multi-Tenant',
      brandName: 'FinPro Cloud'
    });

    assert.equal(result.isStrictlyParallel, true, 'Execução deve ser estritamente paralela');
    assert.equal(result.totalAgents, 16, 'Total de agentes deve ser 16');
    assert.equal(result.creatorsCount, 8);
    assert.equal(result.reviewersCount, 8);
    assert.equal(result.allApproved, true, 'Todas as 16 IAs devem aprovar em fluxo nominal');
    assert.ok(result.iqs >= 95.0, `IQS deve ser >= 95.0 (calculado: ${result.iqs})`);
    assert.equal(result.vetoActive, false, 'Veto não deve estar ativo');
    assert.ok(result.durationMs < 500, `Execução paralela deve resolver em sub-segundo (levou ${result.durationMs}ms)`);
    assert.ok(result.dialogueLogLength >= 16, `Devem ocorrer ao menos 16 mensagens no Barramento A2A (ocorreram ${result.dialogueLogLength})`);
  });

  await t.test('5. Trava de Veto Isolado de Qualquer um dos 16 Agentes Bloqueia o Deploy', async () => {
    const runtime = new SwarmCouncilRuntime16();

    // Red Team Reviewer detecta exploit crítico e emite veto
    const redTeamRev = runtime.getAgent('red_team_reviewer');
    assert.ok(redTeamRev, 'Red Team Reviewer deve existir no runtime');

    redTeamRev.issueVeto('Tentativa de exploração de privilege escalation confirmada.', 'SEC-CRITICAL-RLS-BREACH');

    const result = await runtime.executeParallelSwarm({
      niche: 'SaaS sob Teste de Invasão'
    });

    assert.equal(result.vetoTriggered, true, 'Veto deve estar marcado como disparado');
    assert.equal(result.vetoActive, true, 'Veto deve permanecer ativo');
    assert.equal(result.allApproved, false, 'Veto isolado deve bloquear aprovação unânime de todos os 16 agentes');
    assert.ok(result.summary.includes('Bloqueado'), 'Resumo deve indicar bloqueio');
  });

  await t.test('6. Isolamento em Sandbox do Red Team e Fiscalização Sentinel do Blue Team', async () => {
    const runtime = new SwarmCouncilRuntime16();

    const redTeam = runtime.getAgent('red_team');
    const blueTeam = runtime.getAgent('blue_team');
    const blueTeamRev = runtime.getAgent('blue_team_reviewer');

    assert.ok(redTeam, 'Agente Red Team deve existir');
    assert.ok(blueTeam, 'Agente Blue Team deve existir');
    assert.ok(blueTeamRev, 'Agente Blue Team Reviewer deve existir');

    const result = await runtime.executeParallelSwarm({
      niche: 'SaaS com Sandbox Quarentenada'
    });

    // Encontra os resultados individuais
    const redTeamRes = result.agentResults.find(r => r.role === 'red_team')?.result;
    const blueTeamRes = result.agentResults.find(r => r.role === 'blue_team')?.result;
    const blueTeamRevRes = result.agentResults.find(r => r.role === 'blue_team_reviewer')?.result;

    assert.ok(redTeamRes.rlsBypassAttempted, 'Red Team deve ter tentado bypass de RLS');
    assert.equal(redTeamRes.breachesBlocked, 5, 'Todas as 5 brechas devem ter sido bloqueadas');
    assert.equal(blueTeamRes.quarantineActive, true, 'Blue Team deve assegurar quarentena ativa');
    assert.equal(blueTeamRes.zeroLeakToProduction, true, 'Nenhum script de ataque pode vazar para produção');
    assert.equal(blueTeamRevRes.quarantineCertified, true, 'Blue Team Reviewer deve certificar quarentena');
  });

  await t.test('7. Função de Alto Nível runRealtimeSwarm16 e Interoperabilidade RealtimeSwarmMatrix16', async () => {
    assert.equal(RealtimeSwarmMatrix16, SwarmCouncilRuntime16, 'RealtimeSwarmMatrix16 deve ser alias canônico de SwarmCouncilRuntime16');

    const res = await runRealtimeSwarm16({
      niche: 'Clínica Médica e Telemedicina'
    });

    assert.equal(res.ok, true, 'runRealtimeSwarm16 deve retornar ok === true');
    assert.equal(res.totalAgents, 16, 'Total de agentes deve ser 16');
    assert.equal(res.isStrictlyParallel, true, 'Deve ser estritamente paralelo');
    assert.ok(res.iqs >= 95.0, 'IQS deve ser >= 95.0');
    assert.ok(res.roster.creators.ceo, 'Roster deve conter creators');
    assert.ok(res.roster.reviewers.ceo_reviewer, 'Roster deve conter reviewers');
  });

  await t.test('8. Mapeamento de Aliases e Modelos de Alta Frequência', () => {
    assert.equal(MODEL_ALIASES['fast'], 'groq/openai/gpt-oss-120b', 'Alias fast deve mapear para Groq 120B');
    assert.equal(MODEL_ALIASES['speed'], 'groq/openai/gpt-oss-120b', 'Alias speed deve mapear para Groq 120B');
    assert.equal(MODEL_ALIASES['grok'], 'grok-cli/grok-4.7-medium', 'Alias grok deve mapear para Grok 4.7 Medium');
    assert.equal(MODEL_ALIASES['grok-large'], 'grok-cli/grok-4.7', 'Alias grok-large deve mapear para Grok 4.7');
    assert.equal(MODEL_ALIASES['claude'], 'ag/claude-sonnet-4-6', 'Alias claude deve mapear para Sonnet 4.6');
    assert.equal(MODEL_ALIASES['opus'], 'ag/claude-opus-4-6-thinking', 'Alias opus deve mapear para Opus Thinking');

    // Latência do campeão de velocidade
    assert.equal(BASE_MODEL_LATENCIES['groq/openai/gpt-oss-120b'], 153, 'Groq 120B benchmark latency deve ser 153ms');
  });

  await t.test('9. Protocolo A2A e Telemetria de Memória', () => {
    assert.ok(A2A_SYSTEM_HEADER.includes('[A2A_PROTOCOL_ACTIVE]'), 'A2A header deve conter tag de ativação');
    assert.ok(A2A_SYSTEM_HEADER.includes('Zero cordialidades'), 'A2A deve exigir zero cordialidades');

    const mem = getMemoryReport();
    assert.ok(mem.rssMb, 'Relatório de memória deve conter rssMb');
    assert.ok(mem.heapUsedMb, 'Relatório de memória deve conter heapUsedMb');
  });

});
