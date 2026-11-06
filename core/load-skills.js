/**
 * loadSkills.js
 * Módulo Ultra-Rápido de Lazy Loading e Resolução Semântica de Skills.
 * Carrega APENAS a skill estritamente necessária sob demanda baseada no prompt do usuário,
 * economizando até 90% de tokens de contexto e reduzindo a latência a milissegundos.
 */

const fs = require('fs');
const path = require('path');

class SkillsLoader {
  constructor() {
    this.indexPath = this._resolveIndexPath();
    this.index = null;
    this.cache = new Map();
    this._loadIndex();
  }

  _resolveIndexPath() {
    const candidates = [
      path.resolve(__dirname, '../skills/skills-index.min.json'),
      path.resolve(__dirname, '../skills/skills-index.json'),
      path.resolve(__dirname, 'skills-index.json'),
      path.resolve(__dirname, '../.agents/skills/skills-index.json'),
      path.resolve(__dirname, '../shared/config/skills-index.json'),
      'C:\\Users\\cliente\\.agentic-skills\\skills-index.json'
    ];
    for (const p of candidates) {
      if (fs.existsSync(p)) return p;
    }
    return candidates[0];
  }

  _loadIndex() {
    try {
      if (fs.existsSync(this.indexPath)) {
        const raw = fs.readFileSync(this.indexPath, 'utf8');
        this.index = JSON.parse(raw);
      } else {
        this.index = { totalSkills: 0, skills: [] };
      }
    } catch (err) {
      this.index = { totalSkills: 0, skills: [] };
    }
  }

  /**
   * Encontra as skills mais relevantes para o prompt do usuário
   * @param {string} prompt - Mensagem ou instrução do usuário
   * @param {object} options - { limit: 3, minScore: 1.5, category: null }
   * @returns {Array<{ skill: object, score: number }>}
   */
  match(prompt, options = {}) {
    if (!this.index || !this.index.skills) return [];
    const limit = options.limit || 3;
    const minScore = options.minScore || 1.2;
    const filterCat = options.category || null;

    const queryWords = prompt
      .toLowerCase()
      .replace(/[^a-z0-9áéíóúãõçü_-]+/g, ' ')
      .split(/\s+/)
      .filter(w => w.length >= 3);

    if (queryWords.length === 0) return [];

    const scored = [];

    for (const skill of this.index.skills) {
      if (filterCat && skill.category !== filterCat) continue;

      let score = 0;
      const idLower = skill.id.toLowerCase();
      const nameLower = skill.name.toLowerCase();
      const descLower = skill.description.toLowerCase();
      const tags = (skill.tags || []).map(t => t.toLowerCase());
      const kwSet = new Set(skill.keywords || []);

      for (const word of queryWords) {
        // 1. Match exato no ID (ex: "photocraft", "e2e", "supabase")
        if (idLower === word) score += 12.0;
        else if (idLower.includes(word)) score += 5.0;

        // 2. Match no nome da skill
        if (nameLower.includes(word)) score += 4.0;

        // 3. Match nas tags
        if (tags.some(t => t.includes(word))) score += 3.5;

        // 4. Match no conjunto de keywords
        if (kwSet.has(word)) score += 2.0;

        // 5. Match na descrição
        if (descLower.includes(word)) score += 1.5;
      }

      // Prioridade adicional para skills locais do workspace (.agents/skills)
      if (skill.source === 'workspace') {
        score += 0.5;
      }

      if (score >= minScore) {
        scored.push({ skill, score: Number(score.toFixed(2)) });
      }
    }

    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, limit);
  }

  /**
   * Carrega uma skill específica por ID (com cache em memória)
   * @param {string} skillId - ID da skill (ex: "photocraft")
   * @returns {object|null}
   */
  load(skillId) {
    if (this.cache.has(skillId)) {
      return this.cache.get(skillId);
    }

    const meta = this.index?.skills?.find(s => s.id === skillId);
    if (!meta) return null;

    try {
      if (fs.existsSync(meta.path)) {
        const fullContent = fs.readFileSync(meta.path, 'utf8');

        // Extrai apenas o corpo das instruções (sem preâmbulo gigante se houver)
        let body = fullContent;
        if (body.startsWith('---')) {
          const endFm = body.indexOf('---', 3);
          if (endFm !== -1) {
            body = body.slice(endFm + 3).trim();
          }
        }

        const skillObj = {
          id: meta.id,
          name: meta.name,
          description: meta.description,
          category: meta.category,
          path: meta.path,
          tokenEstimate: meta.tokenEstimate,
          content: fullContent,
          instructions: body
        };

        this.cache.set(skillId, skillObj);
        return skillObj;
      }
    } catch (_) {}

    return null;
  }

  /**
   * Gera o bloco de injeção compacto para o System Prompt da IA
   * Injeta SOMENTE as regras das skills relevantes, cortando 90% dos tokens desnecessários!
   * @param {string} userPrompt - Pergunta/solicitação do usuário
   * @param {object} options - { maxSkills: 2, maxTokens: 900 }
   * @returns {string} - Bloco em markdown pronto para concatenar no system prompt
   */
  getPromptInjection(userPrompt, options = {}) {
    const maxSkills = options.maxSkills || 2;
    const maxTokens = options.maxTokens || 900;

    const matches = this.match(userPrompt, { limit: maxSkills, minScore: 2.0 });
    if (matches.length === 0) return '';

    let injection = '\n\n=== REGRAS DE AGENTIC SKILLS ATIVADAS (LAZY LOADED ON-DEMAND) ===\n';
    let currentTokens = 30;

    for (const match of matches) {
      const skill = this.load(match.skill.id);
      if (!skill) continue;

      // Trunca instruções se excederem o orçamento estrito de tokens
      const maxCharsForSkill = (maxTokens - currentTokens) * 4;
      if (maxCharsForSkill <= 100) break;

      let instructions = skill.instructions;
      if (instructions.length > maxCharsForSkill) {
        instructions = instructions.slice(0, maxCharsForSkill) + '\n... [Instruções truncadas para economizar tokens]';
      }

      injection += `\n--- SKILL: ${skill.name} (${skill.id}) [Relevância: ${match.score}] ---\n`;
      injection += `${instructions}\n`;

      currentTokens += Math.ceil(instructions.length / 4);
      if (currentTokens >= maxTokens) break;
    }

    return injection;
  }

  listByCategory(cat) {
    if (!this.index?.skills) return [];
    return this.index.skills.filter(s => s.category === cat);
  }

  stats() {
    return {
      total: this.index?.totalSkills || 0,
      categories: this.index?.categories || {},
      generatedAt: this.index?.generatedAt || null,
      indexPath: this.indexPath
    };
  }
}

// Singleton global export
const loaderInstance = new SkillsLoader();

// CLI Interface
if (require.main === module) {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'help';

  switch (cmd) {
    case 'stats':
      console.log('📊 Estatísticas do Skills Index:');
      console.log(JSON.stringify(loaderInstance.stats(), null, 2));
      break;

    case 'search':
    case 'find':
      const q = args.slice(1).join(' ');
      if (!q) {
        console.log('Uso: node loadSkills.js search <termo>');
        process.exit(1);
      }
      console.log(`🔍 Buscando skills para: "${q}"...`);
      const results = loaderInstance.match(q, { limit: 5 });
      if (results.length === 0) {
        console.log('Nenhuma skill relevante encontrada.');
      } else {
        results.forEach((r, i) => {
          console.log(`\n[#${i + 1}] ${r.skill.name} (${r.skill.id})`);
          console.log(`    Categoria: ${r.skill.category} | Relevância: ${r.score} | Tokens: ~${r.skill.tokenEstimate}`);
          console.log(`    Descrição: ${r.skill.description}`);
        });
      }
      break;

    case 'load':
    case 'get':
      const id = args[1];
      if (!id) {
        console.log('Uso: node loadSkills.js get <skill-id>');
        process.exit(1);
      }
      const s = loaderInstance.load(id);
      if (!s) console.log(`Skill "${id}" não encontrada.`);
      else {
        console.log(`=== ${s.name} (${s.id}) ===`);
        console.log(`Categoria: ${s.category}`);
        console.log(`Path: ${s.path}`);
        console.log(`Tamanho: ${s.instructions.length} caracteres (~${s.tokenEstimate} tokens)`);
        console.log('\n--- Primeiras 15 linhas ---');
        console.log(s.instructions.split('\n').slice(0, 15).join('\n'));
      }
      break;

    case 'prompt':
      const testPrompt = args.slice(1).join(' ') || 'quero retocar uma foto gastronômica no photocraft com contraste e nitidez';
      console.log(`Testando injeção dinâmica de prompt para: "${testPrompt}"`);
      const snippet = loaderInstance.getPromptInjection(testPrompt);
      console.log('--- SNIPPET GERADO PARA O SYSTEM PROMPT (TOKEN SAVED) ---');
      console.log(snippet);
      break;

    case 'rebuild':
      console.log('Reconstruindo índice...');
      require('./build-skills-index.js');
      break;

    default:
      console.log('Skills Loader CLI:');
      console.log('  node loadSkills.js stats');
      console.log('  node loadSkills.js search <prompt>');
      console.log('  node loadSkills.js get <skill-id>');
      console.log('  node loadSkills.js prompt <mensagem>');
      console.log('  node loadSkills.js rebuild');
      break;
  }
}

module.exports = loaderInstance;
