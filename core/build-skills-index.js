/**
 * build-skills-index.js
 * Escaneia todas as skills do workspace e do repositório global (~2.800 skills),
 * extrai frontmatter YAML, gera palavras-chave, calcula custos de tokens
 * e compila o `skills-index.json` ultra-otimizado para lazy loading.
 */

const fs = require('fs');
const path = require('path');

const WORKSPACE_SKILLS_DIR = path.resolve('.agents/skills');
const GLOBAL_SKILLS_DIR = 'C:\\Users\\cliente\\.agentic-skills\\skills';

const STOPWORDS = new Set([
  'a', 'o', 'as', 'os', 'um', 'uma', 'uns', 'umas', 'de', 'do', 'da', 'dos', 'das',
  'em', 'no', 'na', 'nos', 'nas', 'por', 'para', 'com', 'sem', 'que', 'se', 'e',
  'ou', 'quando', 'onde', 'como', 'the', 'and', 'for', 'with', 'from', 'that', 'this',
  'use', 'when', 'task', 'using', 'is', 'are', 'to', 'in', 'of', 'on', 'at', 'by'
]);

function extractKeywords(text) {
  if (!text) return [];
  const words = text
    .toLowerCase()
    .replace(/[^a-z0-9áéíóúãõçü_-]+/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 3 && !STOPWORDS.has(w));
  return Array.from(new Set(words));
}

function parseFrontmatter(content) {
  const meta = { name: '', description: '', tags: [] };
  if (!content.startsWith('---')) return meta;

  const endIdx = content.indexOf('---', 3);
  if (endIdx === -1) return meta;

  const fm = content.slice(3, endIdx).trim();
  const lines = fm.split('\n');
  let currentKey = null;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    if (trimmed.startsWith('-') && currentKey === 'tags') {
      const val = trimmed.replace(/^-\s*/, '').replace(/['"]/g, '').trim();
      if (val) meta.tags.push(val);
      continue;
    }

    const colonIdx = trimmed.indexOf(':');
    if (colonIdx !== -1) {
      const key = trimmed.slice(0, colonIdx).trim().toLowerCase();
      let val = trimmed.slice(colonIdx + 1).trim();
      val = val.replace(/^['"](.*)['"]$/, '$1').trim();

      currentKey = key;
      if (key === 'name') meta.name = val;
      else if (key === 'description') meta.description = val;
      else if (key === 'tags' && val) {
        meta.tags = val.replace(/[\[\]'"]/g, '').split(',').map(s => s.trim()).filter(Boolean);
      }
    }
  }

  return meta;
}

function categorizeSkill(id, name, desc, tags) {
  const full = `${id} ${name} ${desc} ${tags.join(' ')}`.toLowerCase();

  if (/postgres|supabase|sql|database|db|query|schema|migration|prisma|drizzle|rls/.test(full)) return 'database';
  if (/photo|image|photocraft|psd|canvas|3d|render|svg|draw|diagram|design|threejs/.test(full)) return 'media-design';
  if (/agent|orchestrat|multi-agent|autonomous|eval|memory|swarm|subagent/.test(full)) return 'ai-agents';
  if (/test|playwright|e2e|cypress|jest|qa|assert|spec|verify|audit/.test(full)) return 'testing-qa';
  if (/security|devsec|pentest|vulnerab|cve|auth|jwt|matrix|hardening|injection|xss/.test(full)) return 'security';
  if (/react|vue|angular|frontend|css|html|tailwind|ui|component|web/.test(full)) return 'frontend-ui';
  if (/api|node|express|fastapi|backend|python|rust|golang|worker|microservice/.test(full)) return 'backend-api';
  if (/cloudflare|docker|k8s|kubernetes|aws|azure|devops|deploy|ci|cd|pipeline/.test(full)) return 'devops-cloud';
  if (/crm|sales|marketing|copy|seo|whatsapp|lead|funnel|email|growth/.test(full)) return 'business-growth';

  return 'utility';
}

function scanDir(dirPath, sourceLabel) {
  if (!fs.existsSync(dirPath)) return [];
  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  const skills = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const skillId = entry.name;
    const skillFile = path.join(dirPath, skillId, 'SKILL.md');

    if (!fs.existsSync(skillFile)) continue;

    try {
      const content = fs.readFileSync(skillFile, 'utf8');
      const { name, description, tags } = parseFrontmatter(content);
      const effectiveName = name || skillId;
      const effectiveDesc = description || '';

      const kwDesc = extractKeywords(effectiveDesc);
      const kwName = extractKeywords(effectiveName);
      const kwTags = tags.map(t => t.toLowerCase().trim());
      const allKeywords = Array.from(new Set([...kwName, ...kwDesc, ...kwTags, skillId.toLowerCase()])).slice(0, 24);

      const category = categorizeSkill(skillId, effectiveName, effectiveDesc, tags);
      const charCount = content.length;
      const tokenEstimate = Math.ceil(charCount / 4);

      skills.push({
        id: skillId,
        name: effectiveName,
        description: effectiveDesc.slice(0, 240),
        category: category,
        keywords: allKeywords,
        tags: tags.slice(0, 8),
        path: skillFile.replace(/\\/g, '/'),
        source: sourceLabel,
        tokenEstimate: tokenEstimate
      });
    } catch (_) {}
  }

  return skills;
}

console.log('🚀 Iniciando indexação inteligente de skills...');
const t0 = Date.now();

const workspaceSkills = scanDir(WORKSPACE_SKILLS_DIR, 'workspace');
const globalSkills = scanDir(GLOBAL_SKILLS_DIR, 'global');

// Mescla priorizando workspace em caso de duplicidade de ID
const skillsMap = new Map();
globalSkills.forEach(s => skillsMap.set(s.id, s));
workspaceSkills.forEach(s => skillsMap.set(s.id, s)); // Sobrescreve com workspace

const allSkills = Array.from(skillsMap.values()).sort((a, b) => a.id.localeCompare(b.id));

// Cria índice reverso de palavras-chave para busca instantânea em tempo O(1)
const invertedIndex = {};
allSkills.forEach((s, idx) => {
  s.keywords.forEach(kw => {
    if (!invertedIndex[kw]) invertedIndex[kw] = [];
    invertedIndex[kw].push(idx);
  });
});

const indexPayload = {
  version: '2.0.0',
  generatedAt: new Date().toISOString(),
  totalSkills: allSkills.length,
  workspaceCount: workspaceSkills.length,
  globalCount: globalSkills.length,
  skills: allSkills,
  categories: {
    'media-design': allSkills.filter(s => s.category === 'media-design').length,
    'database': allSkills.filter(s => s.category === 'database').length,
    'ai-agents': allSkills.filter(s => s.category === 'ai-agents').length,
    'testing-qa': allSkills.filter(s => s.category === 'testing-qa').length,
    'security': allSkills.filter(s => s.category === 'security').length,
    'frontend-ui': allSkills.filter(s => s.category === 'frontend-ui').length,
    'backend-api': allSkills.filter(s => s.category === 'backend-api').length,
    'devops-cloud': allSkills.filter(s => s.category === 'devops-cloud').length,
    'business-growth': allSkills.filter(s => s.category === 'business-growth').length,
    'utility': allSkills.filter(s => s.category === 'utility').length,
  }
};

const outputPaths = [
  path.resolve('.agents/skills/skills-index.json'),
  path.resolve('scripts/skills-index.json'),
  path.resolve('shared/config/skills-index.json'),
  'C:\\Users\\cliente\\.agentic-skills\\skills-index.json'
];

outputPaths.forEach(outPath => {
  try {
    const parent = path.dirname(outPath);
    if (!fs.existsSync(parent)) fs.mkdirSync(parent, { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(indexPayload, null, 2), 'utf8');
    fs.writeFileSync(outPath.replace('.json', '.min.json'), JSON.stringify(indexPayload), 'utf8');
    console.log(`✅ Gravado: ${outPath}`);
  } catch (err) {
    console.warn(`⚠️ Não foi possível gravar em ${outPath}: ${err.message}`);
  }
});

const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
console.log(`\n🎉 Concluído com sucesso em ${elapsed}s!`);
console.log(`📊 Total de skills indexadas: ${allSkills.length}`);
console.log(`📂 Categorias:`, indexPayload.categories);
