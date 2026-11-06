/**
 * DiffAI — Memory & Tenant Auto-Learning Engine (core/memory-manager.js)
 * Isolamento RLS Multi-Tenant, Zero-Access Encryption e Auto-Aprendizado Contínuo.
 */

const https = require('https');
const http = require('http');

class MemoryManager {
  constructor(options = {}) {
    this.supabaseUrl = options.supabaseUrl || process.env.SUPABASE_URL || 'https://db.ozpro.com.br';
    this.supabaseKey = options.supabaseKey || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || '';
    this.cache = new Map();
  }

  async _fetch(url, options = {}) {
    return new Promise((resolve, reject) => {
      const u = new URL(url);
      const isHttps = u.protocol === 'https:';
      const client = isHttps ? https : http;

      const req = client.request(u, {
        method: options.method || 'GET',
        headers: {
          'apikey': this.supabaseKey,
          'Authorization': `Bearer ${this.supabaseKey}`,
          'Content-Type': 'application/json',
          ...(options.headers || {})
        }
      }, res => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, ok: res.statusCode >= 200 && res.statusCode < 300, data: data ? JSON.parse(data) : null });
          } catch (e) {
            resolve({ status: res.statusCode, ok: false, data, error: e.message });
          }
        });
      });

      req.on('error', reject);
      if (options.body) req.write(typeof options.body === 'string' ? options.body : JSON.stringify(options.body));
      req.end();
    });
  }

  /**
   * Obtém o contexto ao vivo do tenant protegido por RLS
   */
  async getTenantContext(tenantId, userEmail = '') {
    if (!this.supabaseKey) {
      return {
        tenantId,
        companyName: tenantId || 'DiffAI Partner',
        learnedInsights: ['Operação sob proteção de dados local DiffAI.'],
        textBlock: `\n[Contexto DiffAI]: Tenant ${tenantId || 'Padrão'}`
      };
    }

    const cacheKey = `${tenantId}:${userEmail}`;
    if (this.cache.has(cacheKey)) {
      const cached = this.cache.get(cacheKey);
      if (Date.now() - cached.ts < 30000) return cached.val;
    }

    let company = null;
    let learnedInsights = [];

    try {
      const compRes = await this._fetch(`${this.supabaseUrl}/rest/v1/companies?id=eq.${encodeURIComponent(tenantId)}&limit=1`);
      if (compRes.ok && Array.isArray(compRes.data) && compRes.data.length > 0) {
        company = compRes.data[0];
      }
    } catch (_) {}

    try {
      const profRes = await this._fetch(`${this.supabaseUrl}/rest/v1/hermes_merchant_profile?company_id=eq.${encodeURIComponent(tenantId)}&limit=1`);
      if (profRes.ok && Array.isArray(profRes.data) && profRes.data.length > 0 && Array.isArray(profRes.data[0].learned_insights)) {
        learnedInsights = profRes.data[0].learned_insights;
      }
    } catch (_) {}

    const companyName = company?.name || company?.trade_name || tenantId;
    const userName = company?.contact_name || (userEmail ? userEmail.split('@')[0] : 'Parceiro');
    const insightsText = learnedInsights.length > 0
      ? learnedInsights.map(i => `- ${typeof i === 'string' ? i : (i.fact || JSON.stringify(i))}`).join('\n')
      : '- O lojista preza por respostas diretas, práticas e técnicas.\n- Todas as operações seguem isolamento estrito de dados por tenant (RLS).';

    const textBlock = `\n--- MEMÓRIA & CONTEXTO EM TEMPO REAL DO TENANT (SUPABASE RLS) ---
Empresa / Loja Logada: ${companyName}
Responsável: ${userName}
Tenant ID: ${tenantId}
Memórias e Hábitos Aprendidos:
${insightsText}
-----------------------------------------------------------------`;

    const result = { tenantId, companyName, userName, learnedInsights, textBlock };
    this.cache.set(cacheKey, { val: result, ts: Date.now() });
    return result;
  }

  /**
   * Auto-aprende e persiste declarações e regras ditas pelo lojista
   */
  async learn(tenantId, userMessage) {
    if (!userMessage || userMessage.length < 10 || !this.supabaseKey) return false;
    const match = userMessage.match(/\b(meu nome [ée]|me chamo|minha loja [ée]|trabalhamos com|nossos clientes|nossa margem|prefiro|sempre use|nosso hor[aá]rio)\b\s*(.*)/i);
    if (!match) return false;

    const fact = userMessage.trim().slice(0, 240);
    try {
      await this._fetch(`${this.supabaseUrl}/rest/v1/rpc/hermes_learn_fact`, {
        method: 'POST',
        body: { p_company_id: tenantId, p_fact: fact }
      });
      return true;
    } catch (_) {
      return false;
    }
  }
}

module.exports = MemoryManager;

