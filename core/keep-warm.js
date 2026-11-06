/**
 * DiffAI Keep-Warm Engine (core/keep-warm.js)
 * Mantém os sockets TCP/TLS, conexões HTTP persistentes e nós de IA
 * permanentemente aquecidos na memória RAM (Zero Cold Start / Nunca Esfria).
 */

const http = require('http');

class KeepWarmEngine {
  constructor(options = {}) {
    this.host = options.host || '100.98.113.39';
    this.port = options.port || 20128;
    this.apiKey = options.apiKey || process.env.OMNIROUTER_API_KEY || 'sk-ed50645fd257c652-hrayzt-ae2a2594';
    this.models = options.models || [
      'groq/openai/gpt-oss-120b',
      'ag/claude-sonnet-4-6',
      'gemini/gemini-3.5-flash-lite'
    ];
    this.intervalMs = options.intervalMs || 120000; // 2 minutos
    this.timer = null;
    this.isRunning = false;
    this.stats = {
      totalPings: 0,
      lastPingAt: null,
      results: {}
    };
  }

  pingModel(model) {
    return new Promise((resolve) => {
      const t0 = Date.now();
      const body = JSON.stringify({
        model,
        messages: [{ role: 'user', content: '.' }],
        max_tokens: 1
      });

      const req = http.request({
        hostname: this.host,
        port: this.port,
        path: '/v1/chat/completions',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Length': Buffer.byteLength(body),
          'Connection': 'keep-alive'
        },
        timeout: 10000
      }, res => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => {
          const latency = Date.now() - t0;
          resolve({ model, ok: res.statusCode === 200, status: res.statusCode, latency });
        });
      });

      req.on('timeout', () => {
        req.destroy();
        resolve({ model, ok: false, error: 'timeout', latency: Date.now() - t0 });
      });

      req.on('error', e => resolve({ model, ok: false, error: e.message, latency: Date.now() - t0 }));
      req.write(body);
      req.end();
    });
  }

  async runCycle() {
    this.stats.lastPingAt = new Date().toISOString();
    for (const m of this.models) {
      try {
        const res = await this.pingModel(m);
        this.stats.totalPings++;
        this.stats.results[m] = {
          lastStatus: res.ok ? 'HOT' : 'COLD_FAIL',
          latencyMs: res.latency,
          updatedAt: new Date().toISOString()
        };
      } catch (_) {}
    }
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    console.log(`\n🔥 [Keep-Warm Engine] Ativado! Mantendo ${this.models.length} modelos permanentemente aquecidos na RAM.`);
    this.runCycle(); // Primeiro ciclo imediato
    this.timer = setInterval(() => this.runCycle(), this.intervalMs);
    if (this.timer.unref) this.timer.unref();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.isRunning = false;
    console.log('🧊 [Keep-Warm Engine] Pausado.');
  }

  getStatus() {
    return {
      running: this.isRunning,
      intervalSec: this.intervalMs / 1000,
      totalPings: this.stats.totalPings,
      lastPingAt: this.stats.lastPingAt,
      modelsHot: this.stats.results
    };
  }
}

module.exports = KeepWarmEngine;

