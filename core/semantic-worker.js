/**
 * DiffAI Background Semantic Worker (core/semantic-worker.js)
 * Isolamento de processamento pesado de CPU via worker_threads do Node.js
 * Mantém o Event Loop de I/O de rede a 0.0ms de latência.
 */

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

if (!isMainThread) {
  // Lógica em Thread Secundária de CPU
  parentPort.on('message', (task) => {
    const { id, type, payload } = task;
    try {
      let result = null;

      switch (type) {
        case 'extract_code_blocks': {
          // Extrai todos os blocos de código markdown ```lang ... ```
          const regex = /```([a-zA-Z0-9_\-\.]*)[\r\n]+([\s\S]*?)```/g;
          const blocks = [];
          let match;
          while ((match = regex.exec(payload)) !== null) {
            blocks.push({ lang: match[1] || 'plaintext', code: match[2].trim() });
          }
          result = { blocks, count: blocks.length };
          break;
        }

        case 'syntax_check': {
          // Checagem de balanceamento de chaves {}, colchetes [] e parênteses ()
          const text = payload || '';
          const stack = [];
          let isValid = true;
          let errorPos = null;

          for (let i = 0; i < text.length; i++) {
            const ch = text[i];
            if (ch === '{' || ch === '(' || ch === '[') {
              stack.push({ ch, pos: i });
            } else if (ch === '}' || ch === ')' || ch === ']') {
              const last = stack.pop();
              if (!last || (ch === '}' && last.ch !== '{') || (ch === ')' && last.ch !== '(') || (ch === ']' && last.ch !== '[')) {
                isValid = false;
                errorPos = i;
                break;
              }
            }
          }
          if (stack.length > 0) isValid = false;
          result = { isValid, unclosedCount: stack.length, errorPos };
          break;
        }

        case 'compute_diff': {
          // Linha a linha diff rápido
          const oldLines = (payload.draft || '').split('\n');
          const newLines = (payload.verified || '').split('\n');
          const additions = [];
          const modifications = [];

          newLines.forEach((line, idx) => {
            if (!oldLines.includes(line)) {
              additions.push({ lineIndex: idx + 1, content: line });
            }
          });
          result = {
            oldLineCount: oldLines.length,
            newLineCount: newLines.length,
            changedLines: additions.length,
            additions
          };
          break;
        }

        default:
          result = { ok: true, echo: payload };
      }

      parentPort.postMessage({ id, ok: true, result });
    } catch (err) {
      parentPort.postMessage({ id, ok: false, error: err.message });
    }
  });
} else {
  // Lógica na Thread Principal: Pool de Workers
  let workerInstance = null;
  const pendingRequests = new Map();
  let reqSeq = 0;

  function getWorker() {
    if (!workerInstance) {
      workerInstance = new Worker(__filename);
      workerInstance.on('message', (msg) => {
        const { id, ok, result, error } = msg;
        const cb = pendingRequests.get(id);
        if (cb) {
          pendingRequests.delete(id);
          if (ok) cb.resolve(result);
          else cb.reject(new Error(error));
        }
      });
      workerInstance.on('error', (err) => {
        console.error('[SemanticWorker Error]', err);
        workerInstance = null;
      });
      workerInstance.on('exit', () => {
        workerInstance = null;
      });
    }
    return workerInstance;
  }

  function runCpuTask(type, payload) {
    return new Promise((resolve, reject) => {
      const id = ++reqSeq;
      pendingRequests.set(id, { resolve, reject });
      try {
        const worker = getWorker();
        worker.postMessage({ id, type, payload });
      } catch (err) {
        pendingRequests.delete(id);
        reject(err);
      }
    });
  }

  module.exports = {
    runCpuTask
  };
}
