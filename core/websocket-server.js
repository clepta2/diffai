/**
 * DiffAI Native RFC 6455 WebSocket Engine (core/websocket-server.js)
 * Servidor WebSocket Bidirecional Full-Duplex de Zero Latência (Zero-Dependency)
 */

const crypto = require('crypto');
const BinaryCodec = require('./binary-codec');

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function createFrame(payload, isBinary = false) {
  const buf = isBinary ? (Buffer.isBuffer(payload) ? payload : BinaryCodec.encode(payload)) : Buffer.from(payload, 'utf8');
  const len = buf.length;
  const opcode = isBinary ? 0x82 : 0x81; // 0x81 text, 0x82 binary (FIN bit set)

  let header;
  if (len < 126) {
    header = Buffer.from([opcode, len]);
  } else if (len <= 0xffff) {
    header = Buffer.alloc(4);
    header[0] = opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  return Buffer.concat([header, buf]);
}

function parseFrame(buffer) {
  if (buffer.length < 2) return null;
  const firstByte = buffer[0];
  const secondByte = buffer[1];
  const fin = Boolean(firstByte & 0x80);
  const opcode = firstByte & 0x0f;
  const masked = Boolean(secondByte & 0x80);
  let payloadLength = secondByte & 0x7f;
  let offset = 2;

  if (payloadLength === 126) {
    if (buffer.length < 4) return null;
    payloadLength = buffer.readUInt16BE(2);
    offset = 4;
  } else if (payloadLength === 127) {
    if (buffer.length < 10) return null;
    payloadLength = Number(buffer.readBigUInt64BE(2));
    offset = 10;
  }

  if (masked) {
    if (buffer.length < offset + 4 + payloadLength) return null;
    const maskKey = buffer.subarray(offset, offset + 4);
    offset += 4;
    const payload = buffer.subarray(offset, offset + payloadLength);
    const unmasked = Buffer.alloc(payloadLength);
    for (let i = 0; i < payloadLength; i++) {
      unmasked[i] = payload[i] ^ maskKey[i % 4];
    }
    return { opcode, fin, text: unmasked.toString('utf8'), raw: unmasked, totalLength: offset + payloadLength };
  } else {
    if (buffer.length < offset + payloadLength) return null;
    const payload = buffer.subarray(offset, offset + payloadLength);
    return { opcode, fin, text: payload.toString('utf8'), raw: payload, totalLength: offset + payloadLength };
  }
}

function setupWebSocketServer(httpServer, orchestrator, options = {}) {
  const masterKey = options.masterKey || 'sk-diffai-master-key';

  httpServer.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    if (url.pathname !== '/ws' && url.pathname !== '/ws/pipeline') {
      socket.destroy();
      return;
    }

    const key = req.headers['sec-websocket-key'];
    if (!key) {
      socket.destroy();
      return;
    }

    // Handshake RFC 6455
    const acceptKey = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
    const headers = [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${acceptKey}`,
      '\r\n'
    ];
    socket.write(headers.join('\r\n'));

    // Configurações do socket TCP
    socket.setNoDelay(true);
    socket.setKeepAlive(true, 30000);

    let socketBuf = Buffer.alloc(0);

    const send = (data, isBinary = false) => {
      if (socket.destroyed || !socket.writable) return;
      const payload = typeof data === 'object' && !isBinary ? JSON.stringify(data) : data;
      socket.write(createFrame(payload, isBinary));
    };

    // Keepalive ping
    const pingInterval = setInterval(() => {
      if (socket.destroyed) {
        clearInterval(pingInterval);
        return;
      }
      socket.write(Buffer.from([0x89, 0x00])); // PING frame
    }, 25000);
    if (pingInterval.unref) pingInterval.unref();

    socket.on('data', async (chunk) => {
      socketBuf = Buffer.concat([socketBuf, chunk]);

      while (socketBuf.length >= 2) {
        const frame = parseFrame(socketBuf);
        if (!frame) break;
        socketBuf = socketBuf.subarray(frame.totalLength);

        // Close frame
        if (frame.opcode === 0x08) {
          socket.write(Buffer.from([0x88, 0x00])); // CLOSE ack
          socket.end();
          return;
        }

        // Pong frame
        if (frame.opcode === 0x0a) continue;

        // Ping frame -> responde pong
        if (frame.opcode === 0x09) {
          socket.write(Buffer.from([0x8a, 0x00])); // PONG frame
          continue;
        }

        // Text or Binary Frame
        if (frame.opcode === 0x01 || frame.opcode === 0x02) {
          let message;
          try {
            message = frame.opcode === 0x02 ? BinaryCodec.fromBinary(frame.raw) : JSON.parse(frame.text);
          } catch (_) {
            message = { action: 'chat', prompt: frame.text };
          }

          handleClientMessage(message, send, orchestrator);
        }
      }
    });

    socket.on('close', () => clearInterval(pingInterval));
    socket.on('error', () => {
      clearInterval(pingInterval);
      socket.destroy();
    });

    send({ type: 'connected', service: 'DiffAI Full-Duplex Engine v3.2', timestamp: Date.now() });
  });
}

async function handleClientMessage(msg, send, orchestrator) {
  const action = msg.action || 'chat';
  const requestId = msg.id || ('req_' + Date.now());

  if (action === 'ping') {
    send({ type: 'pong', id: requestId, timestamp: Date.now() });
    return;
  }

  if (action === 'health') {
    send({
      type: 'health',
      id: requestId,
      cluster: orchestrator.getModelHealthReport(),
      memory: orchestrator.getMemoryReport()
    });
    return;
  }

  if (action === 'pipeline') {
    // Execução da Esteira Contínua em Tempo Real Full-Duplex
    const taskPrompt = msg.prompt || msg.message || '';
    send({ type: 'pipeline_start', id: requestId, timestamp: Date.now() });

    try {
      const result = await orchestrator.runContinuousPipeline({
        taskPrompt,
        producerModel: msg.producer || 'groq/openai/gpt-oss-120b',
        consumerModel: msg.consumer || 'ag/claude-sonnet-4-6',
        consumerConcurrency: msg.concurrency || 2,
        onProducerToken: (t) => {
          send({ type: 'producer_token', id: requestId, token: t });
        },
        onProducerUnit: (idx, line) => {
          send({ type: 'producer_unit', id: requestId, index: idx, content: line });
        },
        onConsumerStart: (idx) => {
          send({ type: 'consumer_start', id: requestId, index: idx });
        },
        onConsumerUnitComplete: (item) => {
          send({
            type: 'consumer_unit_done',
            id: requestId,
            index: item.index,
            source: item.source,
            processed: item.processed,
            duration: item.duration
          });
        }
      });

      send({
        type: 'pipeline_done',
        id: requestId,
        totalDuration: result.totalDuration,
        itemsProcessed: result.itemsProcessed,
        results: result.results
      });
    } catch (err) {
      send({ type: 'error', id: requestId, message: err.message });
    }
    return;
  }

  if (action === 'verify') {
    const taskPrompt = msg.prompt || msg.message || '';
    send({ type: 'verify_start', id: requestId });
    try {
      const vRes = await orchestrator.runSpeculativeVerification(taskPrompt, {
        onDraftToken: (t) => send({ type: 'draft_token', id: requestId, token: t })
      });
      send({
        type: 'verify_done',
        id: requestId,
        reply: vRes.reply,
        draftDuration: vRes.draftDuration,
        verificationDuration: vRes.verificationDuration,
        totalDuration: vRes.totalDuration
      });
    } catch (err) {
      send({ type: 'error', id: requestId, message: err.message });
    }
    return;
  }

  // Ação padrão: Chat Streaming Direto
  const model = msg.model || 'diffai-racing';
  const prompt = msg.prompt || msg.message || '';

  try {
    send({ type: 'chat_start', id: requestId, model });
    const res = await orchestrator.streamQueryOmniRouter(
      model,
      prompt,
      (token) => send({ type: 'token', id: requestId, token }),
      msg.system || '',
      { a2a: msg.a2a !== false, autoSkill: msg.autoSkill || false }
    );
    send({ type: 'chat_done', id: requestId, model: res.model, duration: res.totalDuration, ttft: res.ttft });
  } catch (err) {
    send({ type: 'error', id: requestId, message: err.message });
  }
}

module.exports = {
  setupWebSocketServer,
  createFrame,
  parseFrame
};
