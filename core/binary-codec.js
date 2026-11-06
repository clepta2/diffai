/**
 * DiffAI Compact Binary Codec (core/binary-codec.js)
 * Serializador/Deserializador Binário de Alta Densidade (Zero-Dependency)
 * Otimizado para Streaming de Tokens e Comunicação Agent-to-Agent (A2A).
 */

const { Buffer } = require('buffer');

const TYPE_NULL = 0x01;
const TYPE_TRUE = 0x02;
const TYPE_FALSE = 0x03;
const TYPE_UINT8 = 0x04;
const TYPE_INT32 = 0x05;
const TYPE_FLOAT64 = 0x06;
const TYPE_STR_SHORT = 0x07; // Len <= 255 (1 byte)
const TYPE_STR_MED = 0x08;   // Len <= 65535 (2 bytes)
const TYPE_STR_LONG = 0x09;  // Len > 65535 (4 bytes)
const TYPE_ARRAY = 0x0a;     // uint16 count
const TYPE_OBJECT = 0x0b;    // uint16 keyCount
const TYPE_BUFFER = 0x0c;    // uint32 byteLength

class BinaryCodec {
  static encode(val) {
    if (val === null || val === undefined) {
      return Buffer.from([TYPE_NULL]);
    }
    if (val === true) return Buffer.from([TYPE_TRUE]);
    if (val === false) return Buffer.from([TYPE_FALSE]);

    if (typeof val === 'number') {
      if (Number.isInteger(val)) {
        if (val >= 0 && val <= 255) {
          return Buffer.from([TYPE_UINT8, val]);
        }
        if (val >= -2147483648 && val <= 2147483647) {
          const b = Buffer.alloc(5);
          b[0] = TYPE_INT32;
          b.writeInt32BE(val, 1);
          return b;
        }
      }
      const b = Buffer.alloc(9);
      b[0] = TYPE_FLOAT64;
      b.writeDoubleBE(val, 1);
      return b;
    }

    if (typeof val === 'string') {
      const strBuf = Buffer.from(val, 'utf8');
      const len = strBuf.length;
      if (len <= 255) {
        const b = Buffer.alloc(2 + len);
        b[0] = TYPE_STR_SHORT;
        b[1] = len;
        strBuf.copy(b, 2);
        return b;
      } else if (len <= 65535) {
        const b = Buffer.alloc(3 + len);
        b[0] = TYPE_STR_MED;
        b.writeUInt16BE(len, 1);
        strBuf.copy(b, 3);
        return b;
      } else {
        const b = Buffer.alloc(5 + len);
        b[0] = TYPE_STR_LONG;
        b.writeUInt32BE(len, 1);
        strBuf.copy(b, 5);
        return b;
      }
    }

    if (Buffer.isBuffer(val)) {
      const b = Buffer.alloc(5 + val.length);
      b[0] = TYPE_BUFFER;
      b.writeUInt32BE(val.length, 1);
      val.copy(b, 5);
      return b;
    }

    if (Array.isArray(val)) {
      const parts = [Buffer.alloc(3)];
      parts[0][0] = TYPE_ARRAY;
      parts[0].writeUInt16BE(Math.min(val.length, 65535), 1);
      for (const item of val) parts.push(BinaryCodec.encode(item));
      return Buffer.concat(parts);
    }

    if (typeof val === 'object') {
      const keys = Object.keys(val);
      const parts = [Buffer.alloc(3)];
      parts[0][0] = TYPE_OBJECT;
      parts[0].writeUInt16BE(Math.min(keys.length, 65535), 1);
      for (const k of keys) {
        parts.push(BinaryCodec.encode(k));
        parts.push(BinaryCodec.encode(val[k]));
      }
      return Buffer.concat(parts);
    }

    return Buffer.from([TYPE_NULL]);
  }

  static decode(buf, offset = 0) {
    if (offset >= buf.length) return { val: null, nextOffset: offset };
    const type = buf[offset++];

    switch (type) {
      case TYPE_NULL:
        return { val: null, nextOffset: offset };
      case TYPE_TRUE:
        return { val: true, nextOffset: offset };
      case TYPE_FALSE:
        return { val: false, nextOffset: offset };
      case TYPE_UINT8:
        return { val: buf[offset], nextOffset: offset + 1 };
      case TYPE_INT32:
        return { val: buf.readInt32BE(offset), nextOffset: offset + 4 };
      case TYPE_FLOAT64:
        return { val: buf.readDoubleBE(offset), nextOffset: offset + 8 };
      case TYPE_STR_SHORT: {
        const len = buf[offset++];
        const val = buf.toString('utf8', offset, offset + len);
        return { val, nextOffset: offset + len };
      }
      case TYPE_STR_MED: {
        const len = buf.readUInt16BE(offset);
        offset += 2;
        const val = buf.toString('utf8', offset, offset + len);
        return { val, nextOffset: offset + len };
      }
      case TYPE_STR_LONG: {
        const len = buf.readUInt32BE(offset);
        offset += 4;
        const val = buf.toString('utf8', offset, offset + len);
        return { val, nextOffset: offset + len };
      }
      case TYPE_BUFFER: {
        const len = buf.readUInt32BE(offset);
        offset += 4;
        const val = buf.subarray(offset, offset + len);
        return { val, nextOffset: offset + len };
      }
      case TYPE_ARRAY: {
        const count = buf.readUInt16BE(offset);
        offset += 2;
        const arr = [];
        for (let i = 0; i < count; i++) {
          const item = BinaryCodec.decode(buf, offset);
          arr.push(item.val);
          offset = item.nextOffset;
        }
        return { val: arr, nextOffset: offset };
      }
      case TYPE_OBJECT: {
        const count = buf.readUInt16BE(offset);
        offset += 2;
        const obj = {};
        for (let i = 0; i < count; i++) {
          const k = BinaryCodec.decode(buf, offset);
          offset = k.nextOffset;
          const v = BinaryCodec.decode(buf, offset);
          offset = v.nextOffset;
          obj[k.val] = v.val;
        }
        return { val: obj, nextOffset: offset };
      }
      default:
        return { val: null, nextOffset: offset };
    }
  }

  static toBinary(obj) {
    return BinaryCodec.encode(obj);
  }

  static fromBinary(buf) {
    return BinaryCodec.decode(buf, 0).val;
  }
}

module.exports = BinaryCodec;

