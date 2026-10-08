// TypeScript SDK 1.x runtime check: @modelcontextprotocol/sdk 1.32.1 over stdio.
// Usage: node client.mjs <run_dir> <server_python> <with|without>
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { appendFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';

const [run, serverPy, mode] = process.argv.slice(2);
const here = new URL('..', import.meta.url).pathname;
const traceId = randomBytes(16).toString('hex');
const spanId = randomBytes(8).toString('hex');
const traceparent = `00-${traceId}-${spanId}-01`;

const transport = new StdioClientTransport({
  command: 'python3',
  args: [`${here}tee_stdio.py`, `${run}/requests.jsonl`, serverPy, `${here}census_server.py`],
  env: { ...process.env, LEDGER: `${run}/ledger.jsonl`, SPANS: `${run}/spans.jsonl`, SLOW_S: '0' },
});
const client = new Client({ name: 'ts1-census', version: '0.0.0' });
await client.connect(transport);
const params = { name: 'create_order', arguments: { order_id: 'o1' } };
if (mode === 'with') params._meta = { traceparent };
const res = await client.callTool(params);
appendFileSync(`${run}/client.jsonl`, JSON.stringify({ side: 'client', sdk: '@modelcontextprotocol/sdk 1.32.1', mode, app_trace: mode === 'with' ? traceId : null, ok: !res.isError }) + '\n');
await new Promise(r => setTimeout(r, 500));
await client.close();
