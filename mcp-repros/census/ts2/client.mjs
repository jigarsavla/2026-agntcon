// TypeScript SDK 2.x runtime check: @modelcontextprotocol/client 2.3.1 over stdio.
// Usage: node client.mjs <run_dir> <server_python> <with|without>
// "with": the app puts a W3C traceparent into _meta itself. "without": plain callTool.
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport } from '@modelcontextprotocol/client/stdio';
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
const client = new Client({ name: 'ts2-census', version: '0.0.0' });
await client.connect(transport);
const params = { name: 'create_order', arguments: { order_id: 'o1' } };
if (mode === 'with') params._meta = { traceparent };
const res = await client.callTool(params);
appendFileSync(`${run}/client.jsonl`, JSON.stringify({ side: 'client', sdk: '@modelcontextprotocol/client 2.3.1', mode, app_trace: mode === 'with' ? traceId : null, ok: !res.isError }) + '\n');
await new Promise(r => setTimeout(r, 500));
await client.close();
