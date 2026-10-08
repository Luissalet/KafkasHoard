// Local native OCR bridge. No file paths, network calls or image contents in logs.
import { createEngine } from '@arcships/light-ocr';

const chunks = [];
let length = 0;
for await (const chunk of process.stdin) {
  length += chunk.length;
  if (length > 24_000_000) throw new Error('OCR image exceeds byte budget');
  chunks.push(chunk);
}
const engine = await createEngine({ execution: { provider: 'cpu' } });
try {
  const result = await engine.recognizeEncoded(Buffer.concat(chunks));
  process.stdout.write(JSON.stringify({ lines: result.lines }));
} finally {
  await engine.close();
}
