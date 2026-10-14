// 从仪表盘 HTML 中提取 SNAPS 的某个日期键，输出为 JSON 文件。
// 用法: node _snap_dump.js <YYYY-MM-DD> <outJsonPath> [htmlPath]
const fs = require('fs');
const path = require('path');

const date = process.argv[2];
const out = process.argv[3];
const htmlPath = process.argv[4] || path.join(__dirname, 'outputs', 'A股行业轮动与资金流向监控仪表盘.html');

const src = fs.readFileSync(htmlPath, 'utf8');
const marker = 'const SNAPS = {';
const start = src.indexOf(marker);
if (start < 0) { console.error('SNAPS not found'); process.exit(2); }

let i = start + marker.length - 1; // 指向 '{'
let depth = 0, inStr = null, esc = false;
let end = -1;
for (; i < src.length; i++) {
  const ch = src[i];
  if (inStr) {
    if (esc) { esc = false; continue; }
    if (ch === '\\') { esc = true; continue; }
    if (ch === inStr) { inStr = null; }
    continue;
  }
  if (ch === '"' || ch === "'" || ch === '`') { inStr = ch; continue; }
  if (ch === '{') depth++;
  else if (ch === '}') { depth--; if (depth === 0) { end = i; break; } }
}
if (end < 0) { console.error('SNAPS brace mismatch'); process.exit(3); }

const objText = src.slice(start + marker.length - 1, end + 1);
let SNAPS;
try {
  SNAPS = new Function('return ' + objText)();
} catch (e) {
  console.error('eval failed: ' + e.message);
  process.exit(4);
}

const keys = Object.keys(SNAPS).sort();
// 始终输出一份键清单，供 Python 侧统计日期键数
fs.writeFileSync(path.join(path.dirname(out), '_tmp_keys.json'),
  JSON.stringify({ keys }, null, 0), 'utf8');
if (!date) {
  fs.writeFileSync(out, JSON.stringify({ keys }, null, 1), 'utf8');
  console.error('keys=' + keys.length);
  process.exit(0);
}
const snap = SNAPS[date];
if (!snap) { console.error('date not found: ' + date + ' (available: ' + keys.slice(-3).join(',') + ')'); process.exit(5); }
fs.writeFileSync(out, JSON.stringify(snap, null, 1), 'utf8');
console.error('dumped ' + date + ' -> ' + out + ' (keys=' + keys.length + ')');
