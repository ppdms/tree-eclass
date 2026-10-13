import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { join } from 'node:path';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const inputSHA256 = '3e184ecd20404e275f89c21bf9dc428013ec61d0d109efcf51fb047242215542';
const callbackSHA256 = '6c422caedb21078ab5254d797d8a968f87637b9230aa46ec04e7cf5c1538636e';
const blockSHA256 = '22dd8fbd78c94db917d409c8cbfddc372082844f6df8b981200096687808b3df';
const removal = new Set([...Array.from({ length: 12 }, (_, i) => 36 + i), 54, 77]);

function functionHashes(b, module) {
  const hashes = new Map();
  for (let i = 0; i < module.getNumFunctions(); i++) {
    const info = b.getFunctionInfo(module.getFunctionByIndex(i));
    if (info.body) hashes.set(info.name, sha256(b.emitText(info.body)));
  }
  return hashes;
}

// These are the child-expression fields present in the hash-pinned module.
// Const.value and LocalGet.index are immediates, unlike TableGet/Set.index.
function children(b, ref) {
  const info = b.getExpressionInfo(ref);
  const result = [];
  for (const key of ['children', 'operands']) {
    if (Array.isArray(info[key])) result.push(...info[key]);
  }
  const fields = [
    'condition',
    'ifTrue',
    'ifFalse',
    'body',
    'left',
    'right',
    'ptr',
    'target',
    'dest',
    'source',
    'size',
    'tuple',
    'ref',
  ];
  for (const key of fields) {
    if (typeof info[key] === 'number' && info[key]) result.push(info[key]);
  }
  if (info.id !== b.ConstId && typeof info.value === 'number' && info.value) result.push(info.value);
  if ([b.TableGetId, b.TableSetId].includes(info.id) && info.index) result.push(info.index);
  return result;
}

function measureBlock(b, body) {
  let block = 0;
  let matches = 0;
  const find = (ref) => {
    const info = b.getExpressionInfo(ref);
    if (
      info.id === b.BlockId &&
      info.children.length === 102 &&
      b.emitText(info.children[45]).includes('call $fimport$53')
    ) {
      if (sha256(b.emitText(ref)) !== blockSHA256) throw new Error('Pinned callback block changed');
      block = ref;
      matches++;
      return;
    }
    for (const child of children(b, ref)) find(child);
  };
  find(body);
  if (matches !== 1) throw new Error('Expected exactly one pinned callback argument block');
  return block;
}

function removeStyleArgument(b, module, block) {
  const refs = b.getExpressionInfo(block).children;
  if (refs.length !== 102) throw new Error('Unexpected callback statement count');
  const sourceNodeStore = b.getExpressionInfo(refs[38]);
  const nodeTee = b.getExpressionInfo(sourceNodeStore.value);
  if (sourceNodeStore.id !== b.StoreId || nodeTee.id !== b.LocalSetId || !nodeTee.isTee || nodeTee.index !== 117) {
    throw new Error('Unexpected preserved bigint node load');
  }
  const next = [];
  for (let i = 0; i < refs.length; i++) {
    if (i === 36) next.push(module.local.set(117, nodeTee.value));
    if (!removal.has(i)) next.push(refs[i]);
  }
  b.Block.setChildren(block, next);
  b.Block.finalize(block);
  return refs.map((ref) => sha256(b.emitText(ref)));
}

function verifyChangedFunctions(b, module, original) {
  const changed = [];
  for (const [name, hash] of original) {
    const now = b.getFunctionInfo(module.getFunction(name));
    if (sha256(b.emitText(now.body)) !== hash) changed.push(name);
  }
  if (changed.length !== 1 || changed[0] !== '26') throw new Error('Unrelated native body changed');
  return changed;
}

function finish(module, changed, statementHashes) {
  // Ordinary Style construction and tree-owned styles still retain their helpers.
  module.runPasses(['dce', 'remove-unused-module-elements', 'vacuum']);
  if (!module.validate()) throw new Error('Native four-argument measure ABI is invalid');
  const output = module.emitBinary();
  const compiled = new WebAssembly.Module(output);
  if (WebAssembly.Module.imports(compiled).some((item) => item.name === '__wbg_style_new')) {
    throw new Error('Owned callback Style transfer import survived native removal');
  }
  return {
    wasm: output,
    proof: {
      inputSHA256,
      callbackSHA256,
      blockSHA256,
      changedFunctionsBeforeDCE: changed,
      removedStatementIndices: [...removal],
      removedStatementHashes: [...removal].map((i) => statementHashes[i]),
      replacement: 'Preserve exact original i64 node load as local.set 117; no stack Style copy',
      removed: [
        'Style deep-clone call 191',
        '400-byte stack memory.copy',
        '416-byte Box allocation/copy call 323',
        'Style wrapper transfer import',
        'Style externref table allocation',
        'fifth argument push',
        'Style externref release',
      ],
      outputSHA256: sha256(output),
      outputBytes: output.byteLength,
    },
  };
}

// Source equivalent: remove native_style.clone()/JsStyle boxing and the fifth
// callback argument in pinned taffy-layout src/tree.rs:1169-1187. The borrowed
// native Style still drives compute_leaf_layout. This is not a JS wrapper filter.
export async function removeMeasureStyle(wasm, binaryenPath) {
  if (sha256(wasm) !== inputSHA256) throw new Error('Measure ABI requires the exact pinned trim WASM');
  const { default: b } = await import(pathToFileURL(join(binaryenPath, 'index.js')));
  const module = b.readBinary(wasm);
  module.setFeatures(
    b.Features.MutableGlobals |
      b.Features.NontrappingFPToInt |
      b.Features.BulkMemory |
      b.Features.BulkMemoryOpt |
      b.Features.SignExt |
      b.Features.ReferenceTypes |
      b.Features.Multivalue,
  );
  try {
    const func = b.getFunctionInfo(module.getFunction('26'));
    if (sha256(b.emitText(func.body)) !== callbackSHA256) throw new Error('Pinned measure function changed');
    const original = functionHashes(b, module);
    const hashes = removeStyleArgument(b, module, measureBlock(b, func.body));
    return finish(module, verifyChangedFunctions(b, module, original), hashes);
  } finally {
    module.dispose();
  }
}
