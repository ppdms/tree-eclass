import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { runInNewContext } from 'node:vm';

const require = createRequire(import.meta.url);
const { webpack } = require('next/dist/compiled/webpack/webpack');
const postcss = require('postcss');
const stylexPostcss = require('@stylexjs/postcss-plugin');
const postcssOptions = require('../postcss.config.cjs').plugins['@stylexjs/postcss-plugin'];
const frontend = fileURLToPath(new URL('../', import.meta.url));

function writeConstants(root, padding) {
	writeFileSync(
		path.join(root, 'src/values.stylex.js'),
		`import * as stylex from '@stylexjs/stylex'; export const layout = stylex.defineConsts({padding: '${padding}px'});`,
	);
}

function watchConstantChange(compiler, root) {
	let first;
	let watcher;
	const processor = postcss([stylexPostcss({ ...postcssOptions, cwd: root })]);
	const result = new Promise((resolve, reject) => {
		watcher = compiler.watch({ aggregateTimeout: 50 }, async (error, stats) => {
			try {
				if (error) throw error;
				assert.equal(stats.hasErrors(), false, stats.toString({ all: false, errors: true }));
				const output = path.join(root, 'dist/result.cjs');
				const module = { exports: {} };
				runInNewContext(readFileSync(output, 'utf8'), { module, exports: module.exports, require });
				const current = module.exports.className;
				assert.ok(current);
				const styles = await processor.process('@stylex;', { from: path.join(root, 'src/entry.css') });
				assert.ok(styles.css.includes(`.${current.split(' ').at(-1)}`));
				assert.ok(styles.messages.some((message) => message.type === 'dir-dependency'));
				if (first === undefined) {
					first = current;
					assert.match(styles.css, /padding-left:10px/);
					writeConstants(root, 20);
				} else {
					assert.equal(current, first, 'Constants retain a stable symbolic class name');
					assert.match(styles.css, /padding-left:20px/);
					assert.doesNotMatch(styles.css, /padding-left:10px/);
					resolve();
				}
			} catch (failure) {
				reject(failure);
			}
		});
	});
	return { result, close: () => new Promise((resolve) => watcher.close(resolve)) };
}

test('StyleX watch rebuilds resolve updated imported constants in CSS', { timeout: 60_000 }, async () => {
	// Match Webpack's resolved resource paths when the temporary directory is symlinked.
	const root = realpathSync(mkdtempSync(path.join(tmpdir(), 'tree-stylex-watch-')));
	mkdirSync(path.join(root, 'src'));
	symlinkSync(path.join(frontend, 'node_modules'), path.join(root, 'node_modules'), 'dir');
	writeConstants(root, 10);
	writeFileSync(
		path.join(root, 'src/index.js'),
		`import * as stylex from '@stylexjs/stylex';
    import {layout} from './values.stylex';
    const styles = stylex.create({box:{paddingLeft:layout.padding}});
    export const className = stylex.props(styles.box).className;`,
	);
	const compiler = webpack({
		mode: 'development',
		context: root,
		entry: './src/index.js',
		target: 'node',
		devtool: false,
		resolve: { modules: [path.join(frontend, 'node_modules'), 'node_modules'] },
		output: { path: path.join(root, 'dist'), filename: 'result.cjs', library: { type: 'commonjs2' } },
		module: {
			rules: [{ test: /\.js$/, include: path.join(root, 'src'), use: path.join(frontend, 'stylex-loader.cjs') }],
		},
	});
	const watching = watchConstantChange(compiler, root);
	try {
		await watching.result;
	} finally {
		await watching.close();
		await new Promise((resolve) => compiler.close(resolve));
		rmSync(root, { recursive: true, force: true });
	}
});
