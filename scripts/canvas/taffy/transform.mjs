// Retained binding bodies are never rewritten; callback Style is removed natively.
class BindingTransforms {
  constructor(ts, surface) {
    this.ts = ts;
    this.surface = surface;
    this.retainedExports = new Set(surface.retainedWasmExports);
    this.enumNames = new Set(surface.enums);
    this.classNames = new Set(Object.keys(surface.classes));
    this.initNames = new Set(['__wbg_load', '__wbg_get_imports', '__wbg_finalize_init', 'initSync', '__wbg_init']);
    this.printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed, removeComments: true });
  }

  parse(name, text, kind) {
    const { ts } = this;
    const source = ts.createSourceFile(name, text, ts.ScriptTarget.Latest, true, kind);
    if (source.parseDiagnostics.length) {
      throw new Error(`${name}: ${ts.flattenDiagnosticMessageText(source.parseDiagnostics[0].messageText, '\n')}`);
    }
    return source;
  }

  memberName(node) {
    const { ts } = this;
    if (ts.isConstructorDeclaration(node)) return 'constructor';
    if (node.name && (ts.isIdentifier(node.name) || ts.isStringLiteral(node.name))) return node.name.text;
    if (node.name && ts.isComputedPropertyName(node.name)) {
      const expression = node.name.expression;
      if (
        ts.isPropertyAccessExpression(expression) &&
        ts.isIdentifier(expression.expression) &&
        expression.expression.text === 'Symbol' &&
        expression.name.text === 'dispose'
      )
        return '[Symbol.dispose]';
    }
    throw new Error('Unexpected generated class member; review the pinned ABI');
  }

  isExported(node) {
    return node.modifiers?.some((modifier) => modifier.kind === this.ts.SyntaxKind.ExportKeyword) ?? false;
  }

  importName(statement) {
    const { ts } = this;
    if (
      !ts.isExpressionStatement(statement) ||
      !ts.isBinaryExpression(statement.expression) ||
      statement.expression.operatorToken.kind !== ts.SyntaxKind.EqualsToken
    )
      return null;
    const left = statement.expression.left;
    if (!ts.isPropertyAccessExpression(left) || !ts.isPropertyAccessExpression(left.expression)) return null;
    const namespace = left.expression;
    if (
      !ts.isIdentifier(namespace.expression) ||
      namespace.expression.text !== 'imports' ||
      namespace.name.text !== 'wbg'
    )
      return null;
    return left.name.text;
  }

  narrowRuntime(source) {
    const { ts, surface, classNames, enumNames } = this;
    const seenClasses = new Set();
    const seenEnums = new Set();
    const statements = source.statements.map((statement) => {
      if (ts.isClassDeclaration(statement)) {
        const name = statement.name?.text;
        if (!classNames.has(name)) throw new Error(`Unexpected generated class ${name}`);
        seenClasses.add(name);
        const names = new Set(surface.classes[name].runtime);
        const members = statement.members.filter((member) => names.has(this.memberName(member)));
        for (const expected of names) {
          if (!members.some((member) => this.memberName(member) === expected)) {
            throw new Error(`Missing ${name}.${expected}`);
          }
        }
        return ts.factory.updateClassDeclaration(
          statement,
          statement.modifiers,
          statement.name,
          statement.typeParameters,
          statement.heritageClauses,
          members,
        );
      }
      if (this.isExported(statement) && ts.isVariableStatement(statement)) {
        for (const declaration of statement.declarationList.declarations) {
          if (!ts.isIdentifier(declaration.name) || !enumNames.has(declaration.name.text)) {
            throw new Error('Unexpected exported runtime value');
          }
          seenEnums.add(declaration.name.text);
        }
      }
      return statement;
    });
    if (seenClasses.size !== classNames.size || seenEnums.size !== enumNames.size) {
      throw new Error('Incomplete generated runtime surface');
    }
    return ts.factory.updateSourceFile(source, statements);
  }

  collectImports(source) {
    const { ts } = this;
    const functionNode = source.statements.find(
      (statement) => ts.isFunctionDeclaration(statement) && statement.name?.text === '__wbg_get_imports',
    );
    if (!functionNode?.body) throw new Error('Missing generated import initializer');
    const stubs = new Map();
    for (const statement of functionNode.body.statements) {
      const name = this.importName(statement);
      if (name) {
        if (stubs.has(name)) throw new Error(`Duplicate JS import stub ${name}`);
        stubs.set(name, statement);
      }
    }
    return stubs;
  }

  definitions(source) {
    const { ts } = this;
    const result = new Map();
    for (const statement of source.statements) {
      if ((ts.isFunctionDeclaration(statement) || ts.isClassDeclaration(statement)) && statement.name) {
        result.set(statement.name.text, statement);
      } else if (ts.isVariableStatement(statement)) {
        for (const declaration of statement.declarationList.declarations) {
          if (!ts.isIdentifier(declaration.name)) throw new Error('Unexpected generated top-level binding pattern');
          result.set(declaration.name.text, statement);
        }
      }
    }
    return result;
  }

  identifierIsValue(node) {
    const { ts } = this;
    const parent = node.parent;
    if (ts.isPropertyAccessExpression(parent) && parent.name === node) return false;
    if (
      (ts.isVariableDeclaration(parent) ||
        ts.isParameter(parent) ||
        ts.isFunctionDeclaration(parent) ||
        ts.isClassDeclaration(parent) ||
        ts.isMethodDeclaration(parent) ||
        ts.isGetAccessorDeclaration(parent) ||
        ts.isSetAccessorDeclaration(parent) ||
        ts.isPropertyAssignment(parent)) &&
      parent.name === node
    )
      return false;
    return true;
  }

  references(node, defs) {
    const result = new Set();
    const visit = (child) => {
      if (this.ts.isIdentifier(child) && defs.has(child.text) && this.identifierIsValue(child)) result.add(child.text);
      this.ts.forEachChild(child, visit);
    };
    visit(node);
    return result;
  }

  reachableNodes(roots, defs) {
    const nodes = new Set(roots);
    const pending = [...roots];
    while (pending.length) {
      const node = pending.pop();
      for (const name of this.references(node, defs)) {
        const definition = defs.get(name);
        if (!nodes.has(definition)) {
          nodes.add(definition);
          pending.push(definition);
        }
      }
    }
    return nodes;
  }

  wasmReferences(nodes) {
    const { ts } = this;
    const result = new Set();
    const visit = (node) => {
      if (ts.isPropertyAccessExpression(node) && ts.isIdentifier(node.expression) && node.expression.text === 'wasm') {
        result.add(node.name.text);
      } else if (
        ts.isElementAccessExpression(node) &&
        ts.isIdentifier(node.expression) &&
        node.expression.text === 'wasm'
      ) {
        if (!ts.isStringLiteral(node.argumentExpression)) throw new Error('Dynamic WASM export access needs review');
        result.add(node.argumentExpression.text);
      }
      ts.forEachChild(node, visit);
    };
    for (const node of nodes) visit(node);
    return result;
  }

  assertRetainedReferences(names) {
    for (const name of names) {
      if (!this.retainedExports.has(name)) throw new Error(`Retained JS requires unlisted WASM export ${name}`);
    }
  }

  graph(source, wasmExports, wasmImports) {
    const defs = this.definitions(source);
    const stubs = this.collectImports(source);
    const graphNodes = wasmExports.map((item) => ({
      name: `export:${item.name}`,
      export: item.name,
      ...(this.retainedExports.has(item.name) ? { root: true } : {}),
    }));
    for (const item of wasmImports) {
      if (item.module !== 'wbg' || item.kind !== 'function' || !stubs.has(item.name)) {
        throw new Error(`Unexpected WASM import ${item.module}.${item.name}`);
      }
      const exportsUsed = this.wasmReferences(this.reachableNodes([stubs.get(item.name)], defs));
      this.assertRetainedReferences(exportsUsed);
      graphNodes.push({
        name: `import:${item.module}:${item.name}`,
        import: [item.module, item.name],
        reaches: [...exportsUsed].sort().map((name) => `export:${name}`),
      });
    }
    return graphNodes;
  }

  runtime(text, liveImports) {
    const { ts } = this;
    let source = this.narrowRuntime(this.parse('taffy_wasm.js', text, ts.ScriptKind.JS));
    const live = new Set(liveImports.map((item) => item.name));
    const stubs = this.collectImports(source);
    for (const item of liveImports) {
      if (item.module !== 'wbg' || item.kind !== 'function' || !stubs.has(item.name)) {
        throw new Error(`Missing live import stub ${item.module}.${item.name}`);
      }
    }
    source = ts.factory.updateSourceFile(
      source,
      source.statements.map((statement) => {
        if (!ts.isFunctionDeclaration(statement) || statement.name?.text !== '__wbg_get_imports') return statement;
        const body = ts.factory.updateBlock(
          statement.body,
          statement.body.statements.filter((item) => {
            const name = this.importName(item);
            return !name || live.has(name);
          }),
        );
        return ts.factory.updateFunctionDeclaration(
          statement,
          statement.modifiers,
          statement.asteriskToken,
          statement.name,
          statement.typeParameters,
          statement.parameters,
          statement.type,
          body,
        );
      }),
    );
    const defs = this.definitions(source);
    const roots = source.statements.filter((statement) => {
      if (ts.isClassDeclaration(statement)) return true;
      if (ts.isFunctionDeclaration(statement)) return this.initNames.has(statement.name?.text);
      if (ts.isVariableStatement(statement)) return this.isExported(statement);
      // Retain polyfills, Symbol.dispose, exports and every other initializer verbatim.
      return true;
    });
    const reached = this.reachableNodes(roots, defs);
    this.assertRetainedReferences(this.wasmReferences(reached));
    const result = ts.factory.updateSourceFile(
      source,
      source.statements.filter((statement) => reached.has(statement)),
    );
    return this.printer.printFile(result);
  }

  classDeclaration(statement) {
    const name = statement.name?.text;
    if (!this.classNames.has(name)) throw new Error(`Unexpected declaration class ${name}`);
    const names = new Set(this.surface.classes[name].declarations);
    const members = statement.members.filter((member) => names.has(this.memberName(member)));
    for (const expected of names) {
      if (!members.some((member) => this.memberName(member) === expected)) {
        throw new Error(`Missing declaration ${name}.${expected}`);
      }
    }
    return this.ts.factory.updateClassDeclaration(
      statement,
      statement.modifiers,
      statement.name,
      statement.typeParameters,
      statement.heritageClauses,
      members,
    );
  }

  declarations(text) {
    const { ts, retainedExports } = this;
    const source = this.parse('taffy_wasm.d.ts', text, ts.ScriptKind.TS);
    const statements = source.statements.flatMap((statement) => {
      if (ts.isClassDeclaration(statement)) return [this.classDeclaration(statement)];
      if (ts.isInterfaceDeclaration(statement) && statement.name.text === 'InitOutput') {
        const members = statement.members.filter((member) => retainedExports.has(this.memberName(member)));
        return [
          ts.factory.updateInterfaceDeclaration(
            statement,
            statement.modifiers,
            statement.name,
            statement.typeParameters,
            statement.heritageClauses,
            members,
          ),
        ];
      }
      if (
        ts.isModuleDeclaration(statement) &&
        ts.isStringLiteral(statement.name) &&
        statement.name.text === './taffy_wasm'
      ) {
        if (!statement.body || !ts.isModuleBlock(statement.body))
          throw new Error('Unexpected declaration augmentation');
        const members = statement.body.statements.filter(
          (item) => ts.isInterfaceDeclaration(item) && item.name.text === 'Layout',
        );
        if (members.length !== 1) throw new Error('Missing typed Layout.get augmentation');
        return [
          ts.factory.updateModuleDeclaration(
            statement,
            statement.modifiers,
            statement.name,
            ts.factory.updateModuleBlock(statement.body, members),
          ),
        ];
      }
      if (ts.isTypeAliasDeclaration(statement) && statement.name.text === 'StylePropertyArrayValues') return [];
      if (ts.isTypeAliasDeclaration(statement) && statement.name.text === 'MeasureFunction') {
        return [this.measureDeclaration(statement, source)];
      }
      return [statement];
    });
    return this.printer.printFile(ts.factory.updateSourceFile(source, statements));
  }

  measureDeclaration(statement, source) {
    const ts = this.ts;
    if (!ts.isFunctionTypeNode(statement.type) || statement.type.parameters.length !== 5) {
      throw new Error('Unexpected pinned MeasureFunction declaration');
    }
    const style = statement.type.parameters[4];
    if (
      !ts.isIdentifier(style.name) ||
      style.name.text !== 'style' ||
      !ts.isTypeReferenceNode(style.type) ||
      style.type.typeName.getText(source) !== 'Style'
    ) {
      throw new Error('Unexpected owned callback Style declaration');
    }
    return ts.factory.updateTypeAliasDeclaration(
      statement,
      statement.modifiers,
      statement.name,
      statement.typeParameters,
      ts.factory.updateFunctionTypeNode(
        statement.type,
        statement.type.typeParameters,
        statement.type.parameters.slice(0, 4),
        statement.type.type,
      ),
    );
  }

  wasmDeclarations(text) {
    const { ts, retainedExports } = this;
    const source = this.parse('taffy_wasm_bg.wasm.d.ts', text, ts.ScriptKind.TS);
    const statements = source.statements.filter((statement) => {
      if (!ts.isVariableStatement(statement)) throw new Error('Unexpected WASM declaration');
      return statement.declarationList.declarations.every(
        (declaration) => ts.isIdentifier(declaration.name) && retainedExports.has(declaration.name.text),
      );
    });
    if (statements.length !== retainedExports.size) throw new Error('Incomplete retained WASM declarations');
    return this.printer.printFile(ts.factory.updateSourceFile(source, statements));
  }

  prepare(text, wasmExports, wasmImports) {
    const source = this.narrowRuntime(this.parse('taffy_wasm.js', text, this.ts.ScriptKind.JS));
    return this.graph(source, wasmExports, wasmImports);
  }
}

export function createTransforms(ts, surface) {
  return new BindingTransforms(ts, surface);
}
