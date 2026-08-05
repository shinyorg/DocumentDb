// Checks that the tab's scripts will actually run inside Docker Desktop.
//
//   node src/ShinyDocDbMyAdmin.Extension/check.js
//
// Node and no packages on purpose - this has to be runnable without adding a JS toolchain to a .NET
// repo, and the failure it exists to catch needs neither a browser nor Docker.
//
// The failure: the extension page already has a global `ddClient`, declared by the host as a `var`,
// so it is both a property of `window` and a binding that a later top-level `const ddClient`
// collides with. Scripts on a page share one global lexical environment, so the redeclaration throws
// "Identifier 'ddClient' has already been declared" at instantiation - before a single statement
// runs. The page still renders, fully styled, with every control inert and no error visible anywhere
// in the UI. It cost an afternoon once.
//
// Note for anyone tempted to do this with `vm.runInContext`: don't. Each call gets a fresh lexical
// scope, so a `const` in one script never collides with a `var` from another and the check silently
// passes whatever you give it. A page does not work that way. Compiling the host's prelude and both
// files as a single script does model it - the collision is a compile-time error there, which is why
// this only needs to compile and never has to run or stub a DOM.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ui = path.join(__dirname, 'ui');
const files = ['discovery.js', 'app.js'];
const failures = [];

// What Docker Desktop puts on the page before anything of ours loads. `var`, at global scope, because
// that is what makes it collide - an assignment to `window.ddClient` would not, and would let exactly
// the bug this exists for straight through.
const HOST_PRELUDE = 'var ddClient = {};';

const sources = files.map(file => ({ file, source: fs.readFileSync(path.join(ui, file), 'utf8') }));

// 1. Everything the page loads, compiled as the page compiles it.
try {
    new vm.Script([HOST_PRELUDE, ...sources.map(s => s.source)].join('\n;\n'), { filename: 'extension-page' });
    console.log('  scripts compile alongside the host\'s globals');
}
catch (error) {
    failures.push(`compiled together with the host's globals: ${error.message}`);
}

// 2. Nothing at top level, so the next name the host adds cannot collide with ours either. Structural
//    rather than clever: the whole of each file has to sit inside an IIFE.
for (const { file, source } of sources) {
    const code = source
        .split('\n')
        .filter(line => !line.trim().startsWith('//') && line.trim().length > 0)
        .join('\n')
        .trim();

    if (!code.startsWith('(function'))
        failures.push(`${file}: code starts outside an IIFE - top-level declarations can collide with the host's globals`);

    if (!/\}\)\([^)]*\);?$/.test(code))
        failures.push(`${file}: does not end by closing an IIFE`);
}

if (failures.length > 0) {
    console.error('\nFAILED');
    for (const failure of failures)
        console.error(`  - ${failure}`);

    process.exit(1);
}

console.log('OK - the tab\'s scripts load under the host\'s globals and declare none of their own.');
