require = require("esm")(module, { debug: true });
console.time('mostly-feathers-cache import');
module.exports = require('./src/index').default;
console.timeEnd('mostly-feathers-cache import');
