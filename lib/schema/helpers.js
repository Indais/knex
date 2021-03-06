'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.pushQuery = pushQuery;
exports.pushAdditional = pushAdditional;

var _lodash = require('lodash');

// Push a new query onto the compiled "sequence" stack,
// creating a new formatter, returning the compiler.
function pushQuery(query) {
  if (!query) return;
  if ((0, _lodash.isString)(query)) {
    query = { sql: query };
  }
  if (!query.bindings) {
    query.bindings = this.formatter.bindings;
  }
  this.sequence.push(query);
  this.formatter = this.client.formatter();
}

// Used in cases where we need to push some additional column specific statements.
function pushAdditional(fn) {
  var child = new this.constructor(this.client, this.tableCompiler, this.columnBuilder);
  fn.call(child, (0, _lodash.tail)(arguments));
  this.sequence.additional = (this.sequence.additional || []).concat(child.sequence);
}