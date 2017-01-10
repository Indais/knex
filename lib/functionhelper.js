'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

// FunctionHelper
// -------
function FunctionHelper(client) {
  this.client = client;
}

FunctionHelper.prototype.now = function () {
  return this.client.raw('CURRENT_TIMESTAMP');
};

exports.default = FunctionHelper;
module.exports = exports['default'];