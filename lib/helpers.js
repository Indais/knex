'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.skim = skim;
exports.normalizeArr = normalizeArr;
exports.debugLog = debugLog;
exports.error = error;
exports.deprecate = deprecate;
exports.warn = warn;
exports.exit = exit;
exports.containsUndefined = containsUndefined;

var _lodash = require('lodash');

var _chalk = require('chalk');

var _chalk2 = _interopRequireDefault(_chalk);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

// Pick off the attributes from only the current layer of the object.
/* eslint no-console:0 */

function skim(data) {
  return (0, _lodash.map)(data, function (obj) {
    return (0, _lodash.pick)(obj, (0, _lodash.keys)(obj));
  });
}

// Check if the first argument is an array, otherwise uses all arguments as an
// array.
function normalizeArr() {
  var args = new Array(arguments.length);
  for (var i = 0; i < args.length; i++) {
    args[i] = arguments[i];
  }
  if (Array.isArray(args[0])) {
    return args[0];
  }
  return args;
}

function debugLog(msg) {
  console.log(msg);
}

function error(msg) {
  console.log(_chalk2.default.red('Knex:Error ' + msg));
}

// Used to signify deprecated functionality.
function deprecate(method, alternate) {
  warn(method + ' is deprecated, please use ' + alternate);
}

// Used to warn about incorrect use, without error'ing
function warn(msg) {
  console.log(_chalk2.default.yellow('Knex:warning - ' + msg));
}

function exit(msg) {
  console.log(_chalk2.default.red(msg));
  process.exit(1);
}

function containsUndefined(mixed) {
  var argContainsUndefined = false;

  if ((0, _lodash.isTypedArray)(mixed)) return false;

  if (mixed && (0, _lodash.isFunction)(mixed.toSQL)) {
    //Any QueryBuilder or Raw will automatically be validated during compile.
    return argContainsUndefined;
  }

  if ((0, _lodash.isArray)(mixed)) {
    for (var i = 0; i < mixed.length; i++) {
      if (argContainsUndefined) break;
      argContainsUndefined = this.containsUndefined(mixed[i]);
    }
  } else if ((0, _lodash.isObject)(mixed)) {
    for (var key in mixed) {
      if (argContainsUndefined) break;
      argContainsUndefined = this.containsUndefined(mixed[key]);
    }
  } else {
    argContainsUndefined = (0, _lodash.isUndefined)(mixed);
  }

  return argContainsUndefined;
}