'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

exports.default = function (Target) {

  Target.prototype.toQuery = function (tz) {
    var _this = this;

    var data = this.toSQL(this._method, tz);
    if (!(0, _lodash.isArray)(data)) data = [data];
    return (0, _lodash.map)(data, function (statement) {
      return _this.client._formatQuery(statement.sql, statement.bindings, tz);
    }).join(';\n');
  };

  // Create a new instance of the `Runner`, passing in the current object.
  Target.prototype.then = function () /* onFulfilled, onRejected */{
    var result = this.client.runner(this).run();
    return result.then.apply(result, arguments);
  };

  // Add additional "options" to the builder. Typically used for client specific
  // items, like the `mysql` and `sqlite3` drivers.
  Target.prototype.options = function (opts) {
    this._options = this._options || [];
    this._options.push((0, _lodash.clone)(opts) || {});
    return this;
  };

  // Sets an explicit "connnection" we wish to use for this query.
  Target.prototype.connection = function (connection) {
    this._connection = connection;
    return this;
  };

  // Set a debug flag for the current schema query stack.
  Target.prototype.debug = function (enabled) {
    this._debug = arguments.length ? enabled : true;
    return this;
  };

  // Set the transaction object for this query.
  Target.prototype.transacting = function (t) {
    if (t && t.client) {
      if (!t.client.transacting) {
        helpers.warn('Invalid transaction value: ' + t.client);
      } else {
        this.client = t.client;
      }
    }
    return this;
  };

  // Initializes a stream.
  Target.prototype.stream = function (options) {
    return this.client.runner(this).stream(options);
  };

  // Initialize a stream & pipe automatically.
  Target.prototype.pipe = function (writable, options) {
    return this.client.runner(this).pipe(writable, options);
  };

  // Creates a method which "coerces" to a promise, by calling a
  // "then" method on the current `Target`
  (0, _lodash.each)(['bind', 'catch', 'finally', 'asCallback', 'spread', 'map', 'reduce', 'tap', 'thenReturn', 'return', 'yield', 'ensure', 'reflect'], function (method) {
    Target.prototype[method] = function () {
      var then = this.then();
      then = then[method].apply(then, arguments);
      return then;
    };
  });
};

var _helpers = require('./helpers');

var helpers = _interopRequireWildcard(_helpers);

var _lodash = require('lodash');

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }

module.exports = exports['default'];