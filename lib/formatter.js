'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

var _builder = require('./query/builder');

var _builder2 = _interopRequireDefault(_builder);

var _raw = require('./raw');

var _raw2 = _interopRequireDefault(_raw);

var _lodash = require('lodash');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

// Valid values for the `order by` clause generation.
var orderBys = ['asc', 'desc'];

// Turn this into a lookup map
var operators = (0, _lodash.transform)(['=', '<', '>', '<=', '>=', '<>', '!=', 'like', 'not like', 'between', 'ilike', '&', '|', '^', '<<', '>>', 'rlike', 'regexp', 'not regexp', '~', '~*', '!~', '!~*', '#', '&&', '@>', '<@', '||'], function (result, key) {
  result[key] = true;
}, {});

var Formatter = function () {
  function Formatter(client) {
    (0, _classCallCheck3.default)(this, Formatter);

    this.client = client;
    this.bindings = [];
  }

  // Accepts a string or array of columns to wrap as appropriate.


  (0, _createClass3.default)(Formatter, [{
    key: 'columnize',
    value: function columnize(target) {
      var columns = typeof target === 'string' ? [target] : target;
      var str = '',
          i = -1;
      while (++i < columns.length) {
        if (i > 0) str += ', ';
        str += this.wrap(columns[i]);
      }
      return str;
    }

    // Turns a list of values into a list of ?'s, joining them with commas unless
    // a "joining" value is specified (e.g. ' and ')

  }, {
    key: 'parameterize',
    value: function parameterize(values, notSetValue) {
      if (typeof values === 'function') return this.parameter(values);
      values = Array.isArray(values) ? values : [values];
      var str = '',
          i = -1;
      while (++i < values.length) {
        if (i > 0) str += ', ';
        str += this.parameter(values[i] === undefined ? notSetValue : values[i]);
      }
      return str;
    }

    // Checks whether a value is a function... if it is, we compile it
    // otherwise we check whether it's a raw

  }, {
    key: 'parameter',
    value: function parameter(value) {
      if (typeof value === 'function') {
        return this.outputQuery(this.compileCallback(value), true);
      }
      return this.unwrapRaw(value, true) || '?';
    }
  }, {
    key: 'unwrapRaw',
    value: function unwrapRaw(value, isParameter) {
      var query = void 0;
      if (value instanceof _builder2.default) {
        query = this.client.queryCompiler(value).toSQL();
        if (query.bindings) {
          this.bindings = this.bindings.concat(query.bindings);
        }
        return this.outputQuery(query, isParameter);
      }
      if (value instanceof _raw2.default) {
        value.client = this.client;
        query = value.toSQL();
        if (query.bindings) {
          this.bindings = this.bindings.concat(query.bindings);
        }
        return query.sql;
      }
      if (isParameter) {
        this.bindings.push(value);
      }
    }
  }, {
    key: 'rawOrFn',
    value: function rawOrFn(value, method) {
      if (typeof value === 'function') {
        return this.outputQuery(this.compileCallback(value, method));
      }
      return this.unwrapRaw(value) || '';
    }

    // Puts the appropriate wrapper around a value depending on the database
    // engine, unless it's a knex.raw value, in which case it's left alone.

  }, {
    key: 'wrap',
    value: function wrap(value) {
      if (typeof value === 'function') {
        return this.outputQuery(this.compileCallback(value), true);
      }
      var raw = this.unwrapRaw(value);
      if (raw) return raw;
      if (typeof value === 'number') return value;
      return this._wrapString(value + '');
    }
  }, {
    key: 'wrapAsIdentifier',
    value: function wrapAsIdentifier(value) {
      return this.client.wrapIdentifier((value || '').trim());
    }
  }, {
    key: 'alias',
    value: function alias(first, second) {
      return first + ' as ' + second;
    }

    // The operator method takes a value and returns something or other.

  }, {
    key: 'operator',
    value: function operator(value) {
      var raw = this.unwrapRaw(value);
      if (raw) return raw;
      if (operators[(value || '').toLowerCase()] !== true) {
        throw new TypeError('The operator "' + value + '" is not permitted');
      }
      return value;
    }

    // Specify the direction of the ordering.

  }, {
    key: 'direction',
    value: function direction(value) {
      var raw = this.unwrapRaw(value);
      if (raw) return raw;
      return orderBys.indexOf((value || '').toLowerCase()) !== -1 ? value : 'asc';
    }

    // Compiles a callback using the query builder.

  }, {
    key: 'compileCallback',
    value: function compileCallback(callback, method) {
      var client = this.client;

      // Build the callback

      var builder = client.queryBuilder();
      callback.call(builder, builder);

      // Compile the callback, using the current formatter (to track all bindings).
      var compiler = client.queryCompiler(builder);
      compiler.formatter = this;

      // Return the compiled & parameterized sql.
      return compiler.toSQL(method || 'select');
    }

    // Ensures the query is aliased if necessary.

  }, {
    key: 'outputQuery',
    value: function outputQuery(compiled, isParameter) {
      var sql = compiled.sql || '';
      if (sql) {
        if ((compiled.method === 'select' || compiled.method === 'first') && (isParameter || compiled.as)) {
          sql = '(' + sql + ')';
          if (compiled.as) return this.alias(sql, this.wrap(compiled.as));
        }
      }
      return sql;
    }

    // Coerce to string to prevent strange errors when it's not a string.

  }, {
    key: '_wrapString',
    value: function _wrapString(value) {
      var asIndex = value.toLowerCase().indexOf(' as ');
      if (asIndex !== -1) {
        var first = value.slice(0, asIndex);
        var second = value.slice(asIndex + 4);
        return this.alias(this.wrap(first), this.wrapAsIdentifier(second));
      }
      var wrapped = [];
      var i = -1;
      var segments = value.split('.');
      while (++i < segments.length) {
        value = segments[i];
        if (i === 0 && segments.length > 1) {
          wrapped.push(this.wrap((value || '').trim()));
        } else {
          wrapped.push(this.client.wrapIdentifier((value || '').trim()));
        }
      }
      return wrapped.join('.');
    }
  }]);
  return Formatter;
}();

exports.default = Formatter;
module.exports = exports['default'];