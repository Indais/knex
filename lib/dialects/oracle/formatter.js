'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _getPrototypeOf = require('babel-runtime/core-js/object/get-prototype-of');

var _getPrototypeOf2 = _interopRequireDefault(_getPrototypeOf);

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

var _possibleConstructorReturn2 = require('babel-runtime/helpers/possibleConstructorReturn');

var _possibleConstructorReturn3 = _interopRequireDefault(_possibleConstructorReturn2);

var _get2 = require('babel-runtime/helpers/get');

var _get3 = _interopRequireDefault(_get2);

var _inherits2 = require('babel-runtime/helpers/inherits');

var _inherits3 = _interopRequireDefault(_inherits2);

var _formatter = require('../../formatter');

var _formatter2 = _interopRequireDefault(_formatter);

var _utils = require('./utils');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var Oracle_Formatter = function (_Formatter) {
  (0, _inherits3.default)(Oracle_Formatter, _Formatter);

  function Oracle_Formatter() {
    (0, _classCallCheck3.default)(this, Oracle_Formatter);
    return (0, _possibleConstructorReturn3.default)(this, (Oracle_Formatter.__proto__ || (0, _getPrototypeOf2.default)(Oracle_Formatter)).apply(this, arguments));
  }

  (0, _createClass3.default)(Oracle_Formatter, [{
    key: 'alias',
    value: function alias(first, second) {
      return first + ' ' + second;
    }
  }, {
    key: 'parameter',
    value: function parameter(value, notSetValue) {
      // Returning helper uses always ROWID as string
      if (value instanceof _utils.ReturningHelper && this.client.driver) {
        value = new this.client.driver.OutParam(this.client.driver.OCCISTRING);
      } else if (typeof value === 'boolean') {
        value = value ? 1 : 0;
      }
      return (0, _get3.default)(Oracle_Formatter.prototype.__proto__ || (0, _getPrototypeOf2.default)(Oracle_Formatter.prototype), 'parameter', this).call(this, value, notSetValue);
    }
  }]);
  return Oracle_Formatter;
}(_formatter2.default);

exports.default = Oracle_Formatter;
module.exports = exports['default'];