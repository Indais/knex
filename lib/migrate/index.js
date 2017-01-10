'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

var _freeze = require('babel-runtime/core-js/object/freeze');

var _freeze2 = _interopRequireDefault(_freeze);

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _path = require('path');

var _path2 = _interopRequireDefault(_path);

var _mkdirp = require('mkdirp');

var _mkdirp2 = _interopRequireDefault(_mkdirp);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

var _helpers = require('../helpers');

var helpers = _interopRequireWildcard(_helpers);

var _lodash = require('lodash');

var _inherits = require('inherits');

var _inherits2 = _interopRequireDefault(_inherits);

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function LockError(msg) {
  this.name = 'MigrationLocked';
  this.message = msg;
} // Migrator
// -------

(0, _inherits2.default)(LockError, Error);

var SUPPORTED_EXTENSIONS = (0, _freeze2.default)(['.co', '.coffee', '.eg', '.iced', '.js', '.litcoffee', '.ls', '.ts']);

var CONFIG_DEFAULT = (0, _freeze2.default)({
  extension: 'js',
  tableName: 'knex_migrations',
  directory: './migrations',
  disableTransactions: false
});

// The new migration we're performing, typically called from the `knex.migrate`
// interface on the main `knex` object. Passes the `knex` instance performing
// the migration.

var Migrator = function () {
  function Migrator(knex) {
    (0, _classCallCheck3.default)(this, Migrator);

    this.knex = knex;
    this.config = this.setConfig(knex.client.config.migrations);
  }

  // Migrators to the latest configuration.


  (0, _createClass3.default)(Migrator, [{
    key: 'latest',
    value: function latest(config) {
      var _this = this;

      this.config = this.setConfig(config);
      return this._migrationData().tap(validateMigrationList).spread(function (all, completed) {
        return _this._runBatch((0, _lodash.difference)(all, completed), 'up');
      });
    }

    // Rollback the last "batch" of migrations that were run.

  }, {
    key: 'rollback',
    value: function rollback(config) {
      var _this2 = this;

      return _bluebird2.default.try(function () {
        _this2.config = _this2.setConfig(config);
        return _this2._migrationData().tap(validateMigrationList).then(function (val) {
          return _this2._getLastBatch(val);
        }).then(function (migrations) {
          return _this2._runBatch((0, _lodash.map)(migrations, 'name'), 'down');
        });
      });
    }
  }, {
    key: 'status',
    value: function status(config) {
      this.config = this.setConfig(config);

      return _bluebird2.default.all([this.knex(this.config.tableName).select('*'), this._listAll()]).spread(function (db, code) {
        return db.length - code.length;
      });
    }

    // Retrieves and returns the current migration version we're on, as a promise.
    // If no migrations have been run yet, return "none".

  }, {
    key: 'currentVersion',
    value: function currentVersion(config) {
      this.config = this.setConfig(config);
      return this._listCompleted(config).then(function (completed) {
        var val = (0, _lodash.max)((0, _lodash.map)(completed, function (value) {
          return value.split('_')[0];
        }));
        return (0, _lodash.isUndefined)(val) ? 'none' : val;
      });
    }
  }, {
    key: 'forceFreeMigrationsLock',
    value: function forceFreeMigrationsLock(config) {
      var _this3 = this;

      this.config = this.setConfig(config);
      var lockTable = this._getLockTableName();
      return this.knex.schema.hasTable(lockTable).then(function (exist) {
        return exist && _this3._freeLock();
      });
    }

    // Creates a new migration, with a given name.

  }, {
    key: 'make',
    value: function make(name, config) {
      var _this4 = this;

      this.config = this.setConfig(config);
      if (!name) {
        return _bluebird2.default.reject(new Error('A name must be specified for the generated migration'));
      }

      return this._ensureFolder(config).then(function (val) {
        return _this4._generateStubTemplate(val);
      }).then(function (val) {
        return _this4._writeNewMigration(name, val);
      });
    }

    // Lists all available migration versions, as a sorted array.

  }, {
    key: '_listAll',
    value: function _listAll(config) {
      this.config = this.setConfig(config);
      return _bluebird2.default.promisify(_fs2.default.readdir, { context: _fs2.default })(this._absoluteConfigDir()).then(function (migrations) {
        return (0, _lodash.filter)(migrations, function (value) {
          var extension = _path2.default.extname(value);
          return (0, _lodash.includes)(SUPPORTED_EXTENSIONS, extension);
        }).sort();
      });
    }

    // Ensures a folder for the migrations exist, dependent on the migration
    // config settings.

  }, {
    key: '_ensureFolder',
    value: function _ensureFolder() {
      var dir = this._absoluteConfigDir();
      return _bluebird2.default.promisify(_fs2.default.stat, { context: _fs2.default })(dir).catch(function () {
        return _bluebird2.default.promisify(_mkdirp2.default)(dir);
      });
    }

    // Ensures that a proper table has been created, dependent on the migration
    // config settings.

  }, {
    key: '_ensureTable',
    value: function _ensureTable() {
      var _this5 = this;

      var table = this.config.tableName;
      var lockTable = this._getLockTableName();
      return this.knex.schema.hasTable(table).then(function (exists) {
        return !exists && _this5._createMigrationTable(table);
      }).then(function () {
        return _this5.knex.schema.hasTable(lockTable);
      }).then(function (exists) {
        return !exists && _this5._createMigrationLockTable(lockTable);
      }).then(function () {
        return _this5.knex(lockTable).select('*');
      }).then(function (data) {
        return !data.length && _this5.knex(lockTable).insert({ is_locked: 0 });
      });
    }

    // Create the migration table, if it doesn't already exist.

  }, {
    key: '_createMigrationTable',
    value: function _createMigrationTable(tableName) {
      return this.knex.schema.createTableIfNotExists(tableName, function (t) {
        t.increments();
        t.string('name');
        t.integer('batch');
        t.timestamp('migration_time');
      });
    }
  }, {
    key: '_createMigrationLockTable',
    value: function _createMigrationLockTable(tableName) {
      return this.knex.schema.createTableIfNotExists(tableName, function (t) {
        t.integer('is_locked');
      });
    }
  }, {
    key: '_getLockTableName',
    value: function _getLockTableName() {
      return this.config.tableName + '_lock';
    }
  }, {
    key: '_isLocked',
    value: function _isLocked(trx) {
      var tableName = this._getLockTableName();
      return this.knex(tableName).transacting(trx).forUpdate().select('*').then(function (data) {
        return data[0].is_locked;
      });
    }
  }, {
    key: '_lockMigrations',
    value: function _lockMigrations(trx) {
      var tableName = this._getLockTableName();
      return this.knex(tableName).transacting(trx).update({ is_locked: 1 });
    }
  }, {
    key: '_getLock',
    value: function _getLock() {
      var _this6 = this;

      return this.knex.transaction(function (trx) {
        return _this6._isLocked(trx).then(function (isLocked) {
          if (isLocked) {
            throw new Error("Migration table is already locked");
          }
        }).then(function () {
          return _this6._lockMigrations(trx);
        });
      }).catch(function (err) {
        throw new LockError(err.message);
      });
    }
  }, {
    key: '_freeLock',
    value: function _freeLock() {
      var tableName = this._getLockTableName();
      return this.knex(tableName).update({ is_locked: 0 });
    }

    // Run a batch of current migrations, in sequence.

  }, {
    key: '_runBatch',
    value: function _runBatch(migrations, direction) {
      var _this7 = this;

      return this._getLock().then(function () {
        return _bluebird2.default.all((0, _lodash.map)(migrations, (0, _lodash.bind)(_this7._validateMigrationStructure, _this7)));
      }).then(function () {
        return _this7._latestBatchNumber();
      }).then(function (batchNo) {
        if (direction === 'up') batchNo++;
        return batchNo;
      }).then(function (batchNo) {
        return _this7._waterfallBatch(batchNo, migrations, direction);
      }).tap(function () {
        return _this7._freeLock();
      }).catch(function (error) {
        var cleanupReady = _bluebird2.default.resolve();

        if (error instanceof LockError) {
          // If locking error do not free the lock.
          helpers.warn('Can\'t take lock to run migrations: ' + error.message);
          helpers.warn('If you are sure migrations are not running you can release the ' + 'lock manually by deleting all the rows from migrations lock ' + 'table: ' + _this7._getLockTableName());
        } else {
          helpers.warn('migrations failed with error: ' + error.message);
          // If the error was not due to a locking issue, then remove the lock.
          cleanupReady = _this7._freeLock();
        }

        return cleanupReady.finally(function () {
          throw error;
        });
      });
    }

    // Validates some migrations by requiring and checking for an `up` and `down`
    // function.

  }, {
    key: '_validateMigrationStructure',
    value: function _validateMigrationStructure(name) {
      var migration = require(_path2.default.join(this._absoluteConfigDir(), name));
      if (typeof migration.up !== 'function' || typeof migration.down !== 'function') {
        throw new Error('Invalid migration: ' + name + ' must have both an up and down function');
      }
      return name;
    }

    // Lists all migrations that have been completed for the current db, as an
    // array.

  }, {
    key: '_listCompleted',
    value: function _listCompleted() {
      var _this8 = this;

      var tableName = this.config.tableName;

      return this._ensureTable(tableName).then(function () {
        return _this8.knex(tableName).orderBy('id').select('name');
      }).then(function (migrations) {
        return (0, _lodash.map)(migrations, 'name');
      });
    }

    // Gets the migration list from the specified migration directory, as well as
    // the list of completed migrations to check what should be run.

  }, {
    key: '_migrationData',
    value: function _migrationData() {
      return _bluebird2.default.all([this._listAll(), this._listCompleted()]);
    }

    // Generates the stub template for the current migration, returning a compiled
    // template.

  }, {
    key: '_generateStubTemplate',
    value: function _generateStubTemplate() {
      var stubPath = this.config.stub || _path2.default.join(__dirname, 'stub', this.config.extension + '.stub');
      return _bluebird2.default.promisify(_fs2.default.readFile, { context: _fs2.default })(stubPath).then(function (stub) {
        return (0, _lodash.template)(stub.toString(), { variable: 'd' });
      });
    }

    // Write a new migration to disk, using the config and generated filename,
    // passing any `variables` given in the config to the template.

  }, {
    key: '_writeNewMigration',
    value: function _writeNewMigration(name, tmpl) {
      var config = this.config;

      var dir = this._absoluteConfigDir();
      if (name[0] === '-') name = name.slice(1);
      var filename = yyyymmddhhmmss() + '_' + name + '.' + config.extension;
      return _bluebird2.default.promisify(_fs2.default.writeFile, { context: _fs2.default })(_path2.default.join(dir, filename), tmpl(config.variables || {})).return(_path2.default.join(dir, filename));
    }

    // Get the last batch of migrations, by name, ordered by insert id in reverse
    // order.

  }, {
    key: '_getLastBatch',
    value: function _getLastBatch() {
      var tableName = this.config.tableName;

      return this.knex(tableName).where('batch', function (qb) {
        qb.max('batch').from(tableName);
      }).orderBy('id', 'desc');
    }

    // Returns the latest batch number.

  }, {
    key: '_latestBatchNumber',
    value: function _latestBatchNumber() {
      return this.knex(this.config.tableName).max('batch as max_batch').then(function (obj) {
        return obj[0].max_batch || 0;
      });
    }

    // If transaction config for a single migration is defined, use that.
    // Otherwise, rely on the common config. This allows enabling/disabling
    // transaction for a single migration at will, regardless of the common
    // config.

  }, {
    key: '_useTransaction',
    value: function _useTransaction(migration, allTransactionsDisabled) {
      var singleTransactionValue = (0, _lodash.get)(migration, 'config.transaction');

      return (0, _lodash.isBoolean)(singleTransactionValue) ? singleTransactionValue : !allTransactionsDisabled;
    }

    // Runs a batch of `migrations` in a specified `direction`, saving the
    // appropriate database information as the migrations are run.

  }, {
    key: '_waterfallBatch',
    value: function _waterfallBatch(batchNo, migrations, direction) {
      var _this9 = this;

      var knex = this.knex;
      var _config = this.config,
          tableName = _config.tableName,
          disableTransactions = _config.disableTransactions;

      var directory = this._absoluteConfigDir();
      var current = _bluebird2.default.bind({ failed: false, failedOn: 0 });
      var log = [];
      (0, _lodash.each)(migrations, function (migration) {
        var name = migration;
        migration = require(directory + '/' + name);

        // We're going to run each of the migrations in the current "up".
        current = current.then(function () {
          if (_this9._useTransaction(migration, disableTransactions)) {
            return _this9._transaction(migration, direction, name);
          }
          return warnPromise(migration[direction](knex, _bluebird2.default), name);
        }).then(function () {
          log.push(_path2.default.join(directory, name));
          if (direction === 'up') {
            return knex(tableName).insert({
              name: name,
              batch: batchNo,
              migration_time: new Date()
            });
          }
          if (direction === 'down') {
            return knex(tableName).where({ name: name }).del();
          }
        });
      });

      return current.thenReturn([batchNo, log]);
    }
  }, {
    key: '_transaction',
    value: function _transaction(migration, direction, name) {
      return this.knex.transaction(function (trx) {
        return warnPromise(migration[direction](trx, _bluebird2.default), name, function () {
          trx.commit();
        });
      });
    }
  }, {
    key: '_absoluteConfigDir',
    value: function _absoluteConfigDir() {
      return _path2.default.resolve(process.cwd(), this.config.directory);
    }
  }, {
    key: 'setConfig',
    value: function setConfig(config) {
      return (0, _lodash.assign)({}, CONFIG_DEFAULT, this.config || {}, config);
    }
  }]);
  return Migrator;
}();

// Validates that migrations are present in the appropriate directories.


exports.default = Migrator;
function validateMigrationList(migrations) {
  var all = migrations[0];
  var completed = migrations[1];
  var diff = (0, _lodash.difference)(completed, all);
  if (!(0, _lodash.isEmpty)(diff)) {
    throw new Error('The migration directory is corrupt, the following files are missing: ' + diff.join(', '));
  }
}

function warnPromise(value, name, fn) {
  if (!value || typeof value.then !== 'function') {
    helpers.warn('migration ' + name + ' did not return a promise');
    if (fn && typeof fn === 'function') fn();
  }
  return value;
}

// Ensure that we have 2 places for each of the date segments.
function padDate(segment) {
  segment = segment.toString();
  return segment[1] ? segment : '0' + segment;
}

// Get a date object in the correct format, without requiring a full out library
// like "moment.js".
function yyyymmddhhmmss() {
  var d = new Date();
  return d.getFullYear().toString() + padDate(d.getMonth() + 1) + padDate(d.getDate()) + padDate(d.getHours()) + padDate(d.getMinutes()) + padDate(d.getSeconds());
}
module.exports = exports['default'];