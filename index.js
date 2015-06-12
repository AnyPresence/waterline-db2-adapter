var async = require('async');
var _ = require('lodash');
var db2 = require('ibm_db');
var WaterlineAdapterErrors = require('waterline-errors').adapter;


/**
 * Waterline uses columnName property -when defined on model- to define dynamic methods to access to instances. Since
 * db2 columns are always uppercase, id field on model needs columnName = ID. In example:
 *
 * id: {
 *       primaryKey: true,
 *       autoIncrement: true,
 *       columnName: 'ID',
 *       type: 'text'
 *     }
 *
 * Interpolated code in controllers expects for some cases the existance of a function findOneById, which wont exist,
 * since columnName for oracle needs to be 'ID', and then, the dynamically generated function will be 'findOneByID' (
 * note the ID in uppercase). To workaround this, during adapter bootstrap this function needs to be executed to copy
 * implementation of function findByOneID to findByOneId.
 *
 * @param collections collections object
 * @returns {*} augmented collections object with collections that now will contain a findOneById function
 * @private
 * @TODO This is the very same fix used in Oracle Adapter. It would be created a common library or something to avoid
 * duplicated code.
 */
function _fixDynamicallyGeneratedFindById(collections) {
    Object.keys(collections).forEach(function (name) {
        if (collections[name]["findOneByID"]) collections[name]["findOneById"] = collections[name]["findOneByID"];
    });

    return collections;
}


/**
 * Sails Boilerplate Adapter
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters, you may need more flexiblity.
 * In any case, it's probably a good idea to start with one file and refactor only if necessary.
 * If you do go that route, it's conventional in Node to create a `./lib` directory for your private submodules
 * and load them at the top of the file with other dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function () {
    var me = this;

    me.connections = {};

    me.dbPools = {};

    me.getConnectionString = function (connection) {
        var connectionData = [
            'DRIVER={DB2}',
            'DATABASE=' + connection.config.database,
            'HOSTNAME=' + connection.config.host,
            'UID=' + connection.config.user,
            'PWD=' + connection.config.password,
            'PORT=' + connection.config.port,
            'PROTOCOL=TCPIP'
        ];

        return connectionData.join(';');
    };

    me.escape = function (word) {
        return "'" + word.replace("'", "''") + "'";
    };

    me.typeMap = {
        // Times
        TIMESTMP: 'time',
        TIME: 'time',
        DATE: 'date',

        // Binaries
        BINARY: 'binary',
        VARBINARY: 'binary',

        // Strings
        CHAR: 'string',
        VARCHAR: 'string',
        GRAPHIC: 'string',
        VARGRAPHIC: 'string',

        // Integers
        SMALLINT: 'integer',
        INTEGER: 'integer',
        BIGINT: 'integer',

        // Floats
        DECIMAL: 'float',
        DECFLOAT: 'float',
        REAL: 'float',
        DOUBLE: 'float',

        // Texts
        CLOB: 'text',
        BLOB: 'text',
        DBCLOB: 'text',
        XML: 'text'
    };

    me.getSqlType = function (attrType) {
        var type = '';

        switch (attrType) {
            case 'string':
                type = 'VARCHAR';
                break;
            case 'integer':
                type = 'INTEGER';
                break;
            case 'float':
                type = 'DOUBLE';
                break;
            case 'text':
                type = 'BLOB';
                break;
            case 'binary':
                type = 'VARBINARY';
                break;
            case 'time':
                type = 'TIMESTMP';
                break;
            case 'date':
                type = 'DATE'
                break;
        }

        return type;
    };

    me.getSelectAttributes = function (collection) {
        return _.keys(collection.definition).join(',');
    };

    var adapter = {
        identity: 'sails-db2',

        syncable: true,

        defaults: {
            host: 'localhost',
            port: 50000,
            schema: true,
            ssl: false,
            migrate: 'alter'
        },


        /**
         * This method runs when a model is initially registered at server-start-time.  This is the only required method.
         *
         * @param  {[type]}   collection [description]
         * @param  {Function} cb         [description]
         * @return {[type]}              [description]
         */
        registerConnection: function (connection, collections, cb) {
            // Validate arguments
            if (!connection.identity) return cb(WaterlineAdapterErrors.IdentityMissing);
            if (me.connections[connection.identity]) return cb(WaterlineAdapterErrors.IdentityDuplicate);

            me.connections[connection.identity] = {
                config: connection,
                collections: collections,
                pool: connection.pool ? new db2.Pool() : null,
                conn: null
            };

            collections = _fixDynamicallyGeneratedFindById(collections);

            return cb();
        },


        /**
         * Fired when a model is unregistered, typically when the server is killed. Useful for tearing-down remaining open
         * connections, etc.
         *
         * @param  {Function} cb [description]
         * @return {[type]}      [description]
         */
        teardown: function (connectionName, cb) {
            var closeConnection = function (connectionName) {
                var connection = me.connections[connectionName];
                if (connection.conn) connection.conn.close();

                delete me.connections[connectionName];
            };

            if (connectionName) closeConnection(connectionName);
            else _.each(me.connections, closeConnection);

            return cb();
        },


        /**
         * REQUIRED method if integrating with a schemaful (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   definition     [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        define: function (connectionName, collectionName, definition, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                query = 'CREATE TABLE ' + collectionName,
                schemaData = [],
                schemaQuery = '';

            _.each(definition, function (attribute, attrName) {
                var attrType = me.getSqlType(attribute.type),
                    attrQuery = attrName;

                // @todo: handle unique and other DB2 data types
                if (attribute.primaryKey) {
                    if (attribute.autoIncrement) attrQuery += ' INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY';
                    else attrQuery += ' VARCHAR(255) NOT NULL PRIMARY KEY';
                }
                else {
                    switch (attrType) {
                        case 'VARCHAR':
                            var len = attribute.length || 255;
                            attrQuery += ' ' + attrType + '(' + len + ')';
                            break;
                        // @todo: handle each type with correct params
                        case 'DOUBLE':
                        case 'BLOB':
                        case 'VARBINARY':
                        case 'TIMESTMP':
                        case 'DATE':
                        case 'INTEGER':
                        default:
                            attrQuery += ' ' + attrType;
                    }
                }

                schemaData.push(attrQuery);
            });
            schemaQuery += '(' + schemaData.join(',') + ')';

            query += ' ' + schemaQuery;
            // @todo: use DB2 Database describe method instead of a SQL Query
            return adapter.query(connectionName, collectionName, query, function (err, result) {
                if (err) {
                    if (err.state !== '42S01') return cb(err);
                    result = [];
                }

                return cb(null, result);
            });
        },

        /**
         * REQUIRED method if integrating with a schemaful (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        describe: function (connectionName, collectionName, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                query = 'SELECT DISTINCT(NAME), COLTYPE, IDENTITY, KEYSEQ, NULLS FROM Sysibm.syscolumns WHERE tbname = ' + me.escape(collectionName);

            adapter.query(connectionName, collectionName, query, function (err, attrs) {
                if (err) return cb(err);
                if (attrs.length === 0) return cb(null, null);

                var attributes = {};
                // Loop through Schema and attach extra attributes
                // @todo: check out a better solution to define primary keys following db2 docs
                attrs.forEach(function (attr) {
                    var attribute = {
                        type: me.typeMap[attr.COLTYPE.trim()]
                    };

                    if (attr.IDENTITY === 'Y' && attr.KEYSEQ !== 0 && attr.NULLS === 'N' && attribute.type === 'integer') {
                        attribute.primaryKey = true;
                        attribute.autoIncrement = true;
                        attribute.unique = true;
                    }

                    attributes[attr.NAME] = attribute;
                });

                cb(null, attributes);
            });
        },


        /**
         * REQUIRED method if integrating with a schemaful (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   relations      [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        drop: function (connectionName, collectionName, relations, cb) {
            if (_.isFunction(relations)) {
                cb = relations;
                relations = [];
            }

            var connection = me.connections[connectionName],
                connectionString = me.getConnectionString(connection),
                __DROP__ = function () {
                    // Drop any relations
                    var dropTable = function (tableName, next) {
                            // Build query
                            var query = 'DROP TABLE ' + tableName;

                            // Run query
                            connection.conn.query(query, next);
                        },
                        passCallback = function (err, result) {
                            if (err) {
                                if (err.state !== '42S02') return cb(err);
                                result = [];
                            }
                            cb(null, result);
                        };

                    async.eachSeries(relations, dropTable, function (err) {
                        if (err) return cb(err);

                        return dropTable(collectionName, passCallback);
                    });

                    connection.conn.query('DROP TABLE ' + collectionName, relations, passCallback);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __DROP__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },


        query: function (connectionName, collectionName, query, data, cb) {
            if (_.isFunction(data)) {
                cb = data;
                data = null;
            }

            var connection = me.connections[connectionName],
                connectionString = me.getConnectionString(connection),
                __QUERY__ = function () {
                    var callback = function (err, records) {
                        if (err) cb(err);
                        else cb(null, records);
                    };

                    if (data) connection.conn.query(query, data, callback);
                    else connection.conn.query(query, callback);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __QUERY__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },


        /**
         * REQUIRED method if users expect to call Model.find(), Model.findOne(), or related.
         *
         * You should implement this method to respond with an array of instances. Waterline core will take care of
         * supporting all the other different find methods/usages.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        find: function (connectionName, collectionName, options, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __FIND__ = function () {
                    var selectQuery = 'SELECT ' + me.getSelectAttributes(collection),
                        fromQuery = ' FROM ' + collection.tableName,
                        whereData = [],
                        whereQuery = '',
                        limitQuery = !_.isEmpty(options.limit) ? ' FETCH FIRST ' + options.limit + ' ROWS ONLY ' : '',
                        sortData = [],
                        sortQuery = '',
                        params = [],
                        sqlQuery = '';

                    // Building where clause
                    _.each(options.where, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            whereData.push(column + ' = ?');
                            params.push(param);
                        }
                    });
                    whereQuery += whereData.join(' AND ');
                    if (whereQuery.length > 0) whereQuery = ' WHERE ' + whereQuery;

                    // Building sort clause
                    _.each(options.sort, function (direction, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            //ORDER BY APPLICATIONCODE DESC

                            sortData.push(column + ' ' + direction);
                        }
                    });
                    sortQuery += sortData.join(', ');
                    if (sortQuery.length > 0) sortQuery = ' ORDER BY ' + sortQuery;

                    sqlQuery += selectQuery + fromQuery + whereQuery + sortQuery + limitQuery;
                    connection.conn.query(sqlQuery, params, cb);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __FIND__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },

        /**
         * REQUIRED method if users expect to call Model.create() or any methods
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        create: function (connectionName, collectionName, values, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __CREATE__ = function () {
                    var selectQuery = me.getSelectAttributes(collection);
                    var columns = [];
                    var params = [];
                    var questions = [];

                    _.each(values, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            columns.push(column);
                            questions.push("'" + param + "'");
                        }
                    });

                    connection.conn.query('SELECT ' + selectQuery + ' FROM FINAL TABLE (INSERT INTO ' + collection.tableName + ' (' + columns.join(',') + ') VALUES (' + questions.join(',') + '))', null, function (err, results) {
                        if (err) cb(err);
                        else cb(null, results[0]);
                    });
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __CREATE__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },

        /**
         * REQUIRED method if users expect to call Model.update()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        update: function (connectionName, collectionName, options, values, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __UPDATE__ = function () {

                    var selectQuery = me.getSelectAttributes(collection);
                    var setData = [];
                    var setQuery = '';
                    var whereData = [];
                    var whereQuery = '';
                    var params = [];
                    var sqlQuery = '';

                    _.each(values, function (param, column) {
                        if (collection.definition.hasOwnProperty(column) && !collection.definition[column].autoIncrement) {
                            setData.push(column + ' = ' + "'" + param + "'");
                        }
                    });
                    setQuery = ' SET ' + setData.join(',');

                    _.each(options.where, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            whereData.push(column + ' = ' + "'" + param + "'");
                        }
                    });
                    whereQuery += whereData.join(' AND ');

                    if (whereQuery.length > 0) whereQuery = ' WHERE ' + whereQuery;

                    sqlQuery = 'SELECT ' + selectQuery + ' FROM FINAL TABLE (UPDATE ' + collection.tableName + setQuery + whereQuery + ')';

                    connection.conn.query(sqlQuery, null, function (err, results) {
                        if (err) cb(err);
                        else cb(null, results[0]);
                    });
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __UPDATE__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        },

        /**
         * REQUIRED method if users expect to call Model.destroy()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        destroy: function (connectionName, collectionName, options, cb) {
            var connection = me.connections[connectionName],
                collection = connection.collections[collectionName],
                connectionString = me.getConnectionString(connection),
                __DESTROY__ = function () {
                    var whereData = [],
                        whereQuery = '',
                        params = [];

                    _.each(options.where, function (param, column) {
                        if (collection.definition.hasOwnProperty(column)) {
                            whereData.push(column + ' = ?');
                            params.push(param);
                        }
                    });
                    whereQuery += whereData.join(' AND ');

                    if (whereQuery.length > 0) whereQuery = ' WHERE ' + whereQuery;

                    connection.conn.query('DELETE FROM ' + collection.tableName + whereQuery, params, cb);
                },
                operationCallback = function (err, conn) {
                    if (err) return cb(err);
                    else {
                        connection.conn = conn;
                        return __DESTROY__();
                    }
                };

            if (connection.pool) return connection.pool.open(connectionString, operationCallback);
            else return db2.open(connectionString, operationCallback);
        }
    };

    return adapter;
})();
