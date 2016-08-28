var mysql = require('mysql');
//var conf = require('../conf.js');
var Logger = require('./Logger');
var colors = require('colors');

/**
 * @Connection :
 * -------------
 * Creates a new connection object using config variables set in .env
 */

var Connection = function() {
    this.connection = mysql.createConnection({
        host:     process.env.SQL_HOST,
        user:     process.env.SQL_USER,
        password: process.env.SQL_PASSWORD,
        database: process.env.SQL_DATABASE,
        port:     process.env.SQL_PORT
    });
};

/**
 * @end :
 * ------
 * Safely dispose of the connection using connections .end method which gracefully
 * disposes of the connection.
 */

Connection.prototype.end = function() {
    this.connection.end(function (err) {
        if(err) {
            Logger.writeToConsole('[SQL] Failed to kill remaining connections in the queue'.red);
        }
        // all connections in the pool have ended
    });
};

/**
 * @selectUpdate :
 * ---------------
 * Starts a new transaction and selects a row for update.  This causes a lock in the database
 * so if another process selects it a deadlock could occur, we handle this by using a memcache
 * which synchronizes all r/w transactions to the DB
 *
 * @param {string} selectQuery - The select query to run
 * @param {string} updateQuery - The update query to run NOTE: There is a hardcoded append in this file, its a WHERE
 * clause that is set as the result of the SELECT query to append the replayId to UPDATE
 * @param {function} callback - Callback to notify the caller that the transaction has completed, this then notifies
 * them to release the lock on memcached
 */

Connection.prototype.selectUpdate = function(selectQuery, updateQuery, callback) {
    selectQuery = "select * from queue where replayId='c5e1a64527fd4778b5e1c17b1253540f' FOR UPDATE";
    this.connection.connect(function (err) {
        if(err) {
            Logger.writeToConsole("[MYSQL] Error: Connection NOT made".red + err);
            callback(null);
        } else {
            this.connection.beginTransaction(function(err) {
                if(err) {
                    Logger.writeToConsole("[MYSQL] Error: Transaction failed to begin".red + err);
                    callback(null);
                } else {
                    this.connection.query(selectQuery, function(err, result) {
                        if(err) {
                            //Logger.writeToConsole('got a deadlock at select!'.red);
                            this.connection.rollback(function() {
                                Logger.writeToConsole("[MYSQL] Error: Rolled back transaction at SELECT! ".red + err);
                            });
                            this.end();
                            callback(null);
                        } else {
                            if(typeof result !== 'undefined' && result && result.length > 0) {
                                var replay = result[0];
                                updateQuery += ' WHERE replayId= "' + replay.replayId +'"';
                                this.connection.query(updateQuery, function(err, result) {
                                    if(err) {
                                        //Logger.writeToConsole('got a deadlock at update!'.red);
                                        this.connection.rollback(function() {
                                            Logger.writeToConsole("[MYSQL] Error: Rolled back transaction at UPDATE! ".red + err);
                                        });
                                        this.end();
                                        callback(null);
                                    } else {
                                        this.connection.commit(function(err) {
                                            if(err) {
                                                //Logger.writeToConsole('got a deadlock at commit!'.red);
                                                this.connection.rollback(function() {
                                                    Logger.writeToConsole("[MYSQL] Error: Rolled back transaction at COMMIT! ".red + err);
                                                });
                                                this.end();
                                                callback(null);
                                            } else {
                                                this.end();
                                                if(typeof result !== 'undefined' && result && result.changedRows > 0) {
                                                    callback(replay);
                                                } else {
                                                    Logger.writeToConsole('[QUEUE] Replay:'.red + replay.replayId + ' is already reserved!'.red);
                                                    callback(null);
                                                }
                                            }
                                        }.bind(this))
                                    }
                                }.bind(this));
                            } else{
                                this.end();
                                callback(null);
                            }
                        }
                    }.bind(this));
                }
            }.bind(this));
        }
    }.bind(this));
};

/**
 * @query :
 * --------
 * Queries the database with a given query string and calls back to the caller when the
 * query completes, if there is an error the worker dies and the GC disposes of it.
 *
 * @param {string} queryString - The query to be run
 * @param {function} callback - Callback to notify the caller that the query has completed
 */

Connection.prototype.query = function(queryString, callback) {
    this.connection.connect(function (err) {
        if(err) {
            Logger.writeToConsole("[MYSQL] Error: Connection NOT made".red);
        } else {
            this.connection.query(queryString, function(err, rows) {
                if(err) {
                    Logger.writeToConsole("[MYSQL] Error: Query: ".red + queryString + ", was not successful".red);
                }
                if(typeof rows !== "undefined" && rows  && rows.affectedRows > 1) {
                    Logger.writeToConsole('[MYSQL] Saved: '.green + rows.affectedRows + ' rows'.green);
                }
                callback(rows);
                this.end();
            }.bind(this));
        }
    }.bind(this));
};

module.exports = Connection;