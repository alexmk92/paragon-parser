var mysql = require('mysql');
var conf = require('../conf.js');
var Logger = require('./Logger');
var colors = require('colors');

var Connection = function(connectionLimit) {
    this.pool = mysql.createPool({
        host:     conf.HOST,
        user:     conf.USER,
        password: conf.PASSWORD,
        database: conf.DATABASE,
        port:     3306,
        connectionLimit: connectionLimit
    });
};

Connection.prototype.end = function() {
    this.pool.end(function (err) {
        if(err) {
            console.log('[SQL] Failed to kill remaining connections in the queue'.red);
        } else {
            console.log('[SQL] Killed all connections in the pool.'.green);
        }
        // all connections in the pool have ended
    });
};

Connection.prototype.selectUpdate = function(selectQuery, updateQuery, callback) {
    this.pool.getConnection(function (err, connection) {
        if(err) {
            Logger.append('./logs/log.txt', err);
            console.log("[MYSQL] Error: Connection NOT made".red + err);
            callback(null);
        } else if(connection) {
            connection.beginTransaction(function(err) {
                if(err) {
                    console.log("[MYSQL] Error: Transaction failed to begin".red + err);
                    callback(null);
                } else {
                    connection.query(selectQuery, function(err, result) {
                        if(err) {
                            connection.rollback(function() {
                                console.log("[MYSQL] Error: Rolled back transaction at SELECT! ".red + err);
                            });
                            connection.release();
                            callback(null);
                        } else {
                            if(typeof result !== 'undefined' && result && result.length > 0) {
                                var replay = result[0];
                                updateQuery += ' WHERE replayId= "' + replay.replayId + '"';
                                connection.query(updateQuery, function(err, result) {
                                    if(err) {
                                        connection.rollback(function() {
                                            console.log("[MYSQL] Error: Rolled back transaction at UPDATE! ".red + err);
                                        });
                                        connection.release();
                                        callback(null);
                                    } else {
                                        connection.commit(function(err) {
                                            if(err) {
                                                connection.rollback(function() {
                                                    console.log("[MYSQL] Error: Rolled back transaction at COMMIT! ".red + err);
                                                });
                                                connection.release();
                                                callback(null);
                                            } else {
                                                connection.release();
                                                callback(replay);
                                            }
                                        })
                                    }
                                });
                            } else{
                                connection.release();
                                callback(null);
                            }
                        }
                    });
                }
            });
        }
    });
};

Connection.prototype.query = function(queryString, callback) {
    this.pool.getConnection(function (err, connection) {
        if(err) {
            Logger.append('./logs/log.txt', err);
            console.log("[MYSQL] Error: Connection NOT made".red);
        }
        if(connection) {
            connection.query(queryString, function(err, rows) {
                if(err) {
                    Logger.append('./logs/log.txt', err);
                    Logger.append('./logs/log.txt', 'QUERYSTRING: ' + queryString);
                    console.log("[MYSQL] Error: Query: ".red + queryString + ", was not successful".red);
                }
                if(typeof rows !== "undefined" && rows  && rows.affectedRows > 1) {
                    console.log('[MYSQL] Saved: '.green + rows.affectedRows + ' rows'.green);
                }
                connection.release();
                callback(rows);
            });
        }
    });
};

module.exports = Connection;