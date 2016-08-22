var mysql = require('mysql');
var conf = require('../conf.js');
var Logger = require('./Logger');
var colors = require('colors');

var Connection = function() {
    this.connection = mysql.createConnection({
        host:     conf.HOST,
        user:     conf.USER,
        password: conf.PASSWORD,
        database: conf.DATABASE,
        port:     3306
    });
};

Connection.prototype.end = function() {
    this.connection.end(function (err) {
        console.log("ENDED CONNECTION".blue);
        if(err) {
            console.log('[SQL] Failed to kill remaining connections in the queue'.red);
        }
        // all connections in the pool have ended
    });
};

Connection.prototype.selectUpdate = function(selectQuery, updateQuery, callback) {
    this.connection.connect(function (err) {
        if(err) {
            Logger.append('./logs/log.txt', err);
            console.log("[MYSQL] Error: Connection NOT made".red + err);
            callback(null);
        } else {
            this.connection.beginTransaction(function(err) {
                if(err) {
                    console.log("[MYSQL] Error: Transaction failed to begin".red + err);
                    callback(null);
                } else {
                    this.connection.query(selectQuery, function(err, result) {
                        if(err) {
                            this.connection.rollback(function() {
                                console.log("[MYSQL] Error: Rolled back transaction at SELECT! ".red + err);
                            });
                            this.end();
                            callback(null);
                        } else {
                            if(typeof result !== 'undefined' && result && result.length > 0) {
                                var replay = result[0];
                                updateQuery += ' WHERE replayId= "' + replay.replayId + '"';
                                this.connection.query(updateQuery, function(err, result) {
                                    if(err) {
                                        this.connection.rollback(function() {
                                            console.log("[MYSQL] Error: Rolled back transaction at UPDATE! ".red + err);
                                        });
                                        this.end();
                                        callback(null);
                                    } else {
                                        this.connection.commit(function(err) {
                                            if(err) {
                                                this.connection.rollback(function() {
                                                    console.log("[MYSQL] Error: Rolled back transaction at COMMIT! ".red + err);
                                                });
                                                this.end();
                                                callback(null);
                                            } else {
                                                this.end();
                                                callback(replay);
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

Connection.prototype.query = function(queryString, callback) {
    this.connection.connect(function (err) {
        if(err) {
            Logger.append('./logs/log.txt', err);
            console.log("[MYSQL] Error: Connection NOT made".red);
        } else {
            this.connection.query(queryString, function(err, rows) {
                if(err) {
                    Logger.append('./logs/log.txt', 'QUERYSTRING: ' + queryString);
                    console.log("[MYSQL] Error: Query: ".red + queryString + ", was not successful".red);
                }
                if(typeof rows !== "undefined" && rows  && rows.affectedRows > 1) {
                    console.log('[MYSQL] Saved: '.green + rows.affectedRows + ' rows'.green);
                }
                callback(rows);
                this.end();
            }.bind(this));
        }
    }.bind(this));
};

module.exports = Connection;