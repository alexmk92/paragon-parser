var mysql = require('mysql');
//var conf = require('../conf.js');
var Logger = require('./Logger');
var colors = require('colors');

var Connection = function() {
    this.connection = mysql.createConnection({
        host:     process.env.SQL_HOST,
        user:     process.env.SQL_USER,
        password: process.env.SQL_PASSWORD,
        database: process.env.SQL_DATABASE,
        port:     3306
    });
};

Connection.prototype.end = function() {
    this.connection.end(function (err) {
        if(err) {
            Logger.writeToConsole('[SQL] Failed to kill remaining connections in the queue'.red);
        }
        // all connections in the pool have ended
    });
};

Connection.prototype.selectUpdate = function(selectQuery, updateQuery, callback) {
    this.connection.connect(function (err) {
        if(err) {
            Logger.append('./logs/log.txt', err);
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

Connection.prototype.query = function(queryString, callback) {
    this.connection.connect(function (err) {
        if(err) {
            Logger.append('./logs/log.txt', err);
            Logger.writeToConsole("[MYSQL] Error: Connection NOT made".red);
        } else {
            this.connection.query(queryString, function(err, rows) {
                if(err) {
                    Logger.append('./logs/log.txt', 'QUERYSTRING: ' + queryString);
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