var mysql = require('mysql');
var conf = require('../conf.js');
var Logger = require('./Logger');
var colors = require('colors');

var Connection = function() {
    this.pool = mysql.createPool({
        host:     conf.HOST,
        user:     conf.USER,
        password: conf.PASSWORD,
        database: conf.DATABASE,
        port:     3306
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
                    console.log("[MYSQL] Error: Query not successful".red);
                }
                console.log('[MYSQL] Saved: '.green + rows.affectedRows + ' rows'.green);
                connection.release();
                callback();
            });
        }
    });
};

module.exports = Connection;