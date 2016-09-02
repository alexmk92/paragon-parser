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
            this.end();
            return callback(null);
        } else {
            this.connection.query(queryString, function(err, rows) {
                if(err) {
                    //
                    Logger.writeToConsole("[MYSQL] Error: Query: ".red + queryString + ", was not successful.".red);
                    this.end();
                    return callback(null);
                }
                if(typeof rows !== "undefined" && rows  && rows.affectedRows > 1) {
                    Logger.writeToConsole('[MYSQL] '.green + rows.affectedRows + ' rows affected.'.green);
                }
                this.end();
                return callback(rows);
            }.bind(this));
        }
    }.bind(this));
};

module.exports = Connection;