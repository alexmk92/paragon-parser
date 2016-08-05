var mysql = require('mysql');
var conf = require('../conf.js');
var Logger = require('./Logger');

var Connection = function() {
    this.pool = mysql.createPool({
        host:     conf.HOST,
        user:     conf.USER,
        password: conf.PASSWORD,
        database: conf.DATABASE,
        port:     3306
    });
};

Connection.prototype.query = function(query) {
    var query = query;
    this.pool.getConnection(function (err, connection) {
        console.log(test);
        console.log(query);
        if(err) {
            Logger.append('./logs/log.txt', err);
            console.log("connection NOT made");
        }
        if(connection) {
            console.log("query: " + query);
            var query = connection.query(query, function(err, rows) {
                connection.release();
            });
        }
    });

};

module.exports = Connection;