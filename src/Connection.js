var mysql = require('mysql');
var conf = require('../conf.js');
var Logger = require('./Logger');

var Connection = function() {
    this.connection = mysql.createConnection({
        host: conf.HOST,
        user: conf.USER,
        password: conf.PASSWORD,
        database: conf.DATABASE
    });

    this.connection.connect(function(err) {
        if(err) Logger.append('./logs/log.txt', err);
    });
};

Connection.prototype.query = function(query, cb) {
    this.connection.query(query, function(err, rows) {
        if(err) Logger.append('./logs/log.txt', err);
        console.log(rows);
    });
};

module.exports = Connection;