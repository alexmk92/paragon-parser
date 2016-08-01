var fs = require('fs');

var Logger = function() {};

Logger.log = function(filename, data) {
    fs.writeFile(filename, data, function(err) {
        if(err) Logger.append('./logs/log.txt', err);
    });
};

Logger.append = function(filename, data) {
    fs.appendFile(filename, ('\n' + data), function(err) {
        if(err) console.log(err);
    });
};

module.exports = Logger;