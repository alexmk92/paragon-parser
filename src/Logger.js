var fs = require('fs');

var Logger = function() {};

Logger.log = function(filename, data) {
    var message = '[' + new Date() + ']' + data;
    fs.writeFile(filename, message, function(err) {
        if(err) console.log('caller: ' + data + ', ERROR: ' + JSON.stringify(err));
    });
};

Logger.append = function(filename, data) {
    var message = '[' + new Date() + ']' + data;
    fs.appendFile(filename, ('\n' + message), function(err) {
        if(err) console.log('caller: ' + data + ', ERROR: ' + JSON.stringify(err));
    });
};

module.exports = Logger;