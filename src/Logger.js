var fs = require('fs');

var Logger = function() {};

Logger.log = function(filename, data) {
    fs.writeFile(filename, data, function(err) {
        if(err) Logger.append('./logs/log.txt', err);
        console.log('saved to ' + filename);
    });
};

Logger.append = function(filename, data) {
    console.log('appending');
    fs.appendFile(filename, ('\n' + data), function(err) {
        if(err) console.log(err);
        console.log('appended to ' + filename);
    });
};

module.exports = Logger;