var fs = require('fs');

var Logger = function() {};

Logger.log = function(filename, data) {
    fs.writeFile(filename, data, function(err) {
        if(err) console.log('caller: ' + data + ', ERROR: ' + JSON.stringify(err));
    });
};

Logger.append = function(filename, data) {
    fs.appendFile(filename, ('\n' + data), function(err) {
        if(err) console.log('caller: ' + data + ', ERROR: ' + JSON.stringify(err));
    });
};

module.exports = Logger;