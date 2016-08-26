var fs = require('fs');

var Logger = function() {};

Logger.log = function(filename, data) {
    var message = '[' + new Date().toUTCString() + ']' + data;
    fs.writeFile(filename, message, function(err) {
        //if(err) console.log('caller: ' + data + ', ERROR: ' + JSON.stringify(err));
    });
};

Logger.append = function(filename, data) {
    var message = '[' + new Date().toUTCString() + ']' + data;
    fs.appendFile(filename, ('\n' + message), function(err) {
        if(err) console.log('caller: ' + data + ', ERROR: ' + JSON.stringify(err));
    });
};

Logger.writeToConsole = function(message, dataObject) {
    // Data object is an optional parameter, if its sent we can print json object in the terminal
    if(process.env.DEBUG === 'true') {
        if(dataObject) {
            console.log(message, dataObject);
        } else {
            console.log(message);
        }
    }
};

module.exports = Logger;