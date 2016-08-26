var fs = require('fs');

var Logger = function() {};

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