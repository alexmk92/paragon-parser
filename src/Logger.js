var fs = require('fs');

var Logger = function() {};

/**
 * @writeToConsole :
 * -----------------
 * A controlled write to console.log, the DEBUG environment variable set in .env
 * determines if we should write to the console or not (don't set DEBUG to true in production
 * to save CPU).
 *
 * @param {string} message - The message to log, this is encoded with the chalk lib to print coloured
 * messages
 * @param {object} dataObject - If we need to log the contents of an object (i.e we want to see what was returned
 * from an API request, then we pass the raw raw object and log with console.log(string, object) syntax.
 */

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