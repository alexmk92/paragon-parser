require('dotenv').config();
require('colors');

var net = require('net');
var Logger = require('./Logger');
var Connection = require('./Connection');

var clients = [];
var lockedBy = null;
var resumeServingAt = new Date();

/**
 * @QueueManager :
 * ---------------
 * Queue manager acts as an API, he receives requests over TCP and
 * sends bulks of 1000 replays back to QueueClient.js.
 *
 * Queue manager acts as the single interface that all QueueClient
 * instances interact with to transport replay payloads.
 *
 * When Queue manager starts, we dispose of any reserved resources
 * that were held by the previous run, this handles the edge case
 * where the server crashed and couldn't dispose of the resources
 * it reserved before closing.
 *
 * @param {object} socket - The QueueClient socket that connected
 * this contains its process id so that QueueManager can dispose
 * of any resources that they own.
 */

cleanOnStart(function() {
    net.createServer(function(socket) {
        socket.on('data', function(data) {
            data = JSON.parse(data);
            switch(data.action) {
                case 'connectWithProcessId':
                    if(typeof data.processId === 'undefined' || data.processId === null || data.processId.trim() === "") {
                        socket.write(JSON.stringify({ code: 400, action: 'connect', message: 'The socket did not connect with its unique processId, please ensure processId is sent in connection packet.' }));
                    } else {
                        var exists = false;
                        clients.some(function(client) {
                            exists = client.processId === data.processId;
                            return exists;
                        });
                        if(exists) {
                            socket.write(JSON.stringify({ code: 409, action: 'connect', message: 'Process: ' + data.processId + ' already exists on the server!' }));
                        } else {
                            socket.processId = data.processId;
                            socket.hasReservedReplays = false;
                            clients.push(socket);
                            socket.write(JSON.stringify({ code: 200, action: 'connect', message: 'Process: ' + data.processId + ' successfully connected to the server!' }));
                        }
                    }
                    break;
                case 'getReplays':
                    if(lockedBy === null) {
                        lockedBy = socket.processId;
                        Logger.writeToConsole('Client: '.green + socket.processId + ' requested the next '.green + process.env.REPLAY_FETCH_AMOUNT + ' replays'.green);

                        fetchAndReserveReplays(function(replays) {
                            socket.hasReservedReplays = replays !== null;
                            socket.write(JSON.stringify({ code: 200, action: 'replays', message: 'Reserved ' + process.env.REPLAY_FETCH_AMOUNT + ' replays for process: ' + lockedBy, body: replays }));
                            lockedBy = null;
                        });
                    } else {
                        Logger.writeToConsole('Client '.red + socket.processId + ' cannot reserve right now as '.red + lockedBy + ' is fetching replays'.red);
                        socket.write(JSON.stringify({ code: 409, action: 'replays', message: 'Process: ' + lockedBy + ' is already reserving replays, try again later', body: null }));
                    }
                    break;
            }
        });

        socket.on('end', function() {
            dispose(socket);
        });
        socket.on('error', function() {
            dispose(socket);
        });
    }).listen(process.env.QUEUE_MANAGER_PORT);

    Logger.writeToConsole('['.green + process.pid + '] QueueManager server running on: '.green + process.env.QUEUE_MANAGER_HOST + ':' + process.env.QUEUE_MANAGER_PORT);

    // Disposes of the socket
    function dispose(socket) {
        releaseReservedResourcesForProcess(socket, function() {
            clients.splice(clients.indexOf(socket), 1);
            if(clients.length > 0) {
                broadcast(200, 'disconnect', 'Process: ' + socket.processId + ' disconnected from the server!');
            }
        });
    }

    // Broadcast will write to all sockets
    function broadcast(code, action, message) {
        clients.forEach(function(socket) {
            socket.write(JSON.stringify({ code: code, action: action, message: message }));
        });
    }

    // Fetch the next process.env.REPLAY_FETCH_AMOUNT replays and reserve them for this box
    function fetchAndReserveReplays(callback) {
        if(new Date() >= resumeServingAt) {
            var conn = new Connection();
            // Optimised Update query
            var updateQuery = 'UPDATE queue a ' +
                    'JOIN ( ' +
                    '           SELECT replayId, reserved_by ' +
                    '           FROM queue ' +
                    '           WHERE reserved_by IS NULL ' +
                    '           AND completed = false ' +
                    '           AND scheduled <= NOW() ' +
                    '           ORDER BY priority DESC ' +
                    '           LIMIT ' + process.env.REPLAY_FETCH_AMOUNT +
                    '     ) AS b ' +
                    'ON a.replayId = b.replayId ' +
                    'SET a.reserved_by="' + lockedBy + '", reserved_at = NOW(), priority = 0';

            conn.query(updateQuery, function(rows) {
                if(typeof rows !== 'undefined' && rows !== null && rows.hasOwnProperty('affectedRows') && rows.affectedRows > 0) {
                    return callback(rows.affectedRows);
                } else {
                    // lock for 10 seconds in future
                    resumeServingAt = new Date((new Date().getTime() + 10000));
                    return callback(null);
                }
            });
        } else {
            callback(null);
        }
    }

    // Dispose of resources from a specific process ID
    function releaseReservedResourcesForProcess(socket, callback) {
        if(socket.hasReservedReplays) {
            var conn = new Connection();
            // Set the highest priority as the only reason this replay stopped getting processed was due to the process crashing
            Logger.writeToConsole('Disposing of all replays reserved by: '.yellow + socket.processId);
            var updateQuery = 'UPDATE queue SET priority=5, completed=false, checkpointTime=0, reserved_by=NULL WHERE reserved_by="' + socket.processId + '"';
            conn.query(updateQuery, function(rows) {
                if(typeof rows !== 'undefined' && rows !== null && rows.hasOwnProperty('affectedRows') && rows.affectedRows > 0) {
                    Logger.writeToConsole('Released: '.green + rows.affectedRows + ' replays for process: '.green + socket.processId);
                } else {
                    Logger.writeToConsole('Process: '.green + socket.processId + ' did not have any replays reserved.'.green);
                }
                return callback();
            });
        } else {
            return callback();
        }
    }
});

function cleanOnStart(callback) {
    // Dispose of all replays
    var updateQuery = 'UPDATE queue SET priority=5, completed=false, checkpointTime=0, reserved_by=NULL WHERE reserved_by IS NOT NULL';
    var conn = new Connection();
    conn.query(updateQuery, function(rows) {
        if(typeof rows !== 'undefined' && rows !== null && rows.hasOwnProperty('affectedRows') && rows.affectedRows > 0) {
            Logger.writeToConsole('Disposed of: '.green + rows.affectedRows + ' replays, server started successfully!'.green);
        } else {
            Logger.writeToConsole('The server had not reserved any replays, server started successfully!'.green);
        }
        return callback();
    });
}


