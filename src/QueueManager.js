require('dotenv').config();
require('colors');

var net = require('net');
var Connection = require('./Connection');

var clients = [];
var lockedBy = null;

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
                        console.log('Client: '.green + socket.processId + ' requested the next '.green + process.env.REPLAY_FETCH_AMOUNT + ' replays'.green);

                        fetchAndReserveReplays(function(replays) {
                            socket.hasReservedReplays = replays !== null;
                            socket.write(JSON.stringify({ code: 200, action: 'replays', message: 'Reserving ' + process.env.REPLAY_FETCH_AMOUNT + ' replays for process: ' + lockedBy, body: replays }));
                            lockedBy = null;
                        });
                    } else {
                        console.log('Client '.red + socket.processId + ' cannot reserve right now as '.red + lockedBy + ' is fetching replays'.red);
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
    }).listen(process.env.QUEUE_MANAGER_PORT, process.env.QUEUE_MANAGER_HOST);

    console.log('['.green + process.pid + '] QueueManager server running on: '.green + process.env.QUEUE_MANAGER_HOST + ':' + process.env.QUEUE_MANAGER_PORT);

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
        var conn = new Connection();
        var updateQuery = 'UPDATE queue ' +
            'SET reserved_by="' + lockedBy + '", reserved_at=NOW(), priority=0 ' +
            'WHERE replayId IN ' +
            '   (SELECT replayId FROM (' +
            '       SELECT replayId ' +
            '       FROM queue ' +
            '       WHERE completed=false AND reserved_by IS NULL AND scheduled <= NOW() ' +
            '       ORDER BY priority DESC LIMIT ' + process.env.REPLAY_FETCH_AMOUNT + '' +
            '   ) ' +
            'AS t)';

        conn.query(updateQuery, function(rows) {
            if(rows.hasOwnProperty('affectedRows') && rows.affectedRows > 0) {
                callback(rows.affectedRows);
            } else {
                callback(null);
            }
        })
    }

    // Dispose of resources from a specific process ID
    function releaseReservedResourcesForProcess(socket, callback) {
        if(socket.hasReservedReplays) {
            var conn = new Connection();
            // Set the highest priority as the only reason this replay stopped getting processed was due to the process crashing
            console.log('Disposing of all replays reserved by: '.yellow + socket.processId);
            var updateQuery = 'UPDATE queue SET priority=5, completed=false, checkpointTime=0, reserved_by=NULL WHERE reserved_by="' + socket.processId + '"';
            conn.query(updateQuery, function(rows) {
                if(rows.hasOwnProperty('affectedRows') && rows.affectedRows > 0) {
                    console.log('Released: '.green + rows.affectedRows + ' replays for process: '.green + socket.processId);
                } else {
                    console.log('Process: '.green + socket.processId + ' did not have any replays reserved.'.green);
                }
                callback();
            });
        } else {
            callback();
        }
    }
});

function cleanOnStart(callback) {
    // Dispose of all replays
    var updateQuery = 'UPDATE queue SET priority=5, completed=false, checkpointTime=0, reserved_by=NULL WHERE reserved_by IS NOT NULL';
    var conn = new Connection();
    conn.query(updateQuery, function(rows) {
        if(rows.affectedRows > 0) {
            console.log('Disposed of: '.green + rows.affectedRows + ' replays, server shut started successfully!'.green);
        } else {
            console.log('The server had not reserved any replays, server shut started successfully!'.green);
        }
       callback();
    });
}


