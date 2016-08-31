var net = require('net');

describe("QueueManager", function() {
    var QueueManager = require('../src/QueueManager');
    // This first test is simply to clear out old content, it delays 1s so we dont get host conn problems
    it("should clear out all previous reserved replays before starting", function(done) {
        setTimeout(function() {
            done();
        }, 1000);
    });
    it("should respond with a welcome string once the socket is registered with its process id", function(done) {
        var client = createClient('ttdsgg');

        client.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: ttdsgg successfully connected to the server!');
            }
            client.destroy();
            done();
        });
    });
    it("should fail if the client socket does not send its processId or if the processId is null or an empty string", function(done) {
        var client = createClient(null);

        client.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(400);
                expect(data.message).toEqual('The socket did not connect with its unique processId, please ensure processId is sent in connection packet.');
            }
            client.destroy();
            done();
        });
    });
    it("should fail if there is another client with the same processId on the server", function(done) {
        var clientA = createClient('abcdefg');
        clientA.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: abcdefg successfully connected to the server!');
            }

            // Nest client b connection here so we can fire after A - make sure they conn with same ID
            var clientB = createClient('abcdefg');
            clientB.on('data', function(data) {
                data = JSON.parse(data);
                if(data.action === 'connect') {
                    expect(data.code).toBe(409);
                    expect(data.message).toEqual('Process: abcdefg already exists on the server!');
                }
                clientA.destroy();
                clientB.destroy();
                done();
            });
        });
    });
    it("should re-create a new processId if one already exists on the server", function(done) {
        var clientA = createClient('abcdefg');
        clientA.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: ' + clientA.processId + ' successfully connected to the server!');
            }

            // Nest client b connection here so we can fire after A - make sure they conn with same ID
            var clientB = createClient('abcdefg');
            clientB.on('data', function(data) {
                data = JSON.parse(data);
                if(data.action === 'connect') {
                    expect(data.code).toBe(409);
                    expect(data.message).toEqual('Process: ' + clientB.processId + ' already exists on the server!');
                    if(data.code === 409) {
                        setTimeout(function() {
                            clientB = createClient(clientB.processId + Math.floor((Math.random() * 999) + 1));
                            clientB.on('data', function(data) {
                                data = JSON.parse(data);
                                if(data.code === 200) {
                                    expect(data.code).toBe(200);
                                    expect(data.message).toEqual('Process: ' + clientB.processId + ' successfully connected to the server!');
                                    clientA.destroy();
                                    clientB.destroy();
                                    done();
                                }
                            });
                        }, 500);
                    }
                }
            });
        });
    });
    it("should broadcast to other clients when a socket disconnects", function(done) {
        var clientA = createClient('qq4rere');
        clientA.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: qq4rere successfully connected to the server!');

                // Nest client b connection here so we can fire after A - make sure they conn with same ID
                var clientB = createClient('5543dfdf');
                clientB.on('data', function(data) {
                    data = JSON.parse(data);
                    if(data.action === 'connect') {
                        expect(data.code).toBe(200);
                        expect(data.message).toEqual('Process: 5543dfdf successfully connected to the server!');
                        clientA.destroy();
                    } else if(data.action === 'disconnect') {
                        expect(data.code).toBe(200);
                        expect(data.message).toEqual('Process: ' + clientA.processId + ' disconnected from the server!');
                        clientB.destroy();
                        done();
                    }
                });
            }
        });
    });
    it("should only allow one client to hold the lock when making a request at a time", function(done) {
        var requestedReplaysA = false;
        var requestedReplaysB = false;

        var clientA = createClient('ccddefd3s');
        var clientB = createClient('1cvBde234');

        clientA.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: ccddefd3s successfully connected to the server!');
                clientA.write(JSON.stringify({ action: 'getReplays' }))
            } else if(data.action === 'replays') {
                if(data.code === 409) {
                    requestedReplaysA = false;
                    expect(data.code).toBe(409);
                    expect(data.message).toEqual('Process: 1cvBde234 is already reserving replays, try again later');
                    expect(data.body).toBe(null);
                    clientA.destroy();
                    clientB.destroy();
                    done();
                } else {
                    if(!requestedReplaysA) {
                        requestedReplaysA = true;
                        expect(data.code).toBe(200);
                        expect(data.message).toEqual('Reserving ' + process.env.REPLAY_FETCH_AMOUNT + ' replays for process: ccddefd3s');
                        expect(data.body !== null).toBe(true);
                    }
                }
            }
        });
        clientB.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: 1cvBde234 successfully connected to the server!');
                clientB.write(JSON.stringify({ action: 'getReplays' }))
            } else if(data.action === 'replays') {
                if(data.code === 409) {
                    requestedReplaysB = false;
                    expect(data.code).toBe(409);
                    expect(data.message).toEqual('Process: ccddefd3s is already reserving replays, try again later');
                    expect(data.body).toBe(null);
                    clientA.destroy();
                    clientB.destroy();
                    done();
                } else {
                    if(!requestedReplaysB) {
                        requestedReplaysB = true;
                        expect(data.code).toBe(200);
                        expect(data.message).toEqual('Reserving ' + process.env.REPLAY_FETCH_AMOUNT + ' replays for process: 1cvBde234');
                        expect(data.body !== null).toBe(true);
                    }
                }
            }
        });
    });
    it("allows each process to eventually reserve replays once the process locking QueueManager finishes its work", function(done) {
        var completedCount = 0;

        var clientA = createClient('ccddefd3s');
        var clientB = createClient('1cvBde234');

        clientA.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: ' + clientA.processId + ' successfully connected to the server!');
                clientA.write(JSON.stringify({ action: 'getReplays' }))
            } else if(data.action === 'replays') {
                if(data.code === 409) {
                    expect(data.code).toBe(409);
                    //expect(data.message).toEqual('Process: ' + clientB.processId + ' is already reserving replays, try again later');
                    expect(data.body).toBe(null);
                    setTimeout(function() {
                        clientA.write(JSON.stringify({ action: 'getReplays' }))
                    }, 500);
                } else if(data.code === 200) {
                    expect(data.code).toBe(200);
                    expect(data.message).toEqual('Reserving ' + process.env.REPLAY_FETCH_AMOUNT + ' replays for process: ' + clientA.processId);
                    expect(data.body !== null).toBe(true);
                    completedCount++;
                    if(completedCount === 2) {
                        expect(completedCount).toBe(2);
                        clientA.destroy();
                        clientB.destroy();
                        done();
                    }
                }
            }
        });
        clientB.on('data', function(data) {
            data = JSON.parse(data);
            if(data.action === 'connect') {
                expect(data.code).toBe(200);
                expect(data.message).toEqual('Process: ' + clientB.processId + ' successfully connected to the server!');
                clientB.write(JSON.stringify({ action: 'getReplays' }))
            } else if(data.action === 'replays') {
                if(data.code === 409) {
                    expect(data.code).toBe(409);
                    expect(data.message).toEqual('Process: ' + clientA.processId + ' is already reserving replays, try again later');
                    expect(data.body).toBe(null);
                    setTimeout(function() {
                        clientB.write(JSON.stringify({ action: 'getReplays' }))
                    }, 500);
                } else if(data.code === 200) {
                    expect(data.code).toBe(200);
                    expect(data.message).toEqual('Reserving ' + process.env.REPLAY_FETCH_AMOUNT + ' replays for process: ' + clientB.processId);
                    expect(data.body !== null).toBe(true);
                    completedCount++;
                    if(completedCount === 2) {
                        expect(completedCount).toBe(2);
                        clientA.destroy();
                        clientB.destroy();
                        done();
                    }
                }
            }
        });
    });
});

/*
describe("QueueClient", function() {
    var QueueClient = require('../src/QueueClient');
    var qc = new QueueClient(null, 5, 'xb2xxvdez');

    it("")
});
*/

/**
 * @createClient :
 * ---------------
 * Creates a new TCP socket so that we can make requests in Jasmine
 *
 * @param {string} processId - The id of the server we are running
 * @returns {net.Socket} - The socket to
 */

function createClient(processId) {
    var client = new net.Socket();
    client.processId = processId;
    client.connect(process.env.QUEUE_MANAGER_PORT, process.env.QUEUE_MANAGER_HOST, function() {
        client.write(JSON.stringify({ action: 'connectWithProcessId', processId: processId }));
    });
    return client;
}