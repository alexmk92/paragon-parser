require('dotenv').config();

var Logger = require('./src/Logger');

var Memcached = require('memcached');
var memcached = new Memcached('paragongg-queue.t4objd.cfg.use1.cache.amazonaws.com:11211');

memcached.del('replays', function(err) {
    if(err) {
        Logger.writeToConsole('Failed to clear cache, or there was no replay key in memcached.'.red);
    } else {
        Logger.writeToConsole('Successfully cleared replays from memcached.'.green);
    }
});