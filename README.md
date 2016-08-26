## Dependencies
Before running any of the commands below, please ensure that PM2 is installed on the box running this software.  PM2 is not a local dependency due to it being a fairly large library and for the fact that it can act as a container for multiple application, thus deeming it useful for global installation.

`npm install pm2 -g`

After installing pm2, please run `npm install` to ensure that you download all project dependencies.

## To run parser:
Before running the parser, make a `.env` file in the root of this directory.  A `.env.example` file has been provided which contains all constants needed for the operation of this software.  Disabling `DEBUG` will stop `console.log` messages to be written and should **NOT** be used in production.  If you do not set a number for the `WORKERS` environment var, then the application will default to using a single worker, it's recommended to run the application with `150` workers on a t2.large AWS instance, any more workers than this may be throttled due to the bottleneck caused by exceeding the maximum amount of outbound network requests per second.

Once you are happy with the `.env` file check `parser.yaml`, in here you can increase the amount of clusters which will run the parser.  A default of 1 cluster will run (this is because each Node.js process is limited to 1 CPU per process by nature, upping the number of `instances` will allow node to utilise more CPU's, each process will communicate via IPC).  If you would like to ensure that the application restarts on an uncaught exception, then set the `watch` variable to true.

Finally, running:  `pm2 start parser.yaml` will start the parser.

## To run scraper:
The scraper does not need to be ran through PM2, it is set up to run on 1 cluster and will restart itself on an uncaught exception, you can run the parser with: `node scraper.js`

