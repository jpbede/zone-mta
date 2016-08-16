'use strict';

const config = require('config');
const log = require('npmlog');
const sendingZone = require('./sending-zone');
const Server = require('./transport/server');

class QueueServer {
    constructor() {
        this.queue = false;
        this.closing = false;
        this.clients = false;
        this.createServer();
    }

    createServer() {
        this.server = new Server();
        this.server.on('client', client => {
            client.responseHandlers = new Map();
            client.onData = (data, next) => {
                setImmediate(next); // release immediatelly

                if (!data.req) {
                    // ignore
                    return;
                }

                if (!this.queue) {
                    return client.send({
                        req: data.req,
                        error: 'Service not yet started'
                    });
                }

                let zone = sendingZone.get(data.zone);
                if (!zone) {
                    return client.send({
                        req: data.req,
                        error: 'Selected Sending Zone does not exist'
                    });
                }

                switch (data.cmd) {
                    case 'GET':
                        return this.findNext(zone, data, (err, delivery) => {
                            if (err) {
                                return client.send({
                                    req: data.req,
                                    error: err.message || err
                                });
                            }
                            client.send({
                                req: data.req,
                                response: delivery
                            });
                        });
                    case 'RELEASE':
                        return this.releaseDelivery(zone, data, (err, response) => {
                            if (err) {
                                return client.send({
                                    req: data.req,
                                    error: err.message || err
                                });
                            }
                            client.send({
                                req: data.req,
                                response
                            });
                        });
                    case 'DEFER':
                        return this.deferDelivery(zone, data, (err, response) => {
                            if (err) {
                                return client.send({
                                    req: data.req,
                                    error: err.message || err
                                });
                            }
                            client.send({
                                req: data.req,
                                response
                            });
                        });
                }
            };

        });
    }

    start(callback) {
        let returned = false;
        this.server.on('error', err => {
            if (returned) {
                return log.error('QS', err);
            }
            returned = true;
            return callback(err);
        });

        this.server.listen(config.queueServer.port, config.queueServer.host, () => {
            if (returned) {
                return this.server.close();
            }
            returned = true;
            callback(null, true);
        });
    }

    close(callback) {
        this.closing = true;
        this.server.close(callback);
    }

    // Finds and locks details for next delivery
    findNext(zone, req, callback) {
        zone.getNextDelivery(req.client, (err, delivery) => {
            if (err) {
                return callback(err);
            }

            if (!delivery) {
                return callback(null, false);
            }

            this.queue.getMeta(delivery.id, (err, meta) => {
                if (err) {
                    return callback(err);
                }

                Object.keys(meta || {}).forEach(key => {
                    delivery[key] = meta[key];
                });

                let data = {};
                Object.keys(delivery).forEach(key => {
                    if (!data.hasOwnProperty(key)) {
                        data[key] = delivery[key];
                    }
                });

                return callback(null, data);
            });
        });
    }

    // Marks a delivery as done (either bounced or accepted)
    // Does not check the validity of instance id since we need this data
    releaseDelivery(zone, req, callback) {
        this.queue.getDelivery(req.id, req.seq, (err, delivery) => {
            if (err) {
                return callback(err);
            }

            if (!delivery) {
                return callback(new Error('Delivery not found'));
            }

            delivery._lock = req._lock;
            zone.releaseDelivery(delivery, err => {
                if (err) {
                    return callback(err);
                }
                return callback(null, delivery.id + '.' + delivery.seq);
            });
        });
    }

    // Marks a delivery as deferred
    // Does not check the validity of instance id since we need this data
    deferDelivery(zone, req, callback) {
        this.queue.getDelivery(req.id, req.seq, (err, delivery) => {
            if (err) {
                return callback(err);
            }

            if (!delivery) {
                return callback('Delivery not found');
            }

            delivery._lock = req._lock;
            zone.deferDelivery(delivery, Number(req.ttl), err => {
                if (err) {
                    return callback(err);
                }
                return callback(null, delivery.id + '.' + delivery.seq);
            });
        });
    }
}

module.exports = options => new QueueServer(options);