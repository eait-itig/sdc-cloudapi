/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Author: Alex Wilson <alex@uq.edu.au>
 * Copyright 2019, The University of Queensland
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var restify = require('restify');
var schemas = require('joyent-schemas').cloudapi;
var util = require('util');
var vasync = require('vasync');
var net = require('net');
var mooremachine = require('mooremachine');
var watershed = require('watershed');

var shed = new watershed.Watershed();

function mount(server, before, pre) {
    assert.object(server, 'server');
    assert.ok(before, 'before');
    assert.optionalArrayOfFunc(pre, 'pre');

    pre = pre || [];

    server.post({
        path: '/:account/machines/:machine/exec',
        name: 'ExecMachineCommand',
        version: [ '8.4.0' ]
    }, before, execCommand);

    return server;
}

function execCommand(req, res, next) {
    var vm = req.machine;

    if (vm.brand !== 'joyent' && vm.brand !== 'joyent-minimal' &&
        vm.brand !== 'lx') {
        res.send(400, new restify.RestError({
            statusCode: 400,
            restCode: 'MachineIsHVM',
            message: 'Specified machine is an HVM and cannot execute commands'
        }));
        next();
        return;
    }

    if (vm.state !== 'running') {
        res.send(400, new restify.RestError({
            statusCode: 400,
            restCode: 'MachineStopped',
            message: 'Only running machines can execute commands'
        }));
        next();
        return;
    }

    if (res.claimUpgrade) {
        /*
         * Since cloudapi still runs with restify request domains enabled, we
         * need to exit that domain here if we want any errors in the VNC FSM to
         * be reported sensibly (since the request will end from restify's
         * perspective once we send the 101).
         *
         * This can be removed once domains and the uncaughtException handler
         * are turned off for cloudapi.
         */
        var reqdom = process.domain;
        reqdom.exit();

        var fsm = new ExecConnectionFSM();
        fsm.handle(req, res, next);

        reqdom.enter();
        return;
    }

    if (!req.params.argv || !Array.isArray(req.params.argv)) {
        res.send(400, new restify.RestError({
            statusCode: 400,
            restCode: 'InvalidArgument',
            message: 'The "argv" parameter is required and must be an Array'
        }));
        next();
        return;
    }

    var params = {
        'command': {
            'Cmd': req.params.argv
        }
    };
    req.sdc.cnapi.dockerExec(vm.compute_node, vm.id, params,
        function afterDockerExec(err, info) {
            if (err) {
                req.log.error(err, 'failed to start exec job');
                res.send(500, new restify.InternalServerError('Failed to ' +
                    'execute command'));
                next();
                return;
            }

            req.log.debug({ info: info }, 'exec job got connection info');

            var sock = net.createConnection({
                host: info.host,
                port: info.port
            });
            sock.on('connect', function () {
                res.header('content-type', 'application/x-json-stream');
                sock.pipe(res);
                sock.on('end', function () {
                    next();
                });
            });
            sock.on('error', function (serr) {
                req.log.error(serr, 'error from exec job socket');
                res.send(500, new restify.InternalServerError('Failed to ' +
                    'execute command'));
                next();
                return;
            });
    });
}

function ExecConnectionFSM() {
    this.req = undefined;
    this.res = undefined;
    this.next = undefined;
    this.err = undefined;
    this.log = undefined;
    this.upgrade = undefined;
    this.ws = undefined;
    this.socket = undefined;
    this.host = undefined;
    this.port = undefined;

    mooremachine.FSM.call(this, 'init');
}

util.inherits(ExecConnectionFSM, mooremachine.FSM);

ExecConnectionFSM.prototype.state_init = function state_init(S) {
    S.on(this, 'handleAsserted', function handleAsserted() {
        S.gotoState('upgrade');
    });
};

ExecConnectionFSM.prototype.state_reject = function state_rejectsock(S) {
    var err = new restify.InternalServerError();
    var code = err.statusCode;
    var data = JSON.stringify(err.body);
    this.upgrade.socket.write('HTTP/1.1 ' + code + ' Upgrade Rejected\r\n' +
        'Connection: close\r\n' +
        'Content-Type: application/json\r\n' +
        'Content-Length: ' + data.length + '\r\n\r\n');
    this.upgrade.socket.end(data);
    this.next();
};

ExecConnectionFSM.prototype.state_upgrade = function state_upgrade(S) {
    try {
        this.upgrade = this.res.claimUpgrade();
        /*
         * Since VNC prefers low latency over high bandwidth, disable Nagle's
         * algorithm. This means that small data packets (e.g. mouse movements
         * or key presses) will be sent immediately instead of waiting for
         * further data.
         */
        this.upgrade.socket.setNoDelay(true);

        this.ws = shed.accept(this.req, this.upgrade.socket, this.upgrade.head);
    } catch (ex) {
        this.log.error(ex, 'websocket upgrade failed');
        S.gotoState('reject');
        return;
    }
    /*
     * From restify's perspective, the HTTP request ends here. We set the
     * statusCode so that the audit logs show that we upgraded to websockets.
     */
    this.res.statusCode = 101;
    this.next();

    /* Now we continue on to use the websocket. */
    S.gotoState('awaitcmd');
};

ExecConnectionFSM.prototype.state_awaitcmd = function state_awaitcmd(S) {
    var self = this;

    S.on(this.ws, 'error', function connectWsError(err) {
        self.err = err;
        S.gotoState('error');
    });
    S.on(this.ws, 'end', function connectWsEnd() {
        S.gotoState('error');
    });

    S.on(this.ws, 'binary', function (data) {
        var msg = JSON.parse(data.toString('utf-8'));
        if (Array.isArray(msg.argv) && msg.argv.length > 0) {
            self.argv = msg.argv;
            S.gotoState('startexec');
        }
    });
};

ExecConnectionFSM.prototype.state_startexec = function state_getport(S) {
    var req = this.req;
    var vm = req.machine;
    var self = this;
    var params = {
        'command': {
            'Cmd': this.argv,
            'AttachStdin': true,
            'AttachStdout': true,
            'AttachStderr': true,
            'Tty': true
        }
    };
    req.sdc.cnapi.dockerExec(vm.compute_node, vm.id, params,
        function afterDockerExec(err, info) {
            if (err) {
                this.err = err;
                S.gotoState('error');
                return;
            }

            req.log.debug({ info: info }, 'exec job got connection info');

            self.info = info;
            S.gotoState('connect');
    });
};

ExecConnectionFSM.prototype.state_error = function state_error(S) {
    this.log.warn(this.err, 'vnc connection exited with error');
    if (this.ws) {
        try {
            this.ws.end(JSON.stringify({ type: 'error', error: this.err }));
        } catch (ex) {
            this.ws.destroy();
        }
    }
    if (this.socket) {
        this.socket.destroy();
    }
};

ExecConnectionFSM.prototype.state_connect = function state_connect(S) {
    var self = this;

    S.on(this.ws, 'error', function connectWsError(err) {
        self.err = err;
        S.gotoState('error');
    });
    S.on(this.ws, 'end', function connectWsEnd() {
        S.gotoState('error');
    });

    this.socket = net.createConnection({
        allowHalfOpen: true,
        host: this.info.host,
        port: this.info.port
    });

    S.on(this.socket, 'connect', function connected() {
        S.gotoState('connected');
    });
    S.on(this.socket, 'error', function connectSockErr(err) {
        self.log.error(err, 'failed to connect to exec endpoint');
        self.err = new restify.InternalServerError('Failed to connect to ' +
                'exec server');
        S.gotoState('error');
    });
    S.timeout(5000, function connectTimeout() {
        self.log.error('timeout while connecting to exec endpoint');
        self.err = new restify.InternalServerError('Timeout while connecting ' +
            'to exec server');
        S.gotoState('error');
    });
};

ExecConnectionFSM.prototype.state_connected = function state_connected(S) {
    var self = this;
    this.socket.setNoDelay(true);

    S.on(this.ws, 'error', function vncWsError(err) {
        self.log.error(err, 'error on websocket connection to client');
        self.err = err;
        S.gotoState('error');
    });
    S.on(this.ws, 'end', function vncWsEnd() {
        S.gotoState('ws_ended');
    });
    S.on(this.ws, 'connectionReset', function vncWsReset() {
        S.gotoState('ws_ended');
    });

    S.on(this.socket, 'end', function vncSockEnd() {
        S.gotoState('sock_ended');
    });
    S.on(this.socket, 'error', function vncSockErr(err) {
        self.log.error(err, 'error on exec connection');
        S.gotoState('error');
    });

    S.on(this.ws, 'binary', function vncWsGotData(buf) {
        self.socket.write(buf);
    });
    S.on(this.socket, 'readable', function vncSockGotData() {
        var buf;
        while ((buf = self.socket.read()) !== null) {
            self.ws.send(buf);
        }
    });
};

ExecConnectionFSM.prototype.state_ws_ended = function state_ws_ended(S) {
    S.on(this.socket, 'close', function vncSockClose() {
        S.gotoState('closed');
    });
    S.timeout(5000, function vncSockCloseTimeout() {
        S.gotoState('error');
    });
    this.socket.end();
    this.socket = null;
};

ExecConnectionFSM.prototype.state_sock_ended = function state_sock_ended(S) {
    this.ws.end('Remote connection closed');
    this.ws = null;
    S.gotoState('closed');
};

ExecConnectionFSM.prototype.state_closed = function state_closed(S) {
    if (this.socket) {
        this.socket.destroy();
    }
    this.socket = null;
    if (this.ws) {
        this.ws.destroy();
    }
    this.ws = null;
};

ExecConnectionFSM.prototype.handle = function handle(req, res, next) {
    this.req = req;
    this.res = res;
    this.next = next;
    this.log = this.req.log.child({ component: 'ExecConnectionFSM' });

    this.emit('handleAsserted');
};

module.exports = {
    mount: mount
};
