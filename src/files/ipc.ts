import { MsgPackDecoder, MsgPackEncoder } from "msgpackstream";
import { commandOptions } from "redis";
import { Context, IpcRedisClient } from "..";

export enum IPCState {
    running,
    shuttingDown,
    cleanup
}

export enum IPCOptimize {
    speed, stability
}

export const IPCOptionDefaults = {
    pingInterval: 10,
    blockTimeout: 5,
    keepAliveHash: 'ACTIVE_WORKERS',
    keepAliveInstanceHash: 'ACTIVE_WORKER_INSTANCE',
    additionalStreams: [],
    broadcastStreams: [],
    streamLimit: 1000,
    globalCallLimit: 100,
    optimize: IPCOptimize.stability,
    concurencyGroupLimits: {},
    maxQueueLimit: 50,
    maxQueueResume: 10,
    commandHash: 'COMMANDS',
    ignoreDuplicateInstance: false
}

export interface IPCOptions {
    optimize?: IPCOptimize
    pingInterval?: number;
    blockTimeout?: number;
    workerName: string;
    instanceName: string;
    keepAliveHash?: string;
    keepAliveInstanceHash?: string;
    commandHash?: string;
    additionalStreams?: string[];
    broadcastStreams?: string[];
    streamLimit?: number;
    globalCallLimit?: number;
    concurencyGroupLimits?: { [name: string]: number };
    maxQueueLimit?: number;
    maxQueueResume?: number;
    ignoreDuplicateInstance?: boolean;
}

export interface IPCCall {
    sequence: string,
    replyTo: string;
    method: string;
    params?: any[];
    context: any
}

export interface IPCReply {
    sequence: string;
    result: any;
    error: any;
}

export enum IPCCallType {
    invoke,
    broadcast,
    event
}

export enum IPCAckType {
    begin,
    end,
    manual
}

export interface IIpcMethodOptions {
    methodName: string | '*';      /** default: name of method */
    waitTimeout?: number;           /** milliseconds; default: 10s */
    runTimeout?: number;            /** milliseconds; default: 10s */
    maxConcurrent?: number;         /** default: unlimited */
    concurencyGroup?: string; 
    prefetchRequestLimit?: number;      // default: disabled
    queue?: string;                 /** default: constructor */
    type: IPCCallType;
    ackType?: IPCAckType;
    cb: (...args:any[]) => Promise<any>;
    extra?: any;
    argOrder: string[];
}

export class IpcError extends Error {
    constructor(public msg: string, public status: string, public severity: string) { super() }
    toJSON(): any {
        return { msg: this.msg, status: this.status, severity: this.severity }; //TODO: add stack trace / error ID ect.ect.
    }
}

export class IPC {
    #redis: IpcRedisClient;
    #initState: Promise<void>;
    #reply_thread!: Promise<void>;
    #runState?: Promise<void>;

    #options: Required<IPCOptions>;

    #xgroup: string;
    #xconsumer: string;
    #xstreams: { key: string, id: string }[];
    #xbcstreams: { key: string, id: string }[];
    #replystream: { key: string, id: string };

    #status: IPCState = IPCState.running;
    #aliveTimer!: any;

    #activeCalls: { [id: string]: {accept: (result:any) => void, reject: (result: any) => void, timer: NodeJS.Timer}} = {};
    #sequence = + new Date();

    #cmdinfo: { [name: string]: IIpcMethodOptions } = {};

    #methodCache: { [name: string]: IIpcMethodOptions } = {};

    #activeRequests: { [name: string]: Set<Promise<any>> } = {};
    #queuedRequests: { [name: string]: { msg: IPCCall, queue: string, ticketID: string }[]} = {};
    #numTotalQueuedRequests = 0;

    #disableNewTickets: Promise<void>|null = null;
    #disableNewTicketAccept: (() => void)|null = null;

    onStopProcessing?: () => Promise<void>;
    onResumeProcessing?: () => Promise<void>;
    onShutdown?: () => Promise<void>;


    public get $init(): Promise<void> {
        return this.#initState;
    }

    constructor(redis: IpcRedisClient, settings: IPCOptions) {
        this.#redis = redis;
        this.#options = {...IPCOptionDefaults, ...settings};
        this.#xgroup = 'GROUP/'+this.#options.workerName;
        this.#xconsumer = 'CONSUMER/'+this.#options.instanceName;
        this.#xstreams = [ { key: 'STREAM/'+ this.#options.workerName, id: '>' }, ...(this.#options.additionalStreams).map(x => <any>{ key: 'STREAM/'+x, id: '>' })];
        this.#xbcstreams = [ { key: 'BROADCAST/' + this.#options.workerName, id: '$' }, ...(this.#options.broadcastStreams).map(x => <any>{ key: 'BROADCAST/'+x, id: '$'})];
        this.#replystream = { key: 'REPLY/' + this.#options.workerName + '/' + this.#options.instanceName, id: '$'};
        this.#initState = this.#init();
    }

    async #init(): Promise<void> {
        await this.#redis.hSetNX(this.#options.keepAliveHash, this.#options.workerName, (+new Date()).toString());
        const notRunning = await this.#redis.hSetNX(this.#options.keepAliveInstanceHash, this.#options.workerName + '/' + this.#options.instanceName, (+new Date()).toString());
        if(!notRunning && !this.#options.ignoreDuplicateInstance) {
            throw new Error('IPCWorker: workerType ' + this.#options.workerName + ' instance ' + this.#options.instanceName + ' is already running');
        }
        this.#aliveTimer = setInterval(() => { this.#keepalive().catch(() => null) }, this.#options.pingInterval*1000);
        await this.#keepalive();
        await Promise.all(this.#xstreams.map((stream) => {
            this.#redis.xGroupCreate(stream.key, this.#xgroup, '$', {MKSTREAM:true}).catch(() => null);
        }));   
        const streaminfo = await this.#redis.xInfoStream(this.#replystream.key).catch(async () => {
            await this.#redis.xGroupCreate(this.#replystream.key, 'DUMMY/' + this.#xgroup , '$', {MKSTREAM: true});
            await this.#redis.xGroupDestroy(this.#replystream.key, 'DUMMY/' + this.#xgroup);
            return this.#redis.xInfoStream(this.#replystream.key);
        });
        this.#replystream.id = streaminfo.lastGeneratedId;
        await Promise.all(this.#xbcstreams.map(async (bc) => {
           const streaminfo = await this.#redis.xInfoStream(bc.key).catch(async () => {
                await this.#redis.xGroupCreate(bc.key, 'DUMMY/' + this.#xgroup , '$', {MKSTREAM: true});
                await this.#redis.xGroupDestroy(bc.key, 'DUMMY/' + this.#xgroup);
                return this.#redis.xInfoStream(bc.key);
            });
            bc.id = streaminfo.lastGeneratedId;
        }));
        this.#reply_thread = this.#run_reply(this.#redis.duplicate())
    }
    /*
    #decode(msg: any): any {
        return JSON.parse(msg);
    }

    #encode(msg: any): any {
        return JSON.stringify(msg) ?? 'null';
    }
    */
    #msgpack_enc: MsgPackEncoder = new MsgPackEncoder({EnablePacketTable: false, EnableStreamTable: false});
    #msgpack_dec: MsgPackDecoder = new MsgPackDecoder();

    #decode(value: any): any {
        if(!value)
            return null;
        const buffer = Buffer.from(value, 'base64');
        return this.#msgpack_dec.decodeStream(buffer);
    }

    #encode(msg: any): any {
        return Buffer.from(this.#msgpack_enc.encodeStream(msg)).toString('base64');
    }

    
    async #logError(msg: string, severity: string): Promise<void> {
        if(severity !== 'debug')
            await this.#redis.lPush('ERRORS', this.#encode({msg, context: Context.getCallContext()}));
    }

    async #sendResponse(msg: IPCCall, result: any, error: any): Promise<void> {
        if(error !== null && !(error instanceof IpcError)) {
            error = new IpcError(error.toString(), 'IPC:UNKNOWN_ERROR_RESPONSE', 'unknown'); // TODO: log it!
        }
        const res: IPCReply = {
            sequence: msg.sequence,
            result: this.#encode(result),
            error: this.#encode(error)
        }
        await this.#redis.xAdd(msg.replyTo, '*', <any>res, { TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: this.#options.streamLimit}});
    }

    async #handleQueue(queue: string, m: IIpcMethodOptions): Promise<void> {
        const concurrencyGroup = m.concurencyGroup ?? m.methodName;
        if(this.#queuedRequests[concurrencyGroup]?.length > 0) {
            const nextReq = this.#queuedRequests[concurrencyGroup].shift();
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            let msg = nextReq!.msg;
            this.#numTotalQueuedRequests--;
            let method = this.#cmdinfo[queue + '.' + msg.method];
            if(method === undefined)
                method = this.#cmdinfo[queue + '.*'];
            if(method === undefined) {
                // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                await this.#redis.xAck(nextReq!.queue, this.#xgroup, nextReq!.ticketID);
                await this.#sendResponse(msg, null, new IpcError('Command not found', 'IPC:CMD_NOT_FOUND', 'critical'));            
                return this.#handleQueue(queue, m);
            }
            if(this.#numTotalQueuedRequests < this.#options.maxQueueResume && this.#disableNewTicketAccept) {
                this.#disableNewTickets = null;
                this.#disableNewTicketAccept();
                this.#disableNewTicketAccept = null;
                if(this.onResumeProcessing)
                    await this.onResumeProcessing();
            }
            if(msg.params === undefined) {
                // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                const ticket = await this.#redis.xRange(nextReq!.queue, nextReq!.ticketID, nextReq!.ticketID);
                msg = <any>ticket[0].message;

            }
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            return this.#processTicket(nextReq!.queue, nextReq!.ticketID, msg, method);
        }
        //
    }

    async #handleTicket(queue: string, ticketID: string, msg: IPCCall): Promise<void> {
        let method = this.#cmdinfo[queue + '.' + msg.method];
        if(method === undefined)
            method = this.#cmdinfo[queue + '.*'];
        if(method === undefined) {
            await this.#redis.xAck(queue, this.#xgroup, ticketID);
            return this.#sendResponse(msg, null, new IpcError('Command not found', 'IPC:CMD_NOT_FOUND', 'critical'));
        }
        const concurrencyGroup = method.concurencyGroup ?? method.methodName;
        const concurrencyLimit = (method.concurencyGroup ? this.#options.concurencyGroupLimits[concurrencyGroup] : method.maxConcurrent) ?? 0;  
        if(this.#activeRequests[concurrencyGroup]?.size >= concurrencyLimit) {
            if(!this.#queuedRequests[concurrencyGroup])
                this.#queuedRequests[concurrencyGroup] = [];
            if(method.prefetchRequestLimit !== undefined && method.prefetchRequestLimit < (msg.params?.length ?? 0)) {
                delete msg.params;
            }
            this.#queuedRequests[concurrencyGroup].push({msg, queue, ticketID});
            this.#numTotalQueuedRequests++;
            if(this.#numTotalQueuedRequests >= this.#options.maxQueueLimit && !this.#disableNewTicketAccept) {
                this.#disableNewTickets = new Promise((accept) => this.#disableNewTicketAccept = accept);
                if(this.onStopProcessing)
                    await this.onStopProcessing();
            }
        } else {
            return this.#processTicket(queue, ticketID, msg, method);
        }
    }

    async #processTicket(queue: string, ticketID: string, msg: IPCCall, method: IIpcMethodOptions): Promise<void> {
        if((+ticketID.split('-')[0] + (method.waitTimeout ?? 10000)) < + new Date()) {
            setImmediate(():any => this.#handleQueue(queue, method).catch(() => 0));
            await this.#redis.xAck(queue, this.#xgroup, ticketID);
            return this.#sendResponse(msg, null, new IpcError('Command waitTimeout exceeded', 'IPC:TIMEOUT_WAIT_EXCEEDED', 'critical'));
        }
        if(method.ackType === IPCAckType.begin || method.ackType === undefined)
            await this.#redis.xAck(queue, this.#xgroup, ticketID);

        const context = {
            ...msg.context
        };
        let isAck = false;
        if(method.ackType === IPCAckType.manual) {
            context.ack = async ():Promise<void> => {
                isAck = true;
                await this.#redis.xAck(queue, this.#xgroup, ticketID);
            }
        }
        const concurrencyGroup = method.concurencyGroup ?? method.methodName;
        const concurrencyLimit = (method.concurencyGroup ? this.#options.concurencyGroupLimits[concurrencyGroup] : method.maxConcurrent) ?? 0;
        if(!this.#activeRequests[concurrencyGroup] && concurrencyLimit > 0)
            this.#activeRequests[concurrencyGroup] = new Set();
            
        const pr = Context.enterScope(this.#decode(msg.context), method.cb, this.#decode(msg.params)).then(async (res) => {
            if(method.ackType === IPCAckType.end)
                await this.#redis.xAck(queue, this.#xgroup, ticketID);   
            if(method.ackType === IPCAckType.manual && !isAck) 
                throw new IpcError('Unacknowleged ticket', 'IPC:TICKET_UNACK', 'critical');
            if(method.type === IPCCallType.invoke)
                return this.#sendResponse(msg, res, null);
        }).catch(async (err) => {            
            if(method.ackType === IPCAckType.end || (method.ackType === IPCAckType.manual && !isAck))
                await this.#redis.xAck(queue, this.#xgroup, ticketID);               
            if(method.type === IPCCallType.invoke)
                return this.#sendResponse(msg, null, err);
            else
                return this.#logError('async call failed: ' + err.toString(), 'info');             
        }).then(() => {
            setImmediate(():any => this.#handleQueue(queue, method).catch(() => 0));
            if(concurrencyLimit > 0)
                this.#activeRequests[concurrencyGroup].delete(pr);
        }).catch(() => 0);
        if(concurrencyLimit > 0)
            this.#activeRequests[concurrencyGroup].add(pr);
    }

    async on(cmd: IIpcMethodOptions): Promise<void> {
        await this.#init;
        const queue = (cmd.type == IPCCallType.broadcast ? 'BROADCAST/' : 'STREAM/') + (cmd.queue ?? this.#options.workerName);
        this.#cmdinfo[queue + '.' + cmd.methodName] = cmd;
        if(cmd.concurencyGroup && cmd.maxConcurrent)
            throw new IpcError('Method ' + cmd.methodName + ' has maxConcurrect but is part of a concurrecyGroup', 'IPC:MAX_CONCURRENT_CONFLICT', 'critical');
        await this.#redis.hSet(this.#options.commandHash, this.#options.workerName + '.' + cmd.methodName, this.#encode(cmd));

    }

    clearCache(): void {
        this.#methodCache = {};
    }

    async getMethodInfo(worker: string, method: string): Promise<IIpcMethodOptions> {
        let cmd:IIpcMethodOptions|null = null; 
        if(this.#options.optimize === IPCOptimize.speed) {
            cmd = this.#methodCache[worker + '.' + method];
        } else {
            if(!await this.#redis.hGet(this.#options.keepAliveHash, worker))
                throw new IpcError('Target worker not running', 'IPC:WORKER_NOT_RUNNING', 'critical');
        }
        if(!cmd) {
            const cmds = await this.#redis.hmGet(this.#options.commandHash, [worker + '.' + method, worker + '.*']);
            if(!(cmds[0] ?? cmds[1]))
                throw new IpcError('Command not found', 'IPC:CMD_NOT_FOUND', 'critical');   
            cmd = this.#decode(cmds[0] ?? cmds[1]);    
            if(this.#options.optimize === IPCOptimize.speed) {
                // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                this.#methodCache[worker + '.' + method] = cmd!;
            }
        }
        return cmd!;
    }

    async call(queue: string, worker: string, method: string, params: any[], type: 'invoke' | 'event' | 'broadcast'): Promise<any> {
        /* eslint-disable @typescript-eslint/no-misused-promises */
        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (accept, reject) => {
            let cmd:IIpcMethodOptions|null = null;
            try {
                cmd = await this.getMethodInfo(worker, method);
            } catch (e) {
                return reject(e);
            }
            const sequence = (this.#sequence++).toString();
            const replyTo = this.#replystream.key;
            const context = this.#encode(Context.getCallContext());
            const call: IPCCall = {
                method, params: <any>this.#encode(params), sequence, replyTo, context
            };
            const targetQueue = (type !== 'broadcast') ? 'STREAM/' + queue : 'BROADCAST/' + queue;
            if(type === 'invoke') {
                const timer = setTimeout(() => {
                    const req = this.#activeCalls[sequence];
                    delete this.#activeCalls[sequence];
                    clearTimeout(req.timer);
                    req.reject(new IpcError('Command runTimeout exceeded', 'IPC:TIMEOUT_RUN_EXCEEDED', 'critical'));
                }, (cmd?.waitTimeout ?? 10000) + (cmd?.runTimeout ?? 10000));
                this.#activeCalls[sequence] = { accept, reject, timer };
            }
            const id = await this.#redis.xAdd(targetQueue, '*', <any>call, {TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: this.#options.streamLimit}});
            if(type !== 'invoke') 
                accept(id);

        });
        /* eslint-enable @typescript-eslint/no-misused-promises */
    }

    async #handleReply(msg: IPCReply): Promise<void> {
        const req = this.#activeCalls[msg.sequence]
        if(!req)            
            return this.#logError('request ' + msg.sequence + ' not found in reply queue', 'debug');
        try {
            clearTimeout(req.timer);
            delete this.#activeCalls[msg.sequence];
            msg.error = this.#decode(msg.error);
            msg.result = this.#decode(msg.result);
            if(!msg.error) {
                req.accept(msg.result);
            } else {
                req.reject(msg.error);
            }
        } catch (e) {
            return this.#logError('parsing reply failed for seq ' + msg.sequence, 'critical');
        }

    }

    async #resume_ticket(client: IpcRedisClient): Promise<void> {
        const streams = this.#xstreams.map((stream) => <any>{ key: stream.key, id: 0 });
        // eslint-disable-next-line no-constant-condition
        while(true) {
            await this.#disableNewTickets;
            const ticket = await client.xReadGroup(this.#xgroup, this.#xconsumer, this.#xstreams, { COUNT: 1 });
            if(!ticket || !ticket[0])
                break;
            const si = streams.findIndex(x => ticket[0].name === x.key);
            streams[si].id = ticket[0].messages[0].id;
            await this.#handleTicket(ticket[0].name, ticket[0].messages[0].id, <IPCCall><unknown>ticket[0].messages[0].message);
        }
    }

    async #run_ticket(client: IpcRedisClient): Promise<void> {
        await client.connect();
        await this.#resume_ticket(client);
        while(this.#status === IPCState.running) {
            await this.#disableNewTickets;
            const ticket = await client.xReadGroup(this.#xgroup, this.#xconsumer, this.#xstreams, { BLOCK: this.#options.blockTimeout * 1000, COUNT: 1});
            if(!ticket || !ticket[0])
                continue;
            await this.#handleTicket(ticket[0].name, ticket[0].messages[0].id, <IPCCall><unknown>ticket[0].messages[0].message);
        }
        await client.quit();
    }

    async #run_broadcast(client: IpcRedisClient): Promise<void> {
        await client.connect();
        while(this.#status === IPCState.running) {
            await this.#disableNewTickets;
            const msg = await client.xRead(this.#xbcstreams, {BLOCK: this.#options.blockTimeout * 1000, COUNT: 1});
            if(!msg || !msg[0])
                continue;
            const stream = this.#xbcstreams.findIndex(x => msg[0].name === x.key);
            if(stream === -1)
                throw new Error('IPCWorker: FATAL ERROR - received invalid stream name'); //this (theoretically) cannot happen anyway
            this.#xbcstreams[stream].id = msg[0].messages[0].id;
            await this.#handleTicket(msg[0].name, msg[0].messages[0].id, <IPCCall><unknown>msg[0].messages[0].message);
        }
        await client.quit();
    }

    async #run_reply(client: IpcRedisClient): Promise<void> {
        await client.connect();
        while(this.#status !== IPCState.cleanup) {
            const msg = await client.xRead([this.#replystream], {BLOCK: this.#options.blockTimeout * 1000, COUNT: 1});
            if(!msg || !msg[0])
                continue;
            this.#replystream.id = msg[0].messages[0].id;
            await this.#handleReply(<IPCReply><unknown>msg[0].messages[0].message);
        }
        await client.quit();
    }

    async run(): Promise<void> {
        this.#runState = this.#initState.then(async () => this.#run());
    }

    async #run(): Promise<void> {
        const ticket = this.#run_ticket(this.#redis.duplicate())
        const broadcast = this.#run_broadcast(this.#redis.duplicate())
        await Promise.all([ticket, broadcast, this.#reply_thread]);
        await this.#cleanup();
    }

    async #keepalive(): Promise<void> {
        await this.#redis.sendCommand(['EXPIREMEMBER', this.#options.keepAliveHash, this.#options.workerName, (this.#options.pingInterval * 2).toString()]);
        await this.#redis.sendCommand(['EXPIREMEMBER', this.#options.keepAliveInstanceHash, this.#options.workerName + '/' + this.#options.instanceName, (this.#options.pingInterval * 2).toString()]);
    }

    async #cleanup(): Promise<void> {
        clearInterval(this.#aliveTimer);
        await this.#redis.hDel(this.#options.keepAliveInstanceHash, this.#options.workerName + '/' + this.#options.instanceName);
    }

    async shutdown(): Promise<void> {
        if(this.onShutdown)
            await this.onShutdown();
        this.#status = IPCState.cleanup; //TODO: shutingDown state
        await this.#runState;
        await this.#redis.hDel(this.#options.keepAliveInstanceHash, this.#options.workerName + '/' + this.#options.instanceName);
        console.log('shutdown completed');
    }
}
