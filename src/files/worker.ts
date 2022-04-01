import { IpcRedisOptions, IIpcMethodOptions, IPC, IPCOptions, IPCCallType } from "..";
import process from 'process';
import yargs from 'yargs';
import { createClient } from 'redis';
import { RPC } from "./rpc";
import ON_DEATH from 'death';
import * as acorn from "acorn";

type Optional<T, K extends keyof T> = Pick<Partial<T>, K> & Omit<T, K>;

export type RpcMethodOptions = Optional<Omit<Optional<IIpcMethodOptions, 'type'|'methodName'>, 'cb'>, 'argOrder'>;

export class IpcWorker<R = never> {
    static __ipcinfo: { [method: string]: Optional<IIpcMethodOptions, 'cb'> } = {};
    static __rpc: any;

    rpc!: R;

    #__ipc?: IPC;

    onInit?(): Promise<void>;
    onStopProcessing?(): Promise<void>;
    onResumeProcessing?(): Promise<void>;
    onShutdown?(): Promise<void>;

    isDebug: boolean = false;

    static async __init<S, T extends IpcWorker<S>>(this: new() => T, instanceName: string = this.name, options: IRpcWorkerConfig, debug: boolean = false): Promise<T> {
        const redis = createClient(options.redisConnectionInfo);
        await redis.connect();
        const that = new this(); 
        that.isDebug = debug;
        const opts: IPCOptions = {
            workerName: this.name,
            instanceName,
            ...options
        }
        const ipc = new IPC(redis, opts);
        that.#__ipc = ipc;
        await ipc.$init;
        await Promise.all(
            Object.entries((<typeof IpcWorker><unknown>this).__ipcinfo).map(async ([method, info]) => {
                await ipc.on({...info, ...{ methodName: info.methodName ?? method, cb: (...args:any[]):any => (<any>that)[method](...args)}});
            })
        );
        if(that.onResumeProcessing)
            ipc.onResumeProcessing = async ():Promise<void> => (<any>that).onResumeProcessing();
        if(that.onStopProcessing)
            ipc.onStopProcessing = async ():Promise<void> => (<any>that).onStopProcessing();
        if(that.onShutdown)
            ipc.onShutdown = async(): Promise<void> => (<any>that).onShutdown();
        ON_DEATH(async (signal: any) => {
            console.log("process exit requested by signal ", signal);
            await ipc.shutdown().then(async () => redis.quit()).catch((e) => console.error(e));
        });
        that.rpc = <S><unknown>new RPC(ipc);
        if(that.onInit)
            await that.onInit();
        await ipc.run();            
        console.log('STARTING ' + this.name + ' / ' + instanceName);
        // init stuff
        return new this();
    }
}

export interface IRpcWorkerConfig {
    redisConnectionInfo?: IpcRedisOptions;
}

export function IpcMethod(options: RpcMethodOptions = {}) {
    return <S, T extends IpcWorker<S>>(target: T, propertyKey: string): void => {
        if(!options.type) 
            options.type = IPCCallType.invoke;
        const args = 
            (<any>acorn.parse('class XY { ' + (<any>target)[propertyKey].toString() + ' }', { ecmaVersion: 2020 }))
                .body[0].body.body[0].value.params
                .map((e:any) => <any>{
                    name: e.type == 'AssignmentPattern' ? e.left.name : e.name,
                    hasDefault: e.type == 'AssignmentPattern'
                });
        options.argOrder = args;
        
        (<typeof IpcWorker>target.constructor).__ipcinfo[propertyKey] = <any>options;
    }
}

export function IpcWorkerConfig(options: IRpcWorkerConfig & Omit<IPCOptions, 'workerName'|'instanceName'> = {}) {
    return <S, T extends { new(): IpcWorker<S> }>(constructor: T): T => {
        process.on('message', <any>(async (msg: any): Promise<void> => {
            if(typeof msg === 'object' && msg.cmd === 'start') {
                (<any>constructor).__init(msg.instance, options).catch((x:any) => console.error(x));
            }
        }));
        if(yargs.argv.debug) {
            setImmediate(() => {
                (<any>constructor).__init(typeof yargs.argv.instance === 'string' ? yargs.argv.instance : undefined, options, true).catch((x:any) => console.error("Unhandeled error: ", x));
            });
        }
        return constructor;
    }
}