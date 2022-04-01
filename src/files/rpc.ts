import { RpcMethodOptions } from "ipcworker";
import { IPC } from "..";

export class RPC {
    constructor(private ipc: IPC) {
        return new Proxy(this, {
            get: (obj: RPC, name: string | symbol):any => obj.getWorker(name, 'invoke')
        })
    }

    getWorker(name: string|symbol, type: 'invoke' | 'event' | 'broadcast'): any {
        if(name === 'event') {
            return new Proxy(this, {
                get: (obj: RPC, name: string | symbol):any => obj.getWorker(name, 'event')
            })
        } else if( name === 'broadcast') {
            return new Proxy(this, {
                get: (obj: RPC, name: string | symbol):any => obj.getWorker(name, 'broadcast')
            })
        }
        return new Proxy(this, {
            get: (obj: RPC, method: string | symbol): any => obj.getCaller(name, method, type)
        })
    }

    getCaller(name: string | symbol, method: string | symbol, type: 'invoke' | 'event' | 'broadcast'): { (...args:any[]): any, info: () => Promise<RpcMethodOptions> } {
        const caller = async (...args:any[]) => {
            return this.ipc.call(name.toString(), name.toString(), method.toString(), args, type);
        }
        caller.info = () => this.ipc.getMethodInfo(name.toString(), method.toString());
        return caller;
    }

}