
import { Context, IPCCallType, IpcMethod, IpcWorker, IpcWorkerConfig } from '..';
import { ITestWorker } from './interfaces/ITestWorker';
import { IRPC } from './IRPC';

const usleep = async (time: number): Promise<void> => new Promise(accept => setTimeout(accept, time));

@IpcWorkerConfig({

})
export class TestWorker extends IpcWorker<IRPC> implements ITestWorker{
    @IpcMethod()
    async onInit(): Promise<any> {
        //console.log("RPC:", this.rpc);
    }

    @IpcMethod({maxConcurrent: 10, type: IPCCallType.broadcast})
    async TestCall(p1: string,p2: number): Promise<string> {
        console.log(p1, p2, Context.ALS.getStore());

       /* if(p2 > 3)
            await usleep(30000);*/

        await usleep(500);
        return "hallo:" + p2;
    }

    @IpcMethod({ methodName: '*', maxConcurrent: 1, waitTimeout: 60000, prefetchRequestLimit: 10})
    async CatchAll(p3: number): Promise<number> {
        console.log("bla:", p3);
        await usleep(500);
        return 7;
    }
}