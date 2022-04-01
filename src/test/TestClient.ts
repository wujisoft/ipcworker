
import { Context, IPC, IpcWorker, IpcWorkerConfig } from '..';
import { createClient } from 'redis';
import { IRPC } from './IRPC';


@IpcWorkerConfig()
export class TestClient extends IpcWorker<IRPC> {
    async onInit() {
        await Context.enterScope({test: true}, async () => {
            const r = [];
            let count = 1;
            for(let i = 0; i < 30; i++) {
                //r.push(this.rpc.broadcast.TestWorker.TestCall('Test1', count++).then(x => console.log(x)).catch(x => console.error(x)))
                r.push((<any>this.rpc.event.TestWorker).SomeStupidCatchall(count++ + 'blablablabla').then((x:any) => console.log(x)).catch((x:any) => console.error(x)))
            }
            await Promise.all(r);
        }, []);
    }
}
