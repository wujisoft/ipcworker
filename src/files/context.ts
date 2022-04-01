import { AsyncLocalStorage } from "async_hooks";

export class Context {
    static ALS: AsyncLocalStorage<unknown>;
    static {
        Context.ALS = new AsyncLocalStorage();
    }

    static async enterScope(context: any, cb: (...args:any[]) => Promise<any>, data: any[]): Promise<any> {
        return Context.ALS.run(context, async () => cb(...data));
    }

    static getCallContext(): any {
        return Context.ALS.getStore() ?? {};
    }
}