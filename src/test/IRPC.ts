import { ITestWorker } from "./interfaces/ITestWorker";

export interface IRPC {
    event: IRPC;
    broadcast: IRPC;
    
    TestWorker: ITestWorker;
}