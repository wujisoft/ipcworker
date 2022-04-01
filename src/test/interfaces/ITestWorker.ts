
export interface ITestWorker {
    TestCall(p1: string, p2: number): Promise<string>;
}
