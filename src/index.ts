import redis from 'redis';


export type IpcRedisClient = ReturnType<typeof redis.createClient>;
export type IpcRedisOptions = Parameters<typeof redis.createClient>[0];

export * from './files/rpc';
export * from './files/ipc';
export * from './files/context';
export * from './files/worker';