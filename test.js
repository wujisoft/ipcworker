const usleep = (time) => new Promise((accept) => setTimeout(accept, time));

let prAcc;
let pr = new Promise((accept) => { prAcc = accept });

async function test1() {
    usleep(500);
    await pr;
    console.log('test1 returned');
}

async function test2() {
    usleep(500);
    await pr;
    console.log('test2 returned');
}

async function runTests() {
    test1();
    test2();
    await usleep(1000)
    prAcc();
    pr = null;
    prAcc = null;
    console.log('test completed');
}

runTests();