function consoleLoggerProvider (name) {
    // do something with the name
    return {
        debug: console.log.bind(console),
        info: console.info.bind(console),
        warn: console.warn.bind(console),
        error: console.error.bind(console)
    };
}

const kafkaLogging = require('kafka-node/logging');
//kafkaLogging.setLoggerProvider(consoleLoggerProvider);


const kafka = require('kafka-node');
const async = require('async');

const cg = new kafka.ConsumerGroup({ groupId: 'mem-test-group6', fromOffset: 'earliest', fetchMaxBytes: 10000000}, ['mem-issue-test']);

let count = 0;
const queue = async.queue((message, done) => {
    count++;
    if (count % 1000 === 0) {
        console.log(`${new Date()} processed ${count} messages`);
    }
    setTimeout(done, 1000);
}, 100);

queue.saturated = () => cg.pause();
queue.unsaturated = () => cg.resume();
queue.drain = () => console.log('All messages processed');
queue.buffer = 50;

cg.on('error', console.error);
cg.on('message', message => {
    queue.push(message)
});

console.log(`Starting consumer. This process is pid ${process.pid}`);