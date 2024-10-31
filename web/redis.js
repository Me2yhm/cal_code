const Redis = require("ioredis");
const fs = require('fs');
const csv = require('csv-parser');
// import Redis from 'ioredis'
const rl = require('readline').createInterface({
    input: require('fs').createReadStream('../Dump_20241023.csv'),
});
const redis = new Redis(6379)

// value = redis.callBuffer('DUMP', 'keyl')
// value.then(buffer => {
//     data = buffer.toString('hex');
//     content = Buffer.from(data, 'hex');
//     console.log(data);
//     console.log(content);
// }).catch(err => {
//     console.error(err);
// });
rl.on('line', (line) => {
    let [key, content, ttl] = line.split(',');
    key = Buffer.from(key, 'hex').toString();
    content = Buffer.from(content, 'hex');
    redis.callBuffer('RESTORE', key, ttl, content);

})