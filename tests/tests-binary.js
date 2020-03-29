const { expect } = require('chai');
const S3Client = require('../index');
const uniqid = require('uuid');
const fs = require('fs');

const { Encoding } = require('@hkube/encoding');
const encoding = new Encoding({ type: 'bson' });

const mock = {
    mocha: '^5.0.0',
    chai: '^4.1.2',
    coveralls: '^3.0.0',
    eslint: '^4.15.0',
    istanbul: '^1.1.0-alpha.1',
    sinon: '^4.1.3'
};
const createJobId = () => uniqid() + '-ab-cd-ef';
const options = {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000'
};
const client = new S3Client();

describe('s3-client-binary', () => {
    before(() => {
        client.init(options);

        const wrapperGet = (fn) => {
            const wrapper = async (args) => {
                const result = await fn(args);
                return encoding.decode(result);
            };
            return wrapper;
        };

        const wrapperPut = (fn) => {
            const wrapper = (args) => {
                const Body = encoding.encode(args.Body);
                return fn({ ...args, Body });
            };
            return wrapper;
        };

        client.put = wrapperPut(client.put.bind(client));
        client.get = wrapperGet(client.get.bind(client));
    });
    describe('put', () => {
        it('put string as data binary', async () => {
            const Bucket = 'yello';
            const Key = 'yellow:yellow-algorithms:' + createJobId();
            await client.createBucket({ Bucket });
            await client.put({ Bucket, Key, Body: 'str' });
            const result = await client.get({ Bucket, Key });
            expect(result).to.equal('str');
        });
        it('put null as data binary', async () => {
            const Bucket = 'yello';
            const Key = 'yellow:yellow-algorithms:' + createJobId();
            await client.createBucket({ Bucket });
            await client.put({ Bucket, Key, Body: null });
            const result = await client.get({ Bucket, Key });
            expect(result).to.not.exist;
        });
        it('put number as data binary', async () => {
            const Bucket = 'green';
            const Key = 'green:green-algorithms2:' + createJobId();
            await client.createBucket({ Bucket });
            await client.put({ Bucket, Key, Body: 123456 });
            const result = await client.get({ Bucket, Key });
            expect(result).to.equal(123456);
        });
        it('put object as data binary', async () => {
            const Bucket = 'red';
            const Key = 'red:red-algorithms2:' + createJobId();
            await client.createBucket({ Bucket });
            await client.put({ Bucket, Key, Body: mock });
            const result = await client.get({ Bucket, Key });
            expect(result).to.deep.equal(mock);
        });
        it('put object 2 as data binary', async () => {
            const Bucket = 'red';
            const Key = 'red:red-algorithms2:' + createJobId();
            await client.createBucket({ Bucket });
            const data = { command: 'foo', data: Buffer.alloc(10, 0x33) };
            await client.put({ Bucket, Key, Body: data });
            const result = await client.get({ Bucket, Key });
            expect(result).to.deep.equal(data);
        });
        it('put large object as data binary', async () => {
            const Bucket = 'red';
            const Key = 'red:red-algorithms2:' + createJobId();
            await client.createBucket({ Bucket });
            const data = { command: 'foo', data: Buffer.alloc(50 * 1024 * 1024, 0x33), another: 'key' };
            await client.put({ Bucket, Key, Body: data });
            const result = await client.get({ Bucket, Key });
            expect(result).to.deep.equal(data);
        });
        it('put array as data binary', async () => {
            const Bucket = 'black';
            const Key = 'black:black-algorithms2:' + createJobId();
            await client.createBucket({ Bucket });
            await client.put({ Bucket, Key, Body: [1, 2, 3] });
            const result = await client.get({ Bucket, Key });
            expect(result).to.include(1, 2, 3);

            await client.put({ Bucket, Key, Body: [mock, mock] });
            const result1 = await client.get({ Bucket, Key });
            expect(result1).to.deep.include(mock, mock);
        });
        it('put-stream binary', async () => {
            const Bucket = createJobId();
            await client.createBucket({ Bucket });
            const readStream = fs.createReadStream('tests/big-file.txt');
            await client.put({ Bucket, Key: createJobId(), Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await client.put({ Bucket, Key: createJobId(), Body: readStream2 });
        });
    });
});
