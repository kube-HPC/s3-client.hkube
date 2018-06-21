const { expect } = require('chai');
const S3Client = require('../index');
const uniqid = require('uuid');
const fs = require('fs');
const stream = require('stream');

const mock = {
    mocha: '^5.0.0',
    chai: '^4.1.2',
    coveralls: '^3.0.0',
    eslint: '^4.15.0',
    istanbul: '^1.1.0-alpha.1',
    sinon: '^4.1.3'
};
const createJobId = () => uniqid() + '-ab-cd-ef';
describe('s3-client', () => {
    before((done) => {
        const options = {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000'
            // accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'agambstorage',
            // secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '234eqndbpuCkGtH85KSyK/xAv3xuqdOpM3fKOLYlrSerpdKoG1FYy3kh6ArceL+yDwTvQOgs47xYO/ktnNzEeg==',
            // endpoint: process.env.AWS_ENDPOINT || 'http://10.32.10.24:9000'
        };
        S3Client.init(options);
        done();
    });
    describe('put', () => {
        it('should throw error on invalid bucket name (empty)', (done) => {
            S3Client.createBucket({ Bucket: '  ' }).catch((error) => {
                expect(error).to.be.an('error');
                done();
            });
        });
        it('should throw error on invalid bucket name (not string)', (done) => {
            S3Client.createBucket({ Bucket: 3424 }).catch((error) => {
                expect(error).to.be.an('error');
                done();
            });
        });
        it('should throw error on invalid bucket name (null)', (done) => {
            S3Client.createBucket({ Bucket: null }).catch((error) => {
                expect(error).to.be.an('error');
                done();
            });
        });
        it('should throw exception if bucket not exists', (done) => {
            S3Client.put({ Bucket: createJobId(), Key: createJobId(), Body: mock }).catch((error) => {
                expect(error).to.be.an('error');
                done();
            });
        });
        it('put string as data', async () => {
            const Bucket = 'yello';
            const Key = 'yellow:yellow-algorithms:' + createJobId();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: 'str' });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.equal('str');
        });
        it('put number as data', async () => {
            const Bucket = 'green';
            const Key = 'green:green-algorithms2:' + createJobId();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: 123456 });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.equal(123456);
        });
        it('put object as data', async () => {
            const Bucket = 'red';
            const Key = 'red:red-algorithms2:' + createJobId();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: mock });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.deep.equal(mock);
        });
        it('put array as data', async () => {
            const Bucket = 'black';
            const Key = 'black:black-algorithms2:' + createJobId();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: [1, 2, 3] });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.include(1, 2, 3);

            await S3Client.put({ Bucket, Key, Body: [mock, mock] });
            const result1 = await S3Client.get({ Bucket, Key });
            expect(result1).to.deep.include(mock, mock);
        });
        it('add multiple objects to the same bucket', async () => {
            const Bucket = createJobId();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: createJobId(), Body: mock });
            await S3Client.put({ Bucket, Key: createJobId(), Body: mock });
            await S3Client.put({ Bucket, Key: createJobId(), Body: mock });
            await S3Client.put({ Bucket, Key: createJobId(), Body: mock });
            await S3Client.put({ Bucket, Key: createJobId(), Body: mock });
        });
        it('put-stream', async () => {
            const Bucket = createJobId();
            await S3Client.createBucket({ Bucket });
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: createJobId(), Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: createJobId(), Body: readStream2 });
        });
        it('override', async () => {
            const Bucket = createJobId();
            const objectId = createJobId();
            await S3Client.createBucket({ Bucket });
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: objectId, Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: objectId, Body: readStream2 });
        });
    });
    describe('get', () => {
        it('should get job result', async () => {
            const Bucket = createJobId();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: 'result.json', Body: { data: 'test1' } });
            expect(await S3Client.get({ Bucket, Key: 'result.json' })).to.deep.equal({ data: 'test1' });
        });

        it('should get object by bucket & objectId', async () => {
            const Bucket = createJobId();
            const objectId1 = createJobId();
            const objectId2 = createJobId();
            const objectId3 = createJobId();
            const objectId4 = createJobId();

            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: objectId1, Body: { data: 'test1' } });
            await S3Client.put({ Bucket, Key: objectId2, Body: { data: 'test2' } });
            await S3Client.put({ Bucket, Key: objectId3, Body: { data: 'test3' } });
            await S3Client.put({ Bucket, Key: objectId4, Body: { data: 'test4' } });

            expect(await S3Client.get({ Bucket, Key: objectId1 })).to.deep.equal({ data: 'test1' });
            expect(await S3Client.get({ Bucket, Key: objectId2 })).to.deep.equal({ data: 'test2' });
            expect(await S3Client.get({ Bucket, Key: objectId3 })).to.deep.equal({ data: 'test3' });
            expect(await S3Client.get({ Bucket, Key: objectId4 })).to.deep.equal({ data: 'test4' });
        });
        it('should failed if bucket not exists', (done) => {
            S3Client.get({ Bucket: createJobId(), Key: createJobId() }).catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchBucket');
                done();
            });
        });
        it('should failed if objectId not exists', (done) => {
            const Bucket = createJobId();
            const resolvingPromise = new Promise((resolve, reject) => {
                S3Client.createBucket({ Bucket }).then(() => {
                    S3Client.put({ Bucket, Key: createJobId(), Body: mock }).then(() => {
                        S3Client.get({ Bucket, Key: createJobId() }).catch(err => reject(err));
                    });
                });
            });

            resolvingPromise.catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchKey');
                done();
            });
        });
        it('get-stream file', async () => {
            const Bucket = createJobId();
            const Key = createJobId();
            await S3Client.createBucket({ Bucket });

            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key, Body: readStream });
            const res = await S3Client.getStream({ Bucket, Key });
            await new Promise((resolve, reject) => {
                res.pipe(fs.createWriteStream('tests/dest.txt'))
                    .on('error', (err) => {
                        reject(err);
                    }).on('finish', () => {
                        resolve();
                    });
            });
        }).timeout(1000000);
        it('get-stream', async () => {
            const Bucket = createJobId();
            const Key = createJobId();
            await S3Client.createBucket({ Bucket });

            const streamObject = new stream.Readable();
            const array = [];
            for (let i = 0; i < 1000000; i += 1) {
                array.push(mock);
            }
            const bigObject = { arr: array };
            streamObject.push(JSON.stringify(bigObject));
            streamObject.push(null);

            await S3Client.put({ Bucket, Key, Body: streamObject });
            const res = await S3Client.get({ Bucket, Key });
            expect(res).to.deep.equal(bigObject);
        }).timeout(1000000);
    });
    describe('bucket name validations', () => {
        it('Bucket name is longer than 63 characters', (done) => {
            const Bucket = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:' + createJobId();
            S3Client.createBucket({ Bucket }).catch((error) => {
                expect(error).to.be.an('error');
                done();
            });
        });
    });
    describe('listObjects', () => {
        it('get 10 keys', async () => {
            const Bucket = 'test-list-keys';
            await S3Client.createBucket({ Bucket });
            const promiseArray = [];
            for (let i = 0; i < 10; i += 1) {
                promiseArray.push(S3Client.put({ Bucket, Key: `test1/test-${i}/xxx.json`, Body: { value: 'str' } }));
            }
            await Promise.all(promiseArray);
            const objects = await S3Client.listObjects({ Bucket, Prefix: 'test1' });
            expect(objects.length).to.equal(10);
        });
        it('get 10 objects', async () => {
            const Bucket = 'test-list-objects';
            await S3Client.createBucket({ Bucket });
            const promiseArray = [];
            for (let i = 0; i < 10; i += 1) {
                promiseArray.push(S3Client.put({ Bucket, Key: `test1/test/xxx-${i}.json`, Body: { value: 'str' } }));
            }
            await Promise.all(promiseArray);
            const objects = await S3Client.listObjects({ Bucket, Prefix: 'test1/test' });
            expect(objects.length).to.equal(10);
        });
        it('get more than 1000 keys', async () => {
            const Bucket = 'test-1000-keys';
            await S3Client.createBucket({ Bucket });
            const promiseArray = [];
            for (let i = 0; i < 1500; i += 1) {
                promiseArray.push(S3Client.put({ Bucket, Key: `test1/test-${i}/xxx.json`, Body: { value: 'str' } }));
            }
            await Promise.all(promiseArray);
            const objects = await S3Client.listObjects({ Bucket, Prefix: 'test1' });
            expect(objects.length).to.equal(1500);
        }).timeout(10000);
        it('get more than 1000 objects', async () => {
            const Bucket = 'test-1000-objects';
            await S3Client.createBucket({ Bucket });
            const promiseArray = [];
            for (let i = 0; i < 1500; i += 1) {
                promiseArray.push(S3Client.put({ Bucket, Key: `test1/test/xxx-${i}.json`, Body: { value: 'str' } }));
            }
            await Promise.all(promiseArray);
            const objects = await S3Client.listObjects({ Bucket, Prefix: 'test1/test' });
            expect(objects.length).to.equal(1500);
        }).timeout(10000);
    });
    describe('delete', () => {
        it('get 10 objects', async () => {
            const Bucket = 'delete-objects';
            await S3Client.createBucket({ Bucket });
            const promiseArray = [];
            for (let i = 0; i < 10; i += 1) {
                promiseArray.push(S3Client.put({ Bucket, Key: `test1/test/xxx-${i}.json`, Body: { value: 'str' } }));
            }
            await Promise.all(promiseArray);
            const objectsToDelete = await S3Client.listObjects({ Bucket, Prefix: 'test1/test' });
            const res = await S3Client.deleteObjects({ Bucket, Delete: { Objects: objectsToDelete } });
            expect(res.Deleted.length).to.equal(10);
        });
        it('get 10 keys', async () => {
            const Bucket = 'delete-keys';
            await S3Client.createBucket({ Bucket });
            const promiseArray = [];
            for (let i = 0; i < 10; i += 1) {
                promiseArray.push(S3Client.put({ Bucket, Key: `test1/test-${i}/xxx.json`, Body: { value: 'str' } }));
            }
            await Promise.all(promiseArray);

            const objectsToDelete = await S3Client.listObjects({ Bucket, Prefix: 'test1' });
            const res = await S3Client.deleteObjects({ Bucket, Delete: { Objects: objectsToDelete } });
            expect(res.Deleted.length).to.equal(10);
        });
    });
});
