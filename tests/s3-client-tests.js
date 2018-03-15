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
            S3Client.put({ Bucket: uniqid(), Key: uniqid(), Body: mock }).catch((error) => {
                expect(error).to.be.an('error');
                done();
            });
        });
        it('put string as data', async () => {
            const Bucket = 'yellow:' + uniqid();
            const Key = 'yellow:yellow-algorithms:' + uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: 'str' });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.equal('str');
        });
        it('put number as data', async () => {
            const Bucket = 'green:' + uniqid();
            const Key = 'green:green-algorithms2:' + uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: 123456 });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.equal(123456);
        });
        it('put object as data', async () => {
            const Bucket = 'red:' + uniqid();
            const Key = 'red:red-algorithms2:' + uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: mock });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.deep.equal(mock);
        });
        it('put array as data', async () => {
            const Bucket = 'black:' + uniqid();
            const Key = 'black:black-algorithms2:' + uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key, Body: [1, 2, 3] });
            const result = await S3Client.get({ Bucket, Key });
            expect(result).to.include(1, 2, 3);

            await S3Client.put({ Bucket, Key, Body: [mock, mock] });
            const result1 = await S3Client.get({ Bucket, Key });
            expect(result1).to.deep.include(mock, mock);
        });
        it('add multiple objects to the same bucket', async () => {
            const Bucket = uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
        });
        it('put-stream', async () => {
            const Bucket = uniqid();
            await S3Client.createBucket({ Bucket });
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: uniqid(), Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: uniqid(), Body: readStream2 });
        });
        it('override', async () => {
            const Bucket = uniqid();
            const objectId = uniqid();
            await S3Client.createBucket({ Bucket });
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: objectId, Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await S3Client.put({ Bucket, Key: objectId, Body: readStream2 });
        });
    });
    describe('get', () => {
        it('should get object by bucket & objectId', async () => {
            const Bucket = uniqid();
            const objectId1 = uniqid();
            const objectId2 = uniqid();
            const objectId3 = uniqid();
            const objectId4 = uniqid();

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
            S3Client.get({ Bucket: uniqid(), Key: uniqid() }).catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchBucket');
                done();
            });
        });
        it('should failed if objectId not exists', (done) => {
            const Bucket = uniqid();
            const resolvingPromise = new Promise((resolve, reject) => {
                S3Client.createBucket({ Bucket }).then(() => {
                    S3Client.put({ Bucket, Key: uniqid(), Body: mock }).then(() => {
                        S3Client.get({ Bucket, Key: uniqid() }).catch(err => reject(err));
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
            const Bucket = uniqid();
            const Key = uniqid();
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
            const Bucket = uniqid();
            const Key = uniqid();
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
        it('Bucket name contains invalid characters :', async () => {
            const Bucket = 'tal:' + uniqid() + 'tal:tal';
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
        });
        it('Bucket name is longer than 63 characters', async () => {
            const Bucket = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:' + uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
        });
        it('Bucket name must start with a letter or number.', async () => {
            const Bucket = '-dog:' + uniqid();
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });

            const bucketName2 = '*dog:' + uniqid();
            await S3Client.createBucket({ Bucket: bucketName2 });
            await S3Client.put({ Bucket: bucketName2, Key: uniqid(), Body: mock });

            const bucketName3 = '.dog:' + uniqid();
            await S3Client.createBucket({ Bucket: bucketName3 });
            await S3Client.put({ Bucket: bucketName3, Key: uniqid(), Body: mock });
        });
        it('Bucket name must end with a letter or number.', async () => {
            const Bucket = 'cat:' + uniqid() + '-';
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });

            const bucketName2 = 'cat:' + uniqid() + '.';
            await S3Client.createBucket({ Bucket: bucketName2 });
            await S3Client.put({ Bucket: bucketName2, Key: uniqid(), Body: mock });

            const bucketName3 = 'cat:' + uniqid() + '^';
            await S3Client.createBucket({ Bucket: bucketName3 });
            await S3Client.put({ Bucket: bucketName3, Key: uniqid(), Body: mock });
        });
        it('Bucket name cannot contain uppercase letters.', async () => {
            const Bucket = 'TEST:' + uniqid() + 'TEST-';
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
        });
        it('Bucket name cannot contain consecutive periods (.)', async () => {
            const Bucket = 'Tax..zx:' + uniqid() + '-';
            await S3Client.createBucket({ Bucket });
            await S3Client.put({ Bucket, Key: uniqid(), Body: mock });
        });
    });
});
