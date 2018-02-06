const { expect } = require('chai');
const S3Client = require('../index');
const uniqid = require('uuid');
const fs = require('fs');

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
        it('should put new object even if bucket not exists', async () => {
            await S3Client.put({ Bucket: uniqid(), Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('add multiple objects to the same bucket', async () => {
            const bucketName = uniqid();
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('put-stream', async () => {
            const bucketName = uniqid();
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.putStream({ Bucket: bucketName, Key: uniqid(), Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await S3Client.putStream({ Bucket: bucketName, Key: uniqid(), Body: readStream2 });
        }).timeout(5000);
        it('override', async () => {
            const bucketName = uniqid();
            const objectId = uniqid();
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.putStream({ Bucket: bucketName, Key: objectId, Body: readStream });
            const readStream2 = fs.createReadStream('tests/big-file.txt');
            await S3Client.putStream({ Bucket: bucketName, Key: objectId, Body: readStream2 });
        }).timeout(5000);
    });
    describe('get', () => {
        it('should get object by bucket & objectId', async () => {
            const bucketName = uniqid();
            const objectId1 = uniqid();
            const objectId2 = uniqid();
            const objectId3 = uniqid();
            const objectId4 = uniqid();

            await S3Client.put({ Bucket: bucketName, Key: objectId1, Body: JSON.stringify({ data: 'test1' }) });
            await S3Client.put({ Bucket: bucketName, Key: objectId2, Body: JSON.stringify({ data: 'test2' }) });
            await S3Client.put({ Bucket: bucketName, Key: objectId3, Body: JSON.stringify({ data: 'test3' }) });
            await S3Client.put({ Bucket: bucketName, Key: objectId4, Body: JSON.stringify({ data: 'test4' }) });

            expect(await S3Client.get({ Bucket: bucketName, Key: objectId1 })).to.equals(JSON.stringify({ data: 'test1' }));
            expect(await S3Client.get({ Bucket: bucketName, Key: objectId2 })).to.equals(JSON.stringify({ data: 'test2' }));
            expect(await S3Client.get({ Bucket: bucketName, Key: objectId3 })).to.equals(JSON.stringify({ data: 'test3' }));
            expect(await S3Client.get({ Bucket: bucketName, Key: objectId4 })).to.equals(JSON.stringify({ data: 'test4' }));
        }).timeout(5000);
        it('should failed if bucket not exists', (done) => {
            S3Client.get({ Bucket: uniqid(), Key: uniqid() }).catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchBucket');
                done();
            });
        }).timeout(5000);
        it('should failed if objectId not exists', (done) => {
            const bucketName = uniqid();
            const resolvingPromise = new Promise((resolve, reject) => {
                S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) }).then(() => {
                    S3Client.get({ Bucket: bucketName, Key: uniqid() }).catch(err => reject(err));
                });
            });
            resolvingPromise.catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchKey');
                done();
            });
        }).timeout(5000);
        it('get-stream', async () => {
            const bucketName = uniqid();
            const key = uniqid();
            const readStream = fs.createReadStream('tests/big-file.txt');
            await S3Client.putStream({ Bucket: bucketName, Key: key, Body: readStream });
            const res = await S3Client.getStream({ Bucket: bucketName, Key: key });
            await new Promise((resolve, reject) => {
                res.pipe(fs.createWriteStream('tests/dest.txt'))
                    .on('error', (err) => {
                        reject(err);
                    }).on('finish', () => {
                        resolve();
                    });
            });
        }).timeout(35000);
    });

    describe('bucket name validations', () => {
        it('Bucket name contains invalid characters :', async () => {
            const bucketName = 'tal:' + uniqid() + 'tal:tal';
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('Bucket name is longer than 63 characters', async () => {
            const bucketName = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:' + uniqid();
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('Bucket name must start with a letter or number.', async () => {
            const bucketName1 = '-dog:' + uniqid();
            await S3Client.put({ Bucket: bucketName1, Key: uniqid(), Body: JSON.stringify(this) });

            const bucketName2 = '*dog:' + uniqid();
            await S3Client.put({ Bucket: bucketName2, Key: uniqid(), Body: JSON.stringify(this) });

            const bucketName3 = '.dog:' + uniqid();
            await S3Client.put({ Bucket: bucketName3, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('Bucket name must end with a letter or number.', async () => {
            const bucketName1 = 'cat:' + uniqid() + '-';
            await S3Client.put({ Bucket: bucketName1, Key: uniqid(), Body: JSON.stringify(this) });

            const bucketName2 = 'cat:' + uniqid() + '.';
            await S3Client.put({ Bucket: bucketName2, Key: uniqid(), Body: JSON.stringify(this) });

            const bucketName3 = 'cat:' + uniqid() + '^';
            await S3Client.put({ Bucket: bucketName3, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('Bucket name cannot contain uppercase letters.', async () => {
            const bucketName = 'TEST:' + uniqid() + 'TEST-';
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
        it('Bucket name cannot contain consecutive periods (.)', async () => {
            const bucketName = 'Tax..zx:' + uniqid() + '-';
            await S3Client.put({ Bucket: bucketName, Key: uniqid(), Body: JSON.stringify(this) });
        }).timeout(5000);
    });
});
