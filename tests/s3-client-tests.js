
const { expect } = require('chai');
const S3Client = require('../index');
const uniqid = require('uuid');


describe('s3-client', () => {
    let buckets = [];

    before((done) => {
        const options = {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000'
        };
        S3Client.init(options);
        done();
    });
    beforeEach(() => {
        buckets = [];
    });

    afterEach(async () => {
        const bucketsToWaitFor = [];
        buckets.forEach(b => bucketsToWaitFor.push(S3Client.deleteBucket(b)));
        await Promise.all(bucketsToWaitFor);
    });
    function getBucketName() {
        const id = uniqid();
        buckets.push(id);
        return id;
    }

    describe('put', () => {
        it('should put new object even if bucket not exists', async () => {
            const bucketName = getBucketName();
            const objectId = uniqid();
            const result = await S3Client.put(bucketName, 'test1234', objectId);
            expect(result).to.equals(objectId);
        }).timeout(5000);
        it('should put new object even if objectId is missing ', async () => {
            const objectId = await S3Client.put(getBucketName(), 'test1234');
            expect(objectId).to.not.be.undefined;
        }).timeout(5000);
        it('add multiple objects to the same bucket', async () => {
            const bucketName = getBucketName();
            await S3Client.put(bucketName, uniqid());
            await S3Client.put(bucketName, uniqid(), uniqid());
            await S3Client.put(bucketName, uniqid(), uniqid());
            await S3Client.put(bucketName, uniqid());
            await S3Client.put(bucketName, uniqid());
        }).timeout(5000);
    });
    describe('get', () => {
        it('should get object by bucket & objectId', async () => {
            const bucketName = getBucketName();
            const objectId1 = await S3Client.put(bucketName, 'test1');
            const objectId2 = await S3Client.put(bucketName, 'test2');
            const objectId3 = await S3Client.put(bucketName, 'test3');
            const objectId4 = await S3Client.put(bucketName, 'test4');

            expect(await S3Client.get(bucketName, objectId1)).to.equals('test1');
            expect(await S3Client.get(bucketName, objectId2)).to.equals('test2');
            expect(await S3Client.get(bucketName, objectId3)).to.equals('test3');
            expect(await S3Client.get(bucketName, objectId4)).to.equals('test4');
        }).timeout(5000);
        it('should failed if bucket not exists', (done) => {
            S3Client.get(uniqid(), uniqid()).catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchKey');
                done();
            });
        }).timeout(5000);
        it('should failed if objectId not exists', (done) => {
            const bucketName = getBucketName();
            const resolvingPromise = new Promise((resolve, reject) => {
                S3Client.put(bucketName, 'test1').then(() => {
                    S3Client.get(bucketName, 'objectIdNotExists').catch(err => reject(err));
                });
            });
            resolvingPromise.catch((err) => {
                expect(err.statusCode).to.equal(404);
                expect(err.name).to.equal('NoSuchKey');
                done();
            });
        }).timeout(5000);
    });

    describe('bucket name validations', () => {
        it('Bucket name contains invalid characters :', async () => {
            const bucketName = 'tal:' + uniqid() + 'tal:tal';
            buckets.push(bucketName);
            await S3Client.put(bucketName, 'test1');
        }).timeout(5000);
        it('Bucket name is longer than 63 characters', async () => {
            const bucketName = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:' + uniqid();
            buckets.push(bucketName);
            await S3Client.put(bucketName, 'test1');
        }).timeout(5000);
        it('Bucket name must start with a letter or number.', async () => {
            const bucketName1 = '-dog:' + uniqid();
            buckets.push(bucketName1);
            await S3Client.put(bucketName1, 'test1');

            const bucketName2 = '*dog:' + uniqid();
            buckets.push(bucketName2);
            await S3Client.put(bucketName2, 'test1');

            const bucketName3 = '.dog:' + uniqid();
            buckets.push(bucketName3);
            await S3Client.put(bucketName3, 'test1');
        }).timeout(5000);
        it('Bucket name must end with a letter or number.', async () => {
            const bucketName1 = 'cat:' + uniqid() + '-';
            buckets.push(bucketName1);
            await S3Client.put(bucketName1, 'test1');

            const bucketName2 = 'cat:' + uniqid() + '.';
            buckets.push(bucketName2);
            await S3Client.put(bucketName2, 'test1');

            const bucketName3 = 'cat:' + uniqid() + '^';
            buckets.push(bucketName3);
            await S3Client.put(bucketName3, 'test1');
        }).timeout(5000);
        it('Bucket name cannot contain uppercase letters.', async () => {
            const bucketName = 'TEST:' + uniqid() + 'TEST-';
            buckets.push(bucketName);
            await S3Client.put(bucketName, 'test1');
        }).timeout(5000);
        it('Bucket name cannot contain consecutive periods (.)', async () => {
            const bucketName = 'Tax..zx:' + uniqid() + '-';
            buckets.push(bucketName);
            await S3Client.put(bucketName, 'test1');
        }).timeout(5000);
    });
});
