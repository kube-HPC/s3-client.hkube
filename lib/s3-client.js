const AWS = require('aws-sdk');
const uuid = require('uuid');
const constants = require('./constants');

class S3Client {
    init(options) {
        options.s3BucketEndpoint = false;
        options.s3ForcePathStyle = true;
        this.s3 = new AWS.S3(options);
    }

    async put(bucketName, object, objectId) {
        if (!objectId) objectId = uuid();
        bucketName = this._parseBucketName(bucketName);
        const params = { Bucket: bucketName, Key: objectId, Body: object };
        await this._createBucketIfNotExists(bucketName);
        return new Promise((resolve, reject) => {
            this.s3.putObject(params, (err) => {
                return err ? reject(err) : resolve(objectId);
            });
        });
    }

    async get(bucketName, objectId) {
        const params = { Bucket: bucketName, Key: objectId };
        return new Promise((resolve, reject) => {
            this.s3.getObject(params, (err, data) => {
                return err ? reject(err) : resolve(data.Body.toString('utf-8'));
            });
        });
    }

    _createBucketIfNotExists(bucketName) {
        return new Promise(async (resolve, reject) => {
            const params = { Bucket: bucketName };
            const exists = await this._isBucketExists(params);
            if (!exists) {
                this.s3.createBucket(params, (err) => {
                    return err ? reject(err) : resolve();
                });
            }
            else resolve();
        });
    }

    _isBucketExists(bucketName) {
        return new Promise((resolve, reject) => {
            this.s3.headBucket(bucketName, (err) => {
                if (!err) resolve(true);
                if (err && err.statusCode === constants.NO_SUCH_BUCKET_STATUS_CODE) resolve(false);
                else reject(err);
            });
        });
    }

    _parseBucketName(bucketName) {
        if (bucketName.length > 63) {
            bucketName = bucketName.substring(bucketName.length - 63, bucketName.length);
        }
        bucketName = bucketName.toLowerCase();
        bucketName = bucketName.replace(/:/g, '-');
        bucketName = bucketName.replace(/^[^a-z0-9]/, '0');
        bucketName = bucketName.replace(/[^a-z0-9]$/, '0');
        bucketName = bucketName.replace(/\.{2,}/, '0');
        return bucketName;
    }
}

module.exports = new S3Client();
