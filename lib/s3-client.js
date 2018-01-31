const AWS = require('aws-sdk');
const uuid = require('uuid');
const constants = require('./constants');
const { promisify } = require('util');

class S3Client {
    init(options) {
        const opt = {
            s3BucketEndpoint: false,
            s3ForcePathStyle: true,
            ...options
        };
        this.s3 = new AWS.S3(opt);
        this.s3.getObject = promisify(this.s3.getObject);
        this.s3.putObject = promisify(this.s3.putObject);
        this.s3.createBucket = promisify(this.s3.createBucket);
        this.s3.headBucket = promisify(this.s3.headBucket);
        this.s3.deleteBucket = promisify(this.s3.deleteBucket);
    }

    async put(bucketName, object, objectId) {
        if (!objectId) {
            objectId = uuid();
        }
        bucketName = this._parseBucketName(bucketName);
        const params = { Bucket: bucketName, Key: objectId, Body: object };
        await this._createBucketIfNotExists(bucketName);
        await this.s3.putObject(params);
        return objectId;
    }

    async get(bucketName, objectId) {
        bucketName = this._parseBucketName(bucketName);
        const params = { Bucket: bucketName, Key: objectId };
        const data = await this.s3.getObject(params);
        return data.Body.toString('utf-8');
    }

    async deleteBucket(bucketName) {
        bucketName = this._parseBucketName(bucketName);
        const params = { Bucket: bucketName };
        await this.s3.deleteBucket(params);
    }

    async _createBucketIfNotExists(bucketName) {
        const params = { Bucket: bucketName };
        const exists = await this._isBucketExists(params);
        if (!exists) {
            await this.s3.createBucket(params);
        }
    }

    async _isBucketExists(bucketName) {
        try {
            await this.s3.headBucket(bucketName);
            return true;
        }
        catch (error) {
            if (error && error.statusCode === constants.NO_SUCH_BUCKET_STATUS_CODE) {
                return false;
            }
            throw error;
        }
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
