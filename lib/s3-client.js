const AWS = require('aws-sdk');
const constants = require('./constants');
const validator = require('./s3-validator-replacer');

class S3Client {
    constructor() {
        this._s3 = null;
    }
    /**
     * init data for starting
     * @param {object} options 
     * @param {string} options.accessKeyId AWS access key ID 
     * @param {string} options.secretAccessKey AWS secret access key. 
     * @param {string} options.endpoint connection http://{aws server ip}:port
     * @param {boolean} options.s3ForcePathStyle force path style URLs for S3 objects 
     * @param {boolean} options.s3BucketEndpoint provided endpoint addresses an individual bucket (false if it addresses the root API endpoint) 
     * Note: Setting this configuration option requires an endpoint to be provided explicitly to the service constructor.
     */
    init(options) {
        const opt = {
            s3BucketEndpoint: false,
            s3ForcePathStyle: true,
            ...options
        };
        this._s3 = new AWS.S3(opt);
    }

    /**
     * Adds an object to a bucket
     * @param {object} options 
     * @param {string} options.Bucket Bucket name 
     * @param {string} options.Key Object key 
     * @param {object/stream} options.Body Object data/stream
     * @api public
     */
    async put(options) {
        const putParams = validator.validateAndReplacePutParams(options);
        return this._s3.upload(putParams).promise();
    }

    /**
     * Create bucket if not exists
     * @param {string} options.Bucket Bucket name 
     * @param {string} options.CreateBucketConfiguration.LocationConstraint AWS region where to create the bucket.
     * @api public
     */
    async createBucket(options) {
        const bucketParams = validator.validateAndReplaceBucketParams(options);
        return this._createBucketIfNotExists(bucketParams);
    }

    /**
     * Retrieves objects from storage
     * @param {object} options 
     * @param {string} options.Bucket Bucket name 
     * @param {string} options.Key Object key 
     * @returns Object data/stream
     * @api public
     */
    async get(options) {
        const getParams = validator.validateAndReplaceGetParams(options);
        const data = await this._s3.getObject(getParams).promise();
        return JSON.parse(data.Body);
    }

    /**
     * Retrieves objects from storage
     * @param {object} options 
     * @param {string} options.Bucket Bucket name 
     * @param {string} options.Key Object key 
     * @returns stream 
     * @api public
     */
    async getStream(options) {
        const getParams = validator.validateAndReplaceGetParams(options);
        return this._s3.getObject(getParams).createReadStream();
    }

    /**
     * Retrieves objects from storage
     * @param {object} options 
     * @param {string} options.Bucket Bucket name 
     * @param {string} options.Key Object key 
     * @returns Object buffer
     * @api public
     */
    async getBuffer(options) {
        const getParams = validator.validateAndReplaceGetParams(options);
        const data = await this._s3.getObject(getParams).promise();
        return data.Body;
    }

    /**
     * Retrieves objects from storage
     * @param {object} options 
     * @param {string} options.Bucket Bucket name 
     * @param {string} options.Prefix Object key 
     * @returns stream 
     * @api public
     */
    async listObjects(options) {
        let results = [];
        let isTruncated = true;
        let continuationToken = null;

        while (isTruncated) {
            const lastResults = await this._s3.listObjectsV2({
                ...options,
                ContinuationToken: continuationToken
            }).promise();
            results = results.concat(lastResults.Contents);
            isTruncated = lastResults.IsTruncated;
            continuationToken = lastResults.NextContinuationToken;
        }
        return results.map(elem => ({ Key: elem.Key }));
    }

    async deleteObjects(options) {
        const promiseArray = [];
        while (options.Delete.Objects.length) {
            promiseArray.push(this._s3.deleteObjects({
                Bucket: options.Bucket,
                Delete: {
                    Objects: options.Delete.Objects.splice(0, 1000)
                }
            }).promise());
        }
        return Promise.all(promiseArray);
    }

    async _createBucketIfNotExists(options) {
        const exists = await this._isBucketExists(options);
        if (!exists) {
            await this._s3.createBucket(options).promise();
        }
    }

    async _isBucketExists(options) {
        try {
            await this._s3.headBucket({ Bucket: options.Bucket }).promise();
            return true;
        }
        catch (error) {
            if (error && error.statusCode === constants.NO_SUCH_BUCKET_STATUS_CODE) {
                return false;
            }
            throw error;
        }
    }
}

module.exports = new S3Client();
