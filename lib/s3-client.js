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
        const params = validator.validatePutParams(options);
        return this._s3.upload(params).promise();
    }

    /**
     * Create bucket if not exists
     * @param {string} options.Bucket Bucket name 
     * @api public
     */
    async createBucket(options) {
        const params = validator.validateBucketName(options);
        return this._createBucketIfNotExists(params);
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
        const params = validator.validateGetParams(options);
        const data = await this._s3.getObject(params).promise();
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
        const params = validator.validateGetParams(options);
        return this._s3.getObject(params).createReadStream();
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
                Bucket: options.Bucket,
                Prefix: options.Prefix,
                ContinuationToken: continuationToken
            }).promise();
            results = results.concat(lastResults.Contents);
            isTruncated = lastResults.IsTruncated;
            continuationToken = lastResults.NextContinuationToken;
        }
        return results.map(elem => ({ Key: elem.Key }));
    }

    async deleteObjects(options) {
        return this._s3.deleteObjects(options).promise();
    }

    async _createBucketIfNotExists(bucketName) {
        const exists = await this._isBucketExists(bucketName);
        if (!exists) {
            await this._s3.createBucket(bucketName).promise();
        }
        return bucketName;
    }

    async _isBucketExists(bucketName) {
        try {
            await this._s3.headBucket(bucketName).promise();
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
