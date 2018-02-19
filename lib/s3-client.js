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
        return this._s3;
    }

    /**
     * Adds an object to a bucket (create bucket if not exists)
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @param {object} options.Body Object data
     * @api public
     */
    async put(options) {
        const params = validator.validatePutParams(options);
        await this._createBucketIfNotExists(params.Bucket);
        await this._s3.putObject(params).promise();
    }

    /**
     * Adds an object to a bucket (create bucket if not exists)
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @param {stream} options.Body Object data
     * @api public
     */
    async putStream(options) {
        const params = validator.validatePutParams(options);
        await this._createBucketIfNotExists(params.Bucket);
        return this._s3.upload(params).promise();
    }

    /**
     * Retrieves objects from Amazon
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @returns Object data
     * @api public
     */
    async get(options) {
        const params = validator.validateGetParams(options);
        const data = await this._s3.getObject(params).promise();
        return JSON.parse(data.Body);
    }

    /**
     * Retrieves objects from Amazon
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @returns stream handler
     * @api public
     */
    async getStream(options) {
        const params = validator.validateGetParams(options);
        return this._s3.getObject(params).createReadStream();
    }

    async _createBucketIfNotExists(bucketName) {
        const params = { Bucket: bucketName };
        const exists = await this._isBucketExists(params);
        if (!exists) {
            await this._s3.createBucket(params).promise();
        }
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
