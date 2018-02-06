const AWS = require('aws-sdk');
const constants = require('./constants');

class S3Client {
    /**
     * init data for starting
     * @param {object} options 
     * @param {string} options.accessKeyId AWS access key ID 
     * @param {string} options.secretAccessKey AWS secret access key. 
     * @param {string} options.endpoint connection http://{minio}:port
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
        this.s3 = new AWS.S3(opt);
    }

    /**
     * Adds an object to a bucket (create bucket if not exists)
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @param {string} options.Body Object data
     */
    async put(options) {
        options.Bucket = this._parseBucketName(options.Bucket);
        await this._createBucketIfNotExists(options.Bucket);
        await this.s3.putObject(options).promise();
    }

    /**
     * Adds an object to a bucket (create bucket if not exists)
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @param {stream} options.Body Object data
     */
    async putStream(options) {
        await this._createBucketIfNotExists(options.Bucket);
        return this.s3.upload(options).promise();
    }

    /**
     * Retrieves objects from Amazon
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @returns {string} Object data
     */
    async get(options) {
        options.Bucket = this._parseBucketName(options.Bucket);
        const data = await this.s3.getObject(options).promise();
        return data.Body.toString('utf-8');
    }

    /**
     * Retrieves objects from Amazon
     * @param {object} options 
     * @param {string} options.Bucket Bucket name (pipelineID)
     * @param {string} options.Key Object key (taskID)
     * @returns {stream} stream handler
     */
    async getStream(options) {
        options.Bucket = this._parseBucketName(options.Bucket);
        return this.s3.getObject(options).createReadStream();
    }

    async deleteBucket(options) {
        options.Bucket = this._parseBucketName(options.Bucket);
        await this.s3.deleteBucket(options).promise();
    }

    async _createBucketIfNotExists(bucketName) {
        const params = { Bucket: bucketName };
        const exists = await this._isBucketExists(params);
        if (!exists) {
            await this.s3.createBucket(params).promise();
        }
    }

    async _isBucketExists(bucketName) {
        try {
            await this.s3.headBucket(bucketName).promise();
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
