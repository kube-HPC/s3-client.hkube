const { putSchema, getSchema, createSchema } = require('./schemas');
const stream = require('stream');
const djsv = require('djsv');

class S3ValidatorReplacer {
    constructor() {
        this._putSchema = djsv(putSchema);
        this._getSchema = djsv(getSchema);
        this._createSchema = djsv(createSchema);
    }

    validateBucketName(params) {
        this._validateSchema(this._createSchema, params);
        return {
            Bucket: this._parseBucket(params.Bucket)
        };
    }

    validatePutParams(params) {
        this._validateSchema(this._putSchema, params);
        return {
            Bucket: this._parseBucket(params.Bucket),
            Key: this._parseKey(params.Key),
            Body: params.Body instanceof stream === true ? params.Body : JSON.stringify(params.Body)
        };
    }

    validateGetParams(params) {
        this._validateSchema(this._getSchema, params);
        return {
            Bucket: this._parseBucket(params.Bucket),
            Key: this._parseKey(params.Key),
        };
    }

    _validateSchema(validator, object) {
        const schema = validator(object);
        if (!schema.valid) {
            throw new Error(schema.error);
        }
    }

    _parseBucket(bucketName) {
        if (!bucketName.trim()) {
            throw new TypeError('Bucket cannot be empty');
        }
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

    _parseKey(key) {
        if (!key.trim()) {
            throw new Error('Key cannot be empty');
        }
        return key;
    }
}

module.exports = new S3ValidatorReplacer();

