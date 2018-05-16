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
        const split = bucketName.split('.');
        const left = split.shift();
        bucketName = left + (split.length > 0 ? '.' + split.length : '');
        bucketName = bucketName.toLowerCase();
        bucketName = bucketName.replace(/[^A-Za-z0-9]/g, '');
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

