const { putSchema, getSchema, createSchema } = require('./schemas');
const stream = require('stream');
const Validator = require('ajv');
const validatorInstance = new Validator({ useDefaults: true, coerceTypes: false });
class S3ValidatorReplacer {
    constructor() {
        this._putSchema = validatorInstance.compile(putSchema);
        this._getSchema = validatorInstance.compile(getSchema);
        this._createSchema = validatorInstance.compile(createSchema);
    }

    validateAndReplaceBucketParams(params) {
        this._validateSchema(this._createSchema, params);
        return { ...params, Bucket: this._parseBucket(params.Bucket) };
    }

    validateAndReplacePutParams(params) {
        this._validateSchema(this._putSchema, params);
        return {
            Bucket: this._parseBucket(params.Bucket),
            Key: this._parseKey(params.Key),
            Body: params.Body instanceof stream === true ? params.Body : JSON.stringify(params.Body)
        };
    }

    validateAndReplaceGetParams(params) {
        this._validateSchema(this._getSchema, params);
        return {
            Bucket: this._parseBucket(params.Bucket),
            Key: this._parseKey(params.Key),
        };
    }

    _validateSchema(validator, object) {
        const valid = validator(object);
        if (!valid) {
            const error = validatorInstance.errorsText(validator.errors);
            throw new Error(error);
        }
    }

    _parseBucket(bucketName) {
        if (!bucketName.trim()) {
            throw new TypeError('Bucket cannot be empty');
        }
        if (bucketName.length > 63 || bucketName.length < 3) {
            throw new Error('Bucket names must be at least 3 and no more than 63 characters long.');
        }
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

