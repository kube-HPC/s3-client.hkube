const putSchema = {
    type: 'object',
    properties: {
        Bucket: {
            type: 'string',
            minLength: 1
        },
        Key: {
            type: 'string',
            minLength: 1
        },
        Body: {
        }
    },
    required: [
        'Bucket',
        'Key',
        'Body'
    ]
};

const getSchema = {
    type: 'object',
    properties: {
        Bucket: {
            type: 'string',
            minLength: 1
        },
        Key: {
            type: 'string',
            minLength: 1
        }
    },
    required: [
        'Bucket',
        'Key'
    ]
};

module.exports = {
    putSchema,
    getSchema
};
