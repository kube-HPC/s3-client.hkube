const bson = require('bson');

const binaryDecode = (data) => {
    const ret = bson.deserialize(data, { promoteBuffers: true, promoteValues: true });
    return ret.data;
};

const binaryEncode = data => bson.serialize({ data });

module.exports = {
    binaryDecode,
    binaryEncode
};
