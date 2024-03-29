module.exports = {
    "extends": ["airbnb-base"],
    "env": {
        "es6": true,
        "node": true,
        "mocha": true
    },
    "plugins": [
        "chai-friendly"
    ],
    "parserOptions": {
        "sourceType": "module",
        "ecmaVersion": 2017
    },
    "rules": {
        "no-param-reassign": "off",
        "no-use-before-define": "warn",
        "import/newline-after-import": "off",
        "indent": ["warn", 4],
        "prefer-template": "off",
        "comma-dangle": "off",
        "no-underscore-dangle": "off",
        "max-len": ["error", 200],
        "brace-style": ["error", "stroustrup"],
        "no-trailing-spaces": "off",
        "no-console": "error",
        "linebreake-style": "off",
        "no-var": "error",
        "object-curly-spacing": "off",
        "arrow-body-style": "off",
        "class-methods-use-this": "off",
        "no-unused-expressions": 0,
        "chai-friendly/no-unused-expressions": 2,
        "no-await-in-loop": "off"
    }
};