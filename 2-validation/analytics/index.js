const Ajv = require("ajv")
const identity = require("./schemas/identity.json")
const context = require("./schemas/context.json")
const page = require("./schemas/page.json")
const action = require("./schemas/action.json")
const transaction = require("./schemas/transaction.json")
const ajv = new Ajv({schemas: [identity, context, page, action, transaction]})


const main = async data => 
  ajv.validate(identity, data) ? "valid" : "not valid"




module.exports = {main}
