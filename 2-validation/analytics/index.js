const Ajv = require("ajv")
const identity = require("./schemas/identity.json")
const context = require("./schemas/context.json")
const page = require("./schemas/page.json")
const action = require("./schemas/action.json")
const transaction = require("./schemas/transaction.json")
const ajv = new Ajv({schemas: [identity, context, page, action, transaction]})
const pipe = fns => x => fns.reduce((v, f) => f(v), x)

const IDENTITY = "identity"
const PAGE = "page"
const ACTION = "action"
const TRANSACTION = "transaction"


const validate = data => {
  const {type} = data
  switch (type) {
    case IDENTITY:
      return ajv.validate(identity, data) ? true : false
    case PAGE:
      return ajv.validate(page, data) ? true : false
    case ACTION:
      return ajv.validate(action, data) ? true : false
    case TRANSACTION:
      return ajv.validate(transaction, data) ? true : false
    default:
      return false
  }
}

const isValid = valid =>
  valid ? Promise.resolve("valid") : Promise.reject(ajv.errors || "event hat not a valid type")

const redirect = p =>
  p.then(x => x, e => console.log("send to error stream") || e)

const main = async data => 
  pipe([
    validate,
    isValid,
    redirect
  ]) (data)




module.exports = {main}
