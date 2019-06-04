const Ajv = require("ajv")
const identity = require("./schemas/identity.json")
const context = require("./schemas/context.json")
const page = require("./schemas/page.json")
const action = require("./schemas/action.json")
const transaction = require("./schemas/transaction.json")
const ajv = new Ajv({schemas: [identity, context, page, action, transaction]})
const pipe = fns => x => fns.reduce((v, f) => f(v), x)
const encase = prop => data => { try { return data[prop] } catch (e) { return [] }}
const map = f => xs => xs.map(f)

const IDENTITY = "identity"
const PAGE = "page"
const ACTION = "action"
const TRANSACTION = "transaction"

const parseEvent = pipe([
  encase ("Records"),
  map (encase ("kinesis"))
])

const createBuffer = event => event.map(e => ({...e, data: Buffer.from(e.data, 'base64')})) 
const toString = event => event.map(e =>  ({...e, data: e.data.toString('ascii')}))

const decode = pipe([
  createBuffer,
  toString,
])

const jsonParse = event => event.map(e => ({...e, parsed: JSON.parse(e.data)}))


const check = data => {
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
      return ajv.validate(identity, data)
  }
}

const validate = data => data.map(e => ({...e, valid: check(e.parsed)}))

const redirect = data => 
  data.map(e => e.valid 
    ? Promise.resolve(e).then(({data}) => console.log("save to s3", data))
    : Promise.resolve({errors: ajv.errors, data: e.data}).then(e => console.log(e)))

const recover = data =>
    Promise.all(data)
           .then(_ => "success")

const main = 
  pipe([
    parseEvent,
    decode,
    jsonParse,
    validate,
    redirect,
    (data => Promise.all(data).then(_ => "success")) 
  ]) 

const handler = async event =>
  main (event)



module.exports.handler = handler 
