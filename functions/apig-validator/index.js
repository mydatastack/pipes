const AWS = require("aws-sdk")
const firehose = new AWS.Firehose({region: process.env.REGION || "eu-central-1"})
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

const DELIVERY_STREAM_SUCCESS = 
  process.env.DELIVERY_STREAM_SUCCESS 
  || "tarasowski-pipes-local-deployment-AP-EventFirehose-XKUV26WCGYVJ"

const DELIVERY_STREAM_FAILURE = process.env.DELIVERY_STREAM_FAILURE

const IDENTITY = "identity"
const PAGE = "page"
const ACTION = "action"
const TRANSACTION = "transaction"

const parseEvent = pipe([
  encase ("Records"),
  map (encase ("kinesis"))
])

const createBuffer = event => event.map(e => ({...e, originalData: e.data, data: Buffer.from(e.data, 'base64')})) 
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
      return false 
  }
}

const validate = data => data.map(e => ({...e, valid: check(e.parsed)}))

const buildRecords = data => data.map(e => ({Data: Buffer.from(e.originalData)}))
const buildParams = data => ({
  DeliveryStreamName: DELIVERY_STREAM_SUCCESS,
  Records: data
})

const params = pipe([
  buildRecords,
  buildParams,
])

const redirect = data => {
  const valid = data.filter(e => e.valid === true)
  const invalid = data.filter(e => e.valid === false)
  return [valid, invalid]
} 

const report = stream => x => console.log(stream + " FailedPutCount:", x.FailedPutCount || 0, "\n " + stream + "Inserted Records:", x.RequestResponses && x.RequestResponses.length || 0)

const sendData = ([valid, invalid]) =>
  Promise.resolve()
    .then(_ => valid.length > 0
      ? firehose.putRecordBatch(params (valid)).promise() 
      : [])     
    .then(x => report ("SuccessStream") (x) || "success", e => console.log(e) || "error")
    .then(_ => invalid.length > 0
      ? Promise.resolve("errors goes to another stream") 
      : [])     
    .then(x => report ("ErrorStream") (x) || "success", e => console.log(e) || "error")
    .catch(e => console.log(e) || e)


const main = 
  pipe([
    parseEvent,
    decode,
    jsonParse,
    validate,
    redirect,
    sendData
  ]) 

const handler = event =>
  main (event)

module.exports.handler = handler 
