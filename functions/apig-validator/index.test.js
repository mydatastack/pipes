const handler = require("./index.js").handler
const streamPayloadValid = require("./payload_kinesis_valid.json")
const streamPayloadInvalid = require("./payload_kinesis_invalid.json")
const {describe} = require("riteway")

describe("handler()", async assert =>
  assert({
    given: "json from kinesis data stream that is valid to the json schema",
    should: "save data into to firehose",
    actual: await handler(streamPayloadValid).catch(e => console.log(e) || e),
    expected: "success"
  })
)

describe("handler()", async assert =>
  assert({
    given: "json from kinesis with data that is not valid to the schema",
    should: "save data into another stream with error and data itself",
    actual: await handler(streamPayloadInvalid).catch(e => console.log(e) || e),
    expected: "success"
  })
)

