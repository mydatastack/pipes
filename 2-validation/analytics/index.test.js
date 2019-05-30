const {main} = require("./index.js")
const payload = require("./payload.json")
const {describe} = require("riteway")

describe("main()", async assert =>
  assert({
    given: "json payload",
    should: "validate fields",
    actual: await main(payload).catch(e => console.log(e) || e),
    expected: "success"
  })
)
