const {main} = require("./index.js")
const payloadIdentity = require("./payload_identity.json")
const payloadPage = require("./payload_page.json")
const payloadAction = require("./payload_action.json")
const payloadTransaction = require("./payload_transaction.json")
const {describe} = require("riteway")

describe("main()", async assert =>
  assert({
    given: "json pipes.identity() payload",
    should: "validate required and not-required fields",
    actual: await main(payloadIdentity).catch(e => console.log(e) || e),
    expected: "valid"
  })
)

describe("main()", async assert =>
  assert({
    given: "json pipes.page() payload",
    should: "validate required and not-required fields",
    actual: await main(payloadPage).catch(e => console.log(e) || e),
    expected: "valid"
  })
)

describe("main()", async assert =>
  assert({
    given: "json pipes.action() payload",
    should: "validate required and not-required fields",
    actual: await main(payloadAction).catch(e => console.log(e) || e),
    expected: "valid"
  })
)

describe("main()", async assert =>
  assert({
    given: "json pipes.transaction() payload",
    should: "validate required and not-required fields",
    actual: await main(payloadTransaction).catch(e => console.log(e) || e),
    expected: "valid"
  })
)
