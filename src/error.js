const { customLogger, red } = require("./utils")
const {
  INTERNAL_SERVER_ERROR,
  HTTP_OK,
  HTTP_BAD_REQUEST,
  HTTP_INTERNAL_SERVER_ERROR,
} = require("./constants")

class QueueError extends Error {
  constructor(message) {
    super(message)
    this.name = "QueueError"
    this.message = message
  }
}

class ValidationError extends Error {
  constructor(message) {
    super(message)
    this.name = "ValidationError"
    this.message = message
  }
}

const handleCustomError = (err, res) => {
  if (err instanceof ValidationError || err instanceof QueueError) {
    customLogger("error", red, `${err.constructor.name}: ${err.message}`)
    return res.status(HTTP_BAD_REQUEST).json({ error: err.message })
  }

  customLogger("error", red, `error: ${err.message}\n${err.stack}`)
  return res
    .status(HTTP_INTERNAL_SERVER_ERROR)
    .json({ error: INTERNAL_SERVER_ERROR })
}

const handleUnknownError = (err, res) => {
  customLogger("error", red, `error: ${err.message}\n${err.stack}`)
  return res
    .status(HTTP_INTERNAL_SERVER_ERROR)
    .json({ error: INTERNAL_SERVER_ERROR })
}

const handleAppErrors = (err, res) => {
  if (err instanceof ValidationError || err instanceof QueueError) {
    handleCustomError(err, res)
  } else {
    handleUnknownError(err, res)
  }
}

module.exports = {
  QueueError,
  ValidationError,
  handleAppErrors,
}
