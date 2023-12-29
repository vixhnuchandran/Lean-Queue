const { logger } = require("./utils")
const {
  INTERNAL_SERVER_ERROR,
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

const handleCustomError = (err, req, res) => {
  if (err instanceof ValidationError || err instanceof QueueError) {
    logger.error(`${err.constructor.name}: ${err.message}`)
    return res.status(HTTP_BAD_REQUEST).json({ error: err.message })
  }

  logger.error(`error: ${err.message}`)
  return res
    .status(HTTP_INTERNAL_SERVER_ERROR)
    .json({ error: INTERNAL_SERVER_ERROR })
}

const handleUnknownError = (err, req, res) => {
  logger.error(`error: ${err.message}`)
  if (req.QM && req.QM.client) {
    req.QM.client.release()
  }
  return res
    .status(HTTP_INTERNAL_SERVER_ERROR)
    .json({ error: INTERNAL_SERVER_ERROR })
}

const handleAppErrors = (err, req, res, next) => {
  if (err instanceof ValidationError || err instanceof QueueError) {
    return handleCustomError(err, req, res)
  } else {
    return handleUnknownError(err, req, res)
  }
}

module.exports = {
  ValidationError,
  QueueError,
  handleAppErrors,
}
