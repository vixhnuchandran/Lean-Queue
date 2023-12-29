const { pool } = require("./db")
const { QueryManager } = require("./queueManager")
const { v4: uuidv4 } = require("uuid")
const { handleAppErrors } = require("./error")
const { logger } = require("./utils")

const attachRequestIdMiddleware = (req, res, next) => {
  req.requestId = uuidv4()
  next()
}

const attachQueryManagerMiddleware = async (req, res, next) => {
  const client = await pool.connect()
  req.queryManager = new QueryManager(client)

  next()
}

const releaseQueryManagerClientMiddleware = (req, res, next) => {
  req.queryManager.client.release()
  logger.log(`Client released in releaseQueryManagerClientMiddleware`)

  next()
}

const errorHandlingMiddleware = (err, req, res, next) => {
  try {
    handleAppErrors(err, req, res)
  } finally {
    req.queryManager.client.release()
    logger.log("Client released in errorHandlingMiddleware")
  }
}

module.exports = {
  attachRequestIdMiddleware,
  attachQueryManagerMiddleware,
  releaseQueryManagerClientMiddleware,
  errorHandlingMiddleware,
}
