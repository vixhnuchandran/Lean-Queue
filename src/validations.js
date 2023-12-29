const { ValidationError, QueueError } = require("./error")
const { logger } = require("./utils")

const validateQueueId = async (queueId, QM) => {
  if (!Number.isInteger(queueId) || queueId <= 0)
    throw new ValidationError(`invalid queue id`)

  if (!(await doesQueueExist(QM, queueId))) {
    throw new QueueError(`queue id does not exist`)
  }
}

const doesQueueExist = async (QM, queueId) => {
  const queryStr = `
      SELECT EXISTS 
        (SELECT 1 FROM queues WHERE id = $1);
    `
  const response = await QM.client.query(queryStr, [queueId])
  if (!response.rows[0].exists) {
    return false
  }
  return true
}

const validateQueueType = type => {
  if (typeof type !== "string")
    throw new ValidationError(`'callbackurl' must be a string type`)

  if (!/^[a-z0-9-]+$/.test(type))
    throw new ValidationError(`invalid 'type' format`)
}

const validateOptions = options => {
  const urlRegex = /^(https?|http):\/\/[^\s/$.?#].[^\s]*$/
  const expiryTime = options.expiryTime

  if (typeof options !== "object")
    throw new ValidationError(`'options' must be a object type`)

  if (typeof options.callback !== "string")
    throw new ValidationError(`'callbackurl' must be a string type`)

  if (!urlRegex.test(options.callback))
    throw new ValidationError(`invalid 'callbackurl' format`)

  if (!Number.isInteger(expiryTime) && expiryTime <= 0)
    throw new ValidationError(`invalid 'expiryTime'`)

  return true
}

const validateTasks = tasks => {
  tasks.forEach((task, i) => {
    if (typeof task !== "object")
      throw new ValidationError(`tasks[${i}]: not an object`)

    if (Object.keys(task).length <= 0)
      throw new ValidationError(`tasks[${i}]: is empty`)

    if (task.taskId === undefined || task.taskId === null)
      throw new ValidationError(`tasks[${i}]: taskId missing`)

    if (
      task.params === undefined ||
      task.params === null ||
      Object.keys(task.params).length <= 0
    )
      throw new ValidationError(`tasks[${i}]: params missing`)

    if (task.priority !== undefined && task.priority !== null) {
      if (!Number.isInteger(task.priority))
        throw new ValidationError(`tasks[${i}]: priority not an integer`)
    }

    return true
  })
}

const validateNonEmptyRequestBody = req => {
  if (!req.body || Object.keys(req.body).length === 0) {
    throw new ValidationError("empty request body")
  }
}

module.exports = {
  validateNonEmptyRequestBody,
  validateQueueType,
  validateQueueId,
  validateOptions,
  validateTasks,
}
