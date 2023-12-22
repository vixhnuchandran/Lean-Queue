const isQueueIdValid = queueId => {
  if (typeof queueId === "number" && queueId > 0) {
    return true
  } else {
    return false
  }
}

const doesQueueExist = async queueId => {
  const queryStr = `
  SELECT EXISTS 
    (SELECT 1 FROM queues WHERE id = ${queueId});
  `
  const response = await client.query(queryStr)
  return response.rows[0].exists
}

const isQueueTypeValid = type => {
  if (typeof type === "string") {
    return true
  } else {
    return false
  }
}

const areOptionsValid = options => {
  if (typeof options === "object" && typeof options.callback === "string") {
    const urlRegex = /^(http|https):\/\/[^ "]+$/
    const isCallbackUrlValid = urlRegex.test(options.callback)

    const expiryTime = options.expiryTime
    const isExpiryTimeValid = Number.isInteger(expiryTime) && expiryTime > 0

    return isCallbackUrlValid && isExpiryTimeValid
  }
  return false
}

const areAllTasksValid = tasks => {
  return Object.entries(tasks).every(([taskId, taskParams]) => {
    return typeof taskParams === "object" && Object.keys(taskParams).length > 0
  })
}

const validateQueueRequest = (req, res, next) => {
  try {
    const { type, tasks, options } = req.body

    if (!type) {
      throw new QueueError("Missing type")
    }

    if (!tasks) {
      throw new QueueError("Missing tasks")
    }

    if (!validations.isQueueTypeValid(type)) {
      throw new ValidationError("Invalid type")
    }

    if (!validations.areAllTasksValid(tasks)) {
      throw new ValidationError("Invalid tasks")
    }

    if (options && !validations.areOptionsValid(options)) {
      throw new ValidationError("Invalid options")
    }

    next()
  } catch (err) {
    handleError(err, res)
  }
}

module.exports = {
  validateQueueRequest,
  doesQueueExist,
  isQueueIdValid,
  isQueueTypeValid,
  areOptionsValid,
  areAllTasksValid,
}
