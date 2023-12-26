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
  if (typeof type === "string" && /^[a-z0-9-]+$/.test(type)) {
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

    if (isCallbackUrlValid && isExpiryTimeValid) {
      return true
    }
  }
  return false
}

const areAllTasksValid = tasks => {
  return Object.entries(tasks).every(([taskId, taskParams]) => {
    return typeof taskParams === "object" && Object.keys(taskParams).length > 0
  })
}

module.exports = {
  doesQueueExist,
  isQueueIdValid,
  isQueueTypeValid,
  areOptionsValid,
  areAllTasksValid,
  doesQueueExist,
}
