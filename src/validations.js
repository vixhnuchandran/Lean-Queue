const isQueueIdValid = queueId => {
  if (typeof queueId === "number" && queueId > 0) {
    return true
  } else {
    return false
  }
}

const isTagValid = tag => {
  if (typeof tag === "string") {
    return true
  } else {
    return false
  }
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

const areQueueParametersValid = (type, tasks, options) => {
  if (!isQueueTypeValid(type)) {
    return false
  } else if (!areAllTasksValid(tasks)) {
    return false
  } else if (options !== null) {
    if (!areOptionsValid(options)) {
      return false
    }
  }
  return true
}

const areAllTasksValid = tasks => {
  return Object.entries(tasks).every(([taskId, taskParams]) => {
    return typeof taskParams === "object" && Object.keys(taskParams).length > 0
  })
}

module.exports = {
  areQueueParametersValid,
  isTagValid,
  isQueueIdValid,
  isQueueTypeValid,
  areOptionsValid,
  areAllTasksValid,
}
