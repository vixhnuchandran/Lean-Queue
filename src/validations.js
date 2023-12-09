const validateCreateQueuesInput = input => {
  const { type, tasks } = input
  try {
    if (typeof input !== "object") {
      throw new Error("Invalid tasks: Tasks must be an object")
    }
    const { type, tasks } = input

    if (typeof type !== "string") {
      throw new Error("Invalid tyoe: Type must be a non-empty string")
    }
    if (typeof tasks !== "object" || Object.keys(tasks).length === 0) {
      throw new Error("Invalid tasks: Tasks must be a non-empty object")
    }

    Object.entries(tasks).forEach(([taskId, taskParams]) => {
      if (
        typeof taskParams !== "object" ||
        !("num1" in taskParams) ||
        !("num2" in taskParams)
      ) {
        throw new Error(`Invalid parameters for task ${taskId}`)
      }
    })

    return true
  } catch (error) {
    console.log(`Validation Error: ${error.message}`)
    return false
  }
}

const validateAddTasksInput = input => {
  const { queue, tasks } = input
  try {
    if (typeof input !== "object") {
      throw new Error("Invalid input: Input must be an object")
    }

    const { queue, tasks } = input

    if (typeof queue !== "number" || queue < 1) {
      throw new Error("Invalid queue: Queue must be a positive number")
    }

    if (typeof tasks !== "object" || Object.keys(tasks).length === 0) {
      throw new Error("Invalid tasks: Tasks must be a non-empty object")
    }

    Object.entries(tasks).forEach(([taskId, taskParams]) => {
      if (
        typeof taskParams !== "object" ||
        !("num1" in taskParams) ||
        !("num2" in taskParams)
      ) {
        throw new Error(`Invalid parameters for task ${taskId}`)
      }
    })

    return true
  } catch (error) {
    console.error(`Validation Error: ${error.message}`)
    return false
  }
}

const validateQueue = input => {
  try {
    if (typeof input !== "string") {
      throw new Error("Invalid type: Type must be a non-empty string")
    }
    return true
  } catch (error) {
    console.error(`Validation Error: ${error.message}`)
    return false
  }
}

const validateTasks = input => {
  const failedTasks = []
  Object.entries(input).forEach(([taskId, taskParams]) => {
    if (
      typeof taskParams !== "object" ||
      !("num1" in taskParams) ||
      !("num2" in taskParams)
    ) {
      failedTasks.push(taskId)
    }
  })
  return failedTasks
}
module.exports = {
  validateQueue,
  validateTasks,
  validateCreateQueuesInput,
  validateAddTasksInput,
}
