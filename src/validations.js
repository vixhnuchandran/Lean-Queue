// const validateCreateQueuesInput = queue => {
//   const { type, tasks } = queue
//   try {
//     if (typeof queue !== "object") {
//       throw new Error("Invalid tasks: Tasks must be an object")
//     }
//     const { type, tasks } = queue

//     if (typeof type !== "string") {
//       throw new Error("Invalid tyoe: Type must be a non-empty string")
//     }
//     if (typeof tasks !== "object" || Object.keys(tasks).length === 0) {
//       throw new Error("Invalid tasks: Tasks must be a non-empty object")
//     }

//     Object.entries(tasks).forEach(([taskId, taskParams]) => {
//       if (typeof taskParams !== "object") {
//         throw new Error(`Invalid parameters for task ${taskId}`)
//       }
//     })

//     return true
//   } catch (error) {
//     console.log(`Validation Error: ${error.message}`)
//     return false
//   }
// }

// const validateAddTasksInput = alltasks => {
//   const { queue, tasks } = tasks
//   try {
//     if (typeof tasks !== "object") {
//       throw new Error("Invalid input: Input must be an object")
//     }

//     const { queue, tasks } = tasks

//     if (typeof queue !== "number" || queue < 1) {
//       throw new Error("Invalid queue: Queue must be a positive number")
//     }

//     if (typeof tasks !== "object" || Object.keys(tasks).length === 0) {
//       throw new Error("Invalid tasks: Tasks must be a non-empty object")
//     }

//     Object.entries(tasks).forEach(([taskId, taskParams]) => {
//       if (typeof taskParams !== "object") {
//         throw new Error(`Invalid parameters for task ${taskId}`)
//       }
//     })

//     return true
//   } catch (error) {
//     console.error(`Validation Error: ${error.message}`)
//     return false
//   }
// }

const isQueueTypeValid = queueType => {
  try {
    if (typeof queueType !== "string") {
      throw new Error("Invalid type: Type must be a non-empty string")
    }
    return true
  } catch (error) {
    console.error(`Validation Error: ${error.message}`)
    return false
  }
}

const iseAllTasksValid = tasks => {
  const failedTasks = []
  Object.entries(tasks).forEach(([taskId, taskParams]) => {
    if (typeof taskParams !== "object") {
      failedTasks.push(taskId)
    }
  })
  return failedTasks
}

module.exports = {
  isQueueTypeValid,
  iseAllTasksValid,
}
