const router = require("express").Router()
const {
  customLogger,
  red,
  green,
  yellow,
  blue,
  magenta,
  cyan,
} = require("./utils")
const {
  addTasks,
  createQueueAndAddTasks,
  getNextAvailableTaskByQueue,
  getNextAvailableTaskByType,
  submitResults,
  getResults,
} = require("./services")
const {
  areQueueParametersValid,
  isQueueIdValid,
  isQueueTypeValid,
  areOptionsValid,
  areAllTasksValid,
} = require("./validations")
const { QueueError, ValidationError } = require("./error")
require("dotenv").config()
const INTERNAL_SERVER_ERROR = "Internal server error"

/**
 * Route  for handling POST request to create queue
 */
router.post("/create-queue", async (req, res) => {
  const { type, tasks, options = null } = req.body
  client = req.dbClient
  // VALIDATION
  try {
    if (!type) {
      throw new QueueError("Missing type")
    }
    if (!tasks) {
      throw new QueueError("Missing tasks")
    }
    if (!areQueueParametersValid(type, tasks, options)) {
      throw new ValidationError("Invalid tasks")
    }
    if (options && !areOptionsValid(options)) {
      throw new ValidationError("Invalid options")
    }
  } catch (err) {
    if (err instanceof ValidationError) {
      customLogger("error", red, `Validation Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      customLogger("error", red, `Queue Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else {
      customLogger("error", red, "Unknown Error: ", err.message)
    }
  }
  // PROCESSING
  try {
    const { queue, numTasks } = await createQueueAndAddTasks(
      type,
      options,
      tasks
    )

    if (!queue) {
      throw new Error()
    }

    return res.json({ queue, numTasks })
  } catch (err) {
    customLogger("error", red, "Unknown Error: ", err.message)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

/**
 * Route  for handling POST request to add task
 */
router.post("/add-tasks", async (req, res) => {
  const { queue, tasks, options } = req.body
  client = req.dbClient
  // VALIDATION
  try {
    if (!queue) {
      throw new QueueError("Missing queue")
    }
    if (!tasks) {
      throw new QueueError("Missing tasks")
    }
    if (queue && !isQueueIdValid(queue)) {
      throw new ValidationError("Invalid queue")
    }
    if (tasks && !areAllTasksValid(tasks)) {
      throw new ValidationError("Invalid tasks")
    }
    if (options && !areOptionsValid(options)) {
      throw new ValidationError("Invalid options")
    }
  } catch (err) {
    if (err instanceof ValidationError) {
      customLogger("error", red, `Validation Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      customLogger("error", red, `Queue Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else {
      customLogger("error", red, "Unknown Error:", err)
    }
  }
  // PROCESSING
  try {
    const numTasks = await addTasks(queue, tasks, options)
    return res.json({ numTasks })
  } catch (err) {
    customLogger("error", red, "Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

/**
 * Route  for handling POST requestto get the next task
 */
router.post("/get-next-available-task", async (req, res) => {
  const { queue, type } = req.body
  client = req.dbClient
  // VALIDATION
  try {
    if (!queue && !type) {
      throw new QueueError("Either queue or type must be specified")
    } else if (queue && isQueueIdValid) {
      throw new ValidationError("Invalid queueId")
    } else if (type && !isQueueTypeValid) {
      throw new ValidationError("Invalid queueType")
    }
  } catch (err) {
    if (err instanceof ValidationError) {
      customLogger("error", red, `Validation Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      customLogger("error", red, `Queue Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else {
      customLogger("error", red, "Unknown Error:", err)
    }
  }
  // PROCESSING
  try {
    let nextAvailableTask
    if (queue) {
      nextAvailableTask = await getNextAvailableTaskByQueue(queue)
    } else if (type) {
      nextAvailableTask = await getNextAvailableTaskByType(type)
    }
    if (!nextAvailableTask) {
      return res.status(400).json({
        message: "No available task found",
      })
    }
    return res.status(200).json({
      id: nextAvailableTask.id,
      params: nextAvailableTask.params,
    })
  } catch (err) {
    customLogger("error", red, "Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

/**
 * Route for handling GET request to get the results using queue id
 */
router.get("/get-results/:queue", async (req, res) => {
  const queue = req.params.queue
  client = req.dbClient
  // VALIDATION
  try {
    if (!queue || isNaN(parseInt(queue))) {
      throw new QueueError("Missing queue or Invalid queue")
    }

    const isQueue = await isQueuePresent(parseInt(queue))
    if (queue && !isQueue) {
      throw new ValidationError("Invalid queue")
    }
  } catch (err) {
    if (err instanceof ValidationError) {
      customLogger("error", red, `Validation Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      customLogger("error", red, `Queue Error: ${err.message}`)
      return res.status(400).json({ error: err.message })
    } else {
      customLogger("error", red, "Unknown Error:", err.message)
    }
  }
  // PROCESSING
  try {
    const response = await getResults(queue)
    customLogger("error", red, response)
    if (Object.keys(response.results).length === 0) {
      return res.status(400).json({ message: "No completed tasks found" })
    } else {
      return res.status(200).json(response)
    }
  } catch (err) {
    customLogger("error", red, "Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

/**
 * Route  for handling POST request to submit the results
 */
router.post("/submit-results", async (req, res) => {
  const { id, result, error = null } = req.body

  client = req.dbClient
  try {
    await submitResults({ id, result, error })
    res.send({ ok: true })
    return
  } catch (err) {
    customLogger("error", red, "Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

module.exports = router
