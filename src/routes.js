const router = require("express").Router()
const { customLogger, red, cyan } = require("./utils")
const {
  addTasks,
  createQueueAndAddTasks,
  getNextAvailableTaskByQueue,
  getNextAvailableTaskByType,
  getNextAvailableTaskByTag,
  submitResults,
  getResults,
  getStatus,
} = require("./services")
const {
  areQueueParametersValid,
  isQueueIdValid,
  isTagValid,
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
  const { type, tasks, options = null, tags } = req.body
  console.log(type, tasks, options, tags)
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
      tasks,
      tags
    )

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
// TODO instead of id and queue make  tag
router.post("/get-next-available-task", async (req, res) => {
  const { queue, type, tag } = req.body
  client = req.dbClient
  // VALIDATION
  try {
    if (!queue && !type && !tag) {
      throw new QueueError("Either queue, type, or tag must be specified")
    } else if (queue && !isQueueIdValid(queue)) {
      throw new ValidationError("Invalid queueId")
    } else if (type && !isQueueTypeValid(type)) {
      throw new ValidationError("Invalid queueType")
    } else if (tag && !isTagValid(tag)) {
      throw new ValidationError("Invalid tag")
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
    } else if (tag) {
      nextAvailableTask = await getNextAvailableTaskByTag(tag)
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

    const isQueue = await isQueueIdValid(parseInt(queue))
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
 * Route for handling GET request to get the results using queue id
 */
router.get("/status/:queue", async (req, res) => {
  const queue = req.params.queue

  client = req.dbClient
  // VALIDATION
  try {
    if (!queue || isNaN(parseInt(queue))) {
      throw new QueueError("Missing queue or Invalid queue")
    }

    const isQueue = await isQueueIdValid(parseInt(queue))
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
      customLogger("error", red, `Unknown Error: ${err.message}`)
    }
  }
  // PROCESSING
  try {
    const { total_jobs, completed_count, error_count } = await getStatus(
      parseInt(queue)
    )
    customLogger(
      "log",
      cyan,
      `${total_jobs}, ${completed_count}, ${error_count}`
    )

    return res.status(200).json({
      totalTasks: total_jobs,
      completedTasks: completed_count,
      errorTasks: error_count,
    })
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
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
