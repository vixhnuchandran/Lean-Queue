const router = require("express").Router()
const { customLogger, red, cyan } = require("./utils")
const queueFunctions = require("./queueFunctions")
const taskFunctions = require("./taskFunctions")
const validations = require("./validations")
const { QueueError, ValidationError } = require("./error")
require("dotenv").config()

const INTERNAL_SERVER_ERROR = "Internal server error"

router.post("/create-queue", async (req, res) => {
  const { type, tasks, options = null, tags, priority = 5 } = req.body
  client = req.dbClient
  console.log(req.body)
  // validations
  try {
    if (!req.body) {
      throw new QueueError("Missing queue")
    }
    if (!type) {
      throw new QueueError("Missing type")
    }
    if (!tasks) {
      throw new QueueError("Missing tasks")
    }
    if (type && !validations.isQueueTypeValid(type)) {
      throw new ValidationError("Invalid tasks")
    }
    if (tasks && !validations.areAllTasksValid(tasks)) {
      throw new ValidationError("Invalid tasks")
    }
    if (options && !validations.areOptionsValid(options)) {
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
      customLogger("error", red, `Unknown Error: ${err.message}`)
    }
  }
  // processing
  try {
    const { queue, numTasks } = await queueFunctions.createQueueAndAddTasks(
      type,
      tags,
      options,
      tasks,
      priority
    )
    return res.status(200).json({ queue, numTasks })
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

router.post("/add-tasks", async (req, res) => {
  const { queue, tasks, options = null, tags, priority = 5 } = req.body
  client = req.dbClient

  // validation
  try {
    const queueExists = await validations.doesQueueExist(queue)

    if (!queue) {
      throw new QueueError("Missing queue")
    }
    if (!tasks) {
      throw new QueueError("Missing tasks")
    }
    if (queue && !validations.isQueueIdValid(queue)) {
      throw new ValidationError("Invalid queue")
    }
    if (!queueExists) {
      throw new ValidationError("Invalid queue")
    }
    if (tasks && !validations.areAllTasksValid(tasks)) {
      throw new ValidationError("Invalid tasks")
    }
    if (options && !validations.areOptionsValid(options)) {
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
      customLogger("error", red, `Unknown Error: ${err.message}`)
    }
  }
  // processing
  try {
    const numTasks = await queueFunctions.addTasks(
      queue,
      tasks,
      priority,
      options
    )
    return res.json({ numTasks })
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

router.post("/get-next-available-task", async (req, res) => {
  const { queue, type, tags = null, priority = null } = req.body

  client = req.dbClient
  // validation
  try {
    if (!queue && !type && !tags && !priority) {
      throw new QueueError("Either queue, type or tags must be specified")
    } else if (queue && !validations.isQueueIdValid(queue)) {
      throw new ValidationError("Invalid queueId")
    } else if (type && !validations.isQueueTypeValid(type)) {
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
      customLogger("error", red, `Unknown Error: ${err.message}`)
    }
  }
  // processing
  try {
    let nextAvailableTask
    if (queue) {
      nextAvailableTask = await taskFunctions.getNextAvailableTaskByQueue(
        queue,
        priority
      )
    } else if (type) {
      nextAvailableTask = await taskFunctions.getNextAvailableTaskByType(
        type,
        priority
      )
    } else if (tags) {
      nextAvailableTask = await taskFunctions.getNextAvailableTaskByTags(
        tags,
        priority
      )
    } else if (!queue && !type && !tags && priority) {
      nextAvailableTask = await taskFunctions.getNextAvailableTaskByPriority(
        priority
      )
    }

    if (!nextAvailableTask) {
      return res.status(400).json({
        message: "No available task found",
      })
    }
    return res.status(200).json({
      id: nextAvailableTask.id,
      params: nextAvailableTask.params,
      type: nextAvailableTask.queue_type,
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

router.post("/submit-results", async (req, res) => {
  const { id, result, error = null } = req.body

  client = req.dbClient
  try {
    await taskFunctions.submitResults({ id, result, error })
    res.send({ ok: true })
    return
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

router.get("/get-results/:queue", async (req, res) => {
  const queue = req.params.queue
  client = req.dbClient
  // VALIDATION
  try {
    if (!queue || isNaN(parseInt(queue))) {
      throw new QueueError("Missing queue or Invalid queue")
    }

    const isQueue = validations.isQueueIdValid(parseInt(queue))
    if (queue && !isQueue && !(await validations.doesQueueExist(queue))) {
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
    const response = await taskFunctions.getResults(queue)
    if (Object.keys(response.results).length === 0) {
      return res.status(400).json({ message: "No completed tasks found" })
    } else {
      return res.status(200).json(response)
    }
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) {
      req.dbClient.release()
    }
  }
})

router.get("/status/:queue", async (req, res) => {
  const queue = req.params.queue

  client = req.dbClient
  // VALIDATION
  try {
    if (!queue || isNaN(parseInt(queue))) {
      throw new QueueError("Missing queue or Invalid queue")
    }

    const isQueue = await validations.isQueueIdValid(parseInt(queue))
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
    const { total_jobs, completed_count, error_count } =
      await taskFunctions.getStatus(parseInt(queue))

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

module.exports = router
