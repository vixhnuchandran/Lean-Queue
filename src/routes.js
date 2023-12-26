const routes = require("express").Router()
const { customLogger, red } = require("./utils")
const { createQueueAndAddTasks, addTasks } = require("./functions/queue")
const {
  getNextAvailableTaskByPriority,
  getNextAvailableTaskByType,
  getNextAvailableTaskByQueue,
  getNextAvailableTaskByTags,
  submitResults,
  getStatus,
  getResults,
} = require("./functions/task")
const {
  doesQueueExist,
  isQueueIdValid,
  isQueueTypeValid,
  areOptionsValid,
  areAllTasksValid,
} = require("./validations")
const { QueueError, ValidationError } = require("./error")
require("dotenv").config()

const INTERNAL_SERVER_ERROR = "Internal server error"

routes.post("/create-queue", async (req, res) => {
  const { type, tasks, options, tags } = req.body
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
    if (!isQueueTypeValid(type)) {
      throw new ValidationError("Invalid tasks")
    }
    if (!areAllTasksValid(tasks)) {
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
      customLogger("error", red, `Unknown Error: ${err.message}`)
    }
  }
  // processing
  try {
    const { queue, numTasks } = await createQueueAndAddTasks(
      type,
      tasks,
      tags,
      options
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

routes.post("/add-tasks", async (req, res) => {
  const { queue, tasks, options, tags, priority } = req.body
  client = req.dbClient

  // validation
  try {
    if (!queue) throw new QueueError("Missing queue")
    if (!tasks) throw new QueueError("Missing tasks")
    if (!isQueueIdValid(queue)) throw new ValidationError("Invalid queue")
    if (!(await doesQueueExist(queue)))
      throw new ValidationError("Invalid queue")
    if (!areAllTasksValid(tasks)) throw new ValidationError("Invalid tasks")
    if (options && !areOptionsValid(options))
      throw new ValidationError("Invalid options")
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
    const numTasks = await addTasks(queue, tasks, priority, options)
    return res.json({ numTasks })
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.post("/get-next-available-task", async (req, res) => {
  const { queue, type, tags, priority } = req.body
  client = req.dbClient

  // validation
  try {
    if (!queue && !type && !tags && !priority)
      throw new QueueError("Either queue, type or tags must be specified")
    if (queue && !isQueueIdValid(queue))
      throw new ValidationError("Invalid queueId")
    if (type && !isQueueTypeValid(type))
      throw new ValidationError("Invalid queueType")
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
    if (queue)
      nextAvailableTask = await getNextAvailableTaskByQueue(queue, priority)
    else if (type)
      nextAvailableTask = await getNextAvailableTaskByType(type, priority)
    else if (tags)
      nextAvailableTask = await getNextAvailableTaskByTags(tags, priority)
    else if (!queue && !type && !tags && priority)
      nextAvailableTask = await getNextAvailableTaskByPriority(priority)

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
    if (req.dbClient) req.dbClient.release()
  }
})

routes.post("/submit-results", async (req, res) => {
  const { id, result, error } = req.body
  client = req.dbClient

  try {
    await submitResults({ id, result, error })
    res.send({ ok: true })
    return
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.get("/get-results/:queue", async (req, res) => {
  const queue = req.params.queue
  client = req.dbClient
  // validations
  try {
    if (!queue || isNaN(queue)) {
      throw new QueueError("Missing queue or Invalid queue")
    }

    const isQueue = isQueueIdValid(parseInt(queue))
    if (!isQueue || !(await doesQueueExist(queue))) {
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
    const response = await getResults(queue)
    if (Object.keys(response.results).length === 0)
      return res.status(400).json({ message: "No completed tasks found" })
    else return res.status(200).json(response)
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.get("/status/:queue", async (req, res) => {
  const queue = req.params.queue
  client = req.dbClient

  // VALIDATION
  try {
    if (!queue || isNaN(queue))
      throw new QueueError("Missing queue or Invalid queue")
    if (!isQueueIdValid(parseInt(queue)))
      throw new ValidationError("Invalid queue")
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

    return res.status(200).json({
      totalTasks: total_jobs,
      completedTasks: completed_count,
      errorTasks: error_count,
    })
  } catch (err) {
    customLogger("error", red, `Unknown Error: ${err.message}`)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

module.exports = routes
