const routes = require("express").Router()
const { customLogger, red } = require("./utils")
const {
  createQueueAndAddTasks,
  addTasks,
  deleteTasks,
  deleteQueue,
} = require("./functions/queue")
const {
  getNextAvailableTaskByType,
  getNextAvailableTaskByQueue,
  getNextAvailableTaskByTags,
  submitResults,
  getStatus,
  getResults,
} = require("./functions/task")
const {
  validateQueueType,
  validateQueueId,
  validateOptions,
  validateTasks,
} = require("./validations")
const { ValidationError, handleAppErrors } = require("./error")
const {
  INTERNAL_SERVER_ERROR,
  HTTP_OK,
  HTTP_BAD_REQUEST,
  HTTP_INTERNAL_SERVER_ERROR,
} = require("./constants")
require("dotenv").config()

routes.post("/create-queue", async (req, res) => {
  client = req.dbClient
  let requestBody
  try {
    if (!req.body) throw new ValidationError("empty request body")

    requestBody = req.body
    const { type, tasks, options, tags } = requestBody

    if (!type || !tasks)
      throw new ValidationError("type and tasks are required.")

    validateQueueType(type)

    validateTasks(tasks)

    if (options) validateOptions(options)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    const { type, tasks, options, tags } = requestBody

    const { queue, numTasks } = await createQueueAndAddTasks(
      type,
      tasks,
      tags,
      options
    )
    const result = { queue, numTasks }

    return res.status(HTTP_OK).json(result)
  } catch (err) {
    handleAppErrors(err, res)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.post("/add-tasks", async (req, res) => {
  client = req.dbClient
  let requestBody

  try {
    if (!req.body) throw new ValidationError("empty request body")

    requestBody = req.body
    const { type, tasks, options, tags } = requestBody

    if (!queue || !tasks)
      throw new ValidationError("queue and tasks are required.")

    validateQueueId(queue)

    validateTasks(tasks)

    if (options) validateOptions(options)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    const { type, tasks, options, tags } = requestBody

    const numTasks = await addTasks(queue, tasks, options)

    return res.json({ numTasks })
  } catch (err) {
    handleAppErrors(err, res)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.post("/get-next-available-task", async (req, res) => {
  client = req.dbClient
  let requestBody

  try {
    if (!req.body) throw new ValidationError("empty request body")
    requestBody = req.body
    const { queue, type, tags } = requestBody
    if (!queue && !type && !tags)
      throw new ValidationError("either queue, type or tags must be specified")

    if (queue) validateQueueId(queue)

    if (type) validateQueueType(type)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    const { queue, type, tags } = requestBody

    let nextAvailableTask
    if (queue) nextAvailableTask = await getNextAvailableTaskByQueue(queue)
    else if (type) nextAvailableTask = await getNextAvailableTaskByType(type)
    else if (tags) nextAvailableTask = await getNextAvailableTaskByTags(tags)

    if (!nextAvailableTask)
      return res.status(HTTP_BAD_REQUEST).json({
        message: "No available task found",
      })

    return res.status(HTTP_OK).json({
      id: nextAvailableTask.id,
      params: nextAvailableTask.params,
      type: nextAvailableTask.queue_type,
    })
  } catch (err) {
    handleAppErrors(err, res)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.post("/submit-results", async (req, res) => {
  client = req.dbClient

  try {
    const { id, result, error } = req.body

    await submitResults({ id, result, error })

    return res.sendStatus(HTTP_OK)
  } catch (err) {
    handleAppErrors(err, res)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.get("/get-results/:queue", async (req, res) => {
  client = req.dbClient
  let queue

  try {
    if (!req.params.queue) throw new ValidationError("missing queue")
    queue = req.params.queue

    validateQueueId(queue)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    const response = await getResults(queue)

    if (Object.keys(response.results).length === 0)
      return res
        .status(HTTP_BAD_REQUEST)
        .json({ message: "No completed tasks found" })
    else return res.status(HTTP_OK).json(response)
  } catch (err) {
    handleAppErrors(err, res)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

routes.get("/status/:queue", async (req, res) => {
  client = req.dbClient
  let queue

  try {
    if (!req.params.queue) throw new ValidationError("missing queue")

    queue = req.params.queue

    validateQueueId(queue)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    const { total_jobs, completed_count, error_count } = await getStatus(
      parseInt(queue)
    )

    return res.status(HTTP_OK).json({
      totalTasks: total_jobs,
      completedTasks: completed_count,
      errorTasks: error_count,
    })
  } catch (err) {
    handleAppErrors(err, res)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

// ✅
routes.post("/delete-everything/:queue", async (req, res) => {
  client = req.dbClient
  let queue

  try {
    if (!req.params.queue) throw new ValidationError("missing queue")

    queue = req.params.queue

    validateQueueId(queue)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    queue = req.params.queue

    await deleteTasks(queue)
    return res.sendStatus(HTTP_OK)
  } catch (err) {
    handleAppErrors(err, res)
    return res.sendStatus(HTTP_INTERNAL_SERVER_ERROR)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

// ✅
routes.post("/delete-queue/:queue", async (req, res) => {
  client = req.dbClient
  let queue
  try {
    if (!req.params.queue) throw new ValidationError("missing queue")

    queue = req.params.queue

    validateQueueId(queue)
  } catch (err) {
    handleAppErrors(err, res)
  }

  try {
    queue = req.params.queue

    await deleteQueue(queue, res)
    return res.sendStatus(HTTP_OK)
  } catch (err) {
    handleAppErrors(err, res)
    return res.sendStatus(HTTP_INTERNAL_SERVER_ERROR)
  } finally {
    if (req.dbClient) req.dbClient.release()
  }
})

module.exports = routes
