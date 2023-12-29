const routes = require("express").Router()
const {
  validateNonEmptyRequestBody,
  validateQueueType,
  validateQueueId,
  validateOptions,
  validateTasks,
} = require("./validations")
const { ValidationError } = require("./error")
const { HTTP_NO_CONTENT, HTTP_OK } = require("./constants")
const { logger } = require("./utils")

require("dotenv").config()

routes.post("/create-queue", async (req, res, next) => {
  logger.info(
    `Incoming client request for 'create-queue' with requestId ${req.requestId}`
  )

  let requestBody

  try {
    validateNonEmptyRequestBody(req)
    requestBody = req.body
    const { type, tasks, options, tags } = requestBody

    if (!type) throw new ValidationError("type is required.")
    validateQueueType(type)

    if (!tasks) throw new ValidationError("tasks are required.")
    validateTasks(tasks)

    if (options) validateOptions(options)
  } catch (err) {
    return next(err)
  }

  try {
    const { type, tasks, options, tags } = requestBody

    const { queue, numTasks } = await req.queryManager.createQueueAndAddTasks(
      type,
      tasks,
      tags,
      options
    )

    const result = { queue, numTasks }

    return res.status(HTTP_OK).json(result)
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

routes.post("/add-tasks", async (req, res, next) => {
  logger.info(
    `Incoming client request for 'add-tasks' with requestId ${req.requestId}`
  )

  let requestBody

  try {
    validateNonEmptyRequestBody(req)

    requestBody = req.body

    const { queue, tasks, options } = requestBody

    if (!queue) throw new ValidationError("queue is required.")
    await validateQueueId(queue, req.queryManager)

    if (!tasks) throw new ValidationError("tasks are required.")
    validateTasks(tasks)

    if (options) validateOptions(options)
  } catch (err) {
    return next(err)
  }

  try {
    const { queue, tasks, options } = requestBody

    const numTasks = await req.queryManager.addTasks(queue, tasks, options)

    return res.json({ numTasks })
  } catch (err) {
    console.log("add-tasks-error: ", err.message)
    return next(err)
  } finally {
    return next()
  }
})

routes.post("/get-next-available-task", async (req, res, next) => {
  logger.info(
    `Incoming worker request for 'get-next-available-task' with requestId ${req.requestId}`
  )
  let requestBody

  try {
    validateNonEmptyRequestBody(req)

    requestBody = req.body

    const { queue, type, tags } = requestBody

    if (!queue && !type && !tags)
      throw new ValidationError("either queue, type or tags must be specified")

    if (queue) validateQueueId(queue)

    if (type) validateQueueType(type)
  } catch (err) {
    return next(err)
  }

  try {
    const { queue, type, tags } = requestBody
    let nextAvailableTask

    if (queue)
      nextAvailableTask = await req.queryManager.getNextAvailableTaskByQueue(
        queue
      )
    else if (type)
      nextAvailableTask = await req.queryManager.getNextAvailableTaskByType(
        type
      )
    else if (tags)
      nextAvailableTask = await req.queryManager.getNextAvailableTaskByTags(
        tags
      )

    if (!nextAvailableTask)
      return res.status(HTTP_NO_CONTENT).json({
        message: "No available task found",
      })

    return res.status(HTTP_OK).json({
      id: nextAvailableTask.id,
      params: nextAvailableTask.params,
      type: nextAvailableTask.queue_type,
    })
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

routes.post("/submit-results", async (req, res, next) => {
  logger.info(
    `Incoming worker request for 'submit-results' with requestId ${req.requestId}`
  )
  try {
    const { id, result, error } = req.body

    await req.queryManager.submitResults({ id, result, error })

    return res.sendStatus(HTTP_OK)
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

routes.get("/get-results/:queue", async (req, res, next) => {
  logger.info(
    `Incoming client request for 'get-results' with requestId ${req.requestId}`
  )
  let queue

  try {
    if (!req.params.queue) throw new ValidationError("missing queue")

    queue = req.params.queue

    validateQueueId(queue, req.queryManager)
  } catch (err) {
    return next(err)
  }

  try {
    const response = await req.queryManager.getResults(queue)

    if (Object.keys(response.results).length === 0)
      return res
        .status(HTTP_NO_CONTENT)
        .json({ message: "No completed tasks found" })
    else return res.status(HTTP_OK).json(response)
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

routes.get("/status/:queue", async (req, res, next) => {
  logger.info(
    `Incoming client request for 'status' with requestId ${req.requestId}`
  )
  let queue

  try {
    if (!req.params.queue) throw new ValidationError("missing queue")

    queue = req.params.queue

    validateQueueId(queue, req.queryManager)
  } catch (err) {
    return next(err)
  }

  try {
    const { total_jobs, completed_count, error_count } =
      await req.queryManager.getStatus(parseInt(queue, 10))

    return res.status(HTTP_OK).json({
      totalTasks: total_jobs,
      completedTasks: completed_count,
      errorTasks: error_count,
    })
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

routes.post("/delete-queue/:queue", async (req, res, next) => {
  logger.info(
    `Incoming client request for 'delete-queue' with requestId ${req.requestId}`
  )
  let queue
  try {
    if (!req.params.queue) throw new ValidationError("missing queue")

    queue = req.params.queue

    validateQueueId(queue, req.queryManager)
  } catch (err) {
    return next(err)
  }

  try {
    await req.queryManager.deleteQueue(queue)

    return res.sendStatus(HTTP_OK)
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

routes.delete("/delete-everything", async (req, res, next) => {
  logger.info(
    `Incoming client request for 'delete-everything' with requestId ${req.requestId}`
  )
  try {
    await req.queryManager.deleteEverything()
    return res.sendStatus(HTTP_OK)
  } catch (err) {
    return next(err)
  } finally {
    return next()
  }
})

module.exports = routes
