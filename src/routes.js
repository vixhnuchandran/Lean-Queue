const router = require("express").Router()
const { logError, red, green, yellow, blue, magenta, cyan } = require("./utils")
const Services = require("./services")
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
 * Route  for handling POST request to create queue ✅
 */
router.post("/create-queue", async (req, res) => {
  const { type, tasks, options = null } = req.body

  // VALIDATION
  try {
    if (!type) {
      throw new QueueError("Missing type")
    } else if (!tasks) {
      throw new QueueError("Missing tasks")
    } else if (!areQueueParametersValid(type, tasks, options)) {
      throw new ValidationError("Invalid tasks")
    }
  } catch (error) {
    if (err instanceof ValidationError) {
      console.log("ValidationError: Invalid queue parameters")
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      console.log("QueueError")
      return res.status(400).json({ error: err.message })
    } else {
      console.log("Unknown Error: ", err.message)
    }
  }

  // PROCESSING
  try {
    const { queue, numTasks } = await Services.createQueueAndAddTasks(
      type,
      options,
      tasks
    )
    return res.json({ queue, numTasks })
  } catch (err) {
    console.log("Unknown Error: ", err.message)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  }
})

/**
 * Route  for handling POST request to add task ✅
 */
router.post("/add-tasks", async (req, res) => {
  const { queue, tasks, options } = req.body

  // VALIDATION
  try {
    if (!queue) {
      throw new QueueError("Missing queue")
    } else if (!tasks) {
      throw new QueueError("Missing tasks")
    } else if (queue && !isQueueIdValid(queue)) {
      throw new ValidationError("Invalid queue")
    } else if (tasks && !areAllTasksValid(tasks)) {
      throw new ValidationError("Invalid tasks")
    }
  } catch (err) {
    if (err instanceof ValidationError) {
      console.log("ValidationError: Invalid queue parameters")
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      console.log("QueueError")
      return res.status(400).json({ error: err.message })
    } else {
      console.log("Unknown Error:", err)
    }
  }

  // PROCESSING
  try {
    const numTasks = await Services.addTasks(queue, tasks, options)
    return res.json({ numTasks })
  } catch (err) {
    console.log("Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  }
})

/**
 * Route  for handling POST requestto get the next task
 */
router.post("/get-next-available-task", async (req, res) => {
  const { queue, type } = req.body

  try {
    if (!queue && !type) {
      throw new QueueError("Either queue or type must be specified")
    } else if (queue && isQueueIdValid) {
      throw new ValidationError("Invalid queueId")
    } else if (type && !isQueueTypeValid) {
      throw new ValidationError("Invalid queueType")
    }
  } catch {
    if (err instanceof ValidationError) {
      console.log("ValidationError: Invalid queue parameters")
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      console.log("QueueError")
      return res.status(400).json({ error: err.message })
    } else {
      console.log("Unknown Error:", err)
    }
  }

  try {
    let nextAvailableTask
    if (queue) {
      nextAvailableTask = await Services.getNextAvailableTaskByQueue(queue)
    } else if (type) {
      nextAvailableTask = await Services.getNextAvailableTaskByType(type)
    }
    return res.status(200).json({
      id: nextAvailableTask.id,
      params: nextAvailableTask.params,
    })
  } catch (err) {
    console.log("Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  }
})

/**
 * Route for handling GET request to get the results using queue id
 */
router.get("/get-results/:queue", async (req, res) => {
  const queue = req.params.queue

  try {
    // check queue is valid
    // check queue in db
  } catch (err) {
    if (err instanceof ValidationError) {
      console.log("ValidationError: Invalid queue parameters")
      return res.status(400).json({ error: err.message })
    } else if (err instanceof QueueError) {
      console.log("QueueError")
      return res.status(400).json({ error: err.message })
    } else {
      console.log("Unknown Error:", err)
    }
  }

  try {
    const response = await Services.getResults(queue)
    console.log(response)
    if (Object.keys(response.results).length === 0) {
      return res.status(400).json({ message: "No completed tasks found" })
    } else {
      return res.status(200).json(response)
    }
  } catch (err) {
    console.log("Unknown Error:", err)
    return res.status(500).json({ error: INTERNAL_SERVER_ERROR })
  }
})

/**
 * Route  for handling POST request to submit the results
 */
router.post("/submit-results", async (req, res) => {
  const { id, result, error } = req.body

  try {
    const response = await Services.submitResults({ id, result, error })
    console.log(response)
    if (response) {
      console.log("All jobs finished")
    }
    res.send({ ok: true })
    return
  } catch (err) {
    console.log(red("Error in route for submit-results: ", err))
    res.status(500).send({ ok: false, error: "Internal Server Error" })
    return
  }
})

module.exports = router

// add get esults ebndpoiunt tpo dend result

// ma esqkl  file and useeit in iosde
