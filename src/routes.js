const router = require("express").Router()
const { colorize } = require("./utils")
const {
  createQueues,
  addTasks,
  getNextTasks,
  submitResults,
} = require("./services")
require("dotenv").config()

/**
 * Route  for handling POST request to create queue ✅
 */
router.post("/create-queue", async (req, res) => {
  const { type, tasks } = req.body

  try {
    if (!type) {
      return res.status(400).json({ error: `type not specified` })
    }
    if (!tasks) {
      return res.status(400).json({ error: `tasks not specified` })
    }

    const id = await createQueues(type)
    const numTasks = await addTasks(id, tasks)

    return res.json({ queue: id, numTasks })
  } catch (error) {
    console.error(
      colorize(`Error in route for create-queue: ${error.message}`, "red")
    )
    return res.status(500).json({ error: "Internal server error" })
  }
})

/**
 * Route  for handling POST request to add task ✅
 */
router.post("/add-tasks", async (req, res) => {
  const { queue, tasks } = req.body
  try {
    if (!queue) {
      res.status(400).json({ error: "Bad Request: Missing queue" })
    }
    if (!tasks) {
      res.status(400).json({ error: "Bad Request: Missing tasks" })
    }
    const numTasks = await addTasks(queue, tasks)
    return res.json({ numTasks })
  } catch (error) {
    console.error(
      colorize(`Error in route for add-tasks: ${error.message}`, "red")
    )
    res.status(500).json({ error: "Internal server error" })
    return
  }
})

/**
 * Route  for handling POST request to get the next task ✅
 */
router.post("/get-available-tasks", async (req, res) => {
  const queue = req.body

  if (!queue) {
    res.status(400).json({ error: "Bad Request: Missing queue" })
  }

  try {
    const task = await getNextTasks(queue)
    if (!task) {
      return res.send(404)
    }
    return res.status(200).json({
      id: task.id,
      params: task.params,
    })
  } catch (error) {
    console.error(
      colorize("Error in route for get-available-tasks: ", error.message),
      "red"
    )
    return res.status(500).json({
      error: error.message,
    })
  }
})

/**
 * Route  for handling POST request to submit the results ✅
 */
router.post("/submit-results", async (req, res) => {
  const { id, result, error } = req.body

  try {
    const response = await submitResults({ id, result, error })

    if (response) {
      console.log("All jobs finished")
    }
    res.send({ ok: true })
    return
  } catch (error) {
    console.error(
      colorize("Error in route for get-available-tasks: ", error.stack),
      "red"
    )
    res.status(500).send({ ok: false, error: "Internal Server Error" })
    return
  }
})

module.exports = router
