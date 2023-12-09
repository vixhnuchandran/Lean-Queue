const { colorize } = require("./utils")
const { client } = require("./database")
const { validateQueue, validateTasks } = require("./validations")

const createQueues = async type => {
  try {
    const isValid = validateQueue(type)
    if (!isValid) {
      throw new Error("Error type format")
    }
    const result = await client.query(
      "INSERT INTO queues (type) VALUES ($1) ON CONFLICT (type) DO UPDATE SET type = $1 RETURNING id;",
      [type]
    )
    return result.rows[0].id
  } catch (error) {
    console.error("Error in createQueues:", error.message)
    throw new Error("Failed to create or find the queue.")
  }
}

const addTasks = async (id, tasks) => {
  try {
    const isValid = validateTasks(tasks)
    console.log("failed Tasks: ", isValid)
    if (isValid.length !== 0) {
      throw new Error("Error task format")
    }
    await Promise.all(
      Object.entries(tasks).map(([taskid, params]) => {
        addTask(taskid, params, id)
      })
    )
    return Object.keys(tasks).length
  } catch (error) {
    console.error(`Error in addTasks: ${error.message}`)
    throw new Error("Failed to create tasks.")
  }
}

const addTask = async (taskid, params, queueId) => {
  try {
    await client.query(
      "INSERT INTO tasks (taskid, params, queueid) VALUES ($1, $2, $3)",
      [taskid, JSON.stringify(params), queueId]
    )
  } catch (error) {
    console.error(`Error in addTask: ${error.stack}`)
    throw new Error("Failed to add task to the queue.")
  }
}

const getNextTaskByQueue = async queue => {
  let data
  await client.query("BEGIN")
  try {
    const result = await client.query(
      "SELECT * FROM tasks WHERE queueid = $1  AND (status = $2 OR (status = $3 AND expiryTime < NOW())) LIMIT 1 FOR UPDATE SKIP LOCKED;",
      [queue.id, "available", "processing"]
    )

    data = result.rows[0]

    if (data) {
      const startTime = new Date()
      const expiryTime = new Date(startTime.getTime() + 15000) // + 15 seconds

      await client.query(
        "UPDATE tasks SET status = $1, startTime = $2, expiryTime = $3 WHERE id = $4",
        ["processing", startTime, expiryTime, data.id]
      )
    }

    await client.query("COMMIT")
  } catch (error) {
    await client.query("ROLLBACK")
    console.error(`Error in getNextTaskByQueue: ${error.message}`)
    throw new Error("Failed to get the next task by queue.")
  }

  return data
}

const getNextTaskByType = async type => {
  let data

  await client.query("BEGIN")

  try {
    const queueResult = await client.query(
      "SELECT id FROM queues WHERE type = $1 LIMIT 1",
      [type]
    )

    if (queueResult.rows.length > 0) {
      const queueId = queueResult.rows[0].id

      const result = await client.query(
        "SELECT * FROM tasks WHERE queueid = $1 AND (status = $2 OR (status = $3 AND expiryTime < NOW())) LIMIT 1 FOR UPDATE SKIP LOCKED;",
        [queueId, "available", "processing"]
      )

      data = result.rows[0]

      if (data) {
        const startTime = new Date()
        const expiryTime = new Date(startTime.getTime() + 2 * 60 * 1000) // + 2 minutes

        await client.query(
          "UPDATE tasks SET status = $1, startTime = $2, expiryTime = $3 WHERE id = $4",
          ["processing", startTime, expiryTime, data.id]
        )
      }
    }

    await client.query("COMMIT")
  } catch (error) {
    await client.query("ROLLBACK")
    console.error(`${colors.red}Error in getNextTaskByType: ${error.message}`)
    throw new Error("Failed to get the next task by type.")
  }

  return data
}

const getNextTasks = async ({ queue, type }) => {
  let data = null

  try {
    if (queue) {
      data = await getNextTaskByQueue(queue)
    }
    if (type) {
      data = await getNextTaskByType(type)
    }
    return data
  } catch (error) {
    console.log(colorize(`Error in getNextTasks: ${error.stack}`, "red"))
  }
}

const submitResults = async ({ id, result, error }) => {
  const endTime = new Date()
  let isFinished = false
  let total,
    finished,
    response = null
  try {
    if (error) {
      response = await client.query(
        "UPDATE tasks SET status = $1, endTime = $2, result = $3 WHERE id = $4 RETURNING queueid",
        ["error", endTime, { result: { sum: null, error } }, id]
      )
      return response
    } else if (result) {
      response = await client.query(
        "UPDATE tasks SET status = $1, endTime = $2, result = $3 WHERE id = $4 RETURNING queueid",
        ["completed", endTime, { result: { sum: result, error: null } }, id]
      )
      const queue = response.rows[0].queueid
      if (queue) {
        total = await totalTasksCount(queue)
        finished = await finishedTasksCount(queue)
      }

      if (
        JSON.stringify(total.rows[0].count) ===
        JSON.stringify(finished.rows[0].count)
      ) {
        isFinished = true
      }
      return isFinished
    }
  } catch (error) {
    console.error(colorize(`Error in submitResults: ${error.message}`, "red"))
    throw new Error(`Error in submitResults: ${error.message}`)
  }
}

const totalTasksCount = async queue => {
  const response = await client.query(
    "SELECT COUNT(*) FROM tasks WHERE queueid = $1",
    [queue]
  )
  return response
}

const finishedTasksCount = async queue => {
  const response = await client.query(
    "SELECT COUNT(*) FROM tasks WHERE queueid = $1 AND status IN ('completed', 'error')",
    [queue]
  )
  return response
}

module.exports = {
  createQueues,
  addTasks,
  getNextTasks,
  submitResults,
  totalTasksCount,
  finishedTasksCount,
}
