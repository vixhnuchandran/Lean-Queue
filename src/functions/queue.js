const { performance } = require("perf_hooks")
const format = require("pg-format")
const { red, green, yellow, customLogger } = require("../utils")

const createQueueAndAddTasks = async (type, tasks, tags, options) => {
  let queue, numTasks

  try {
    let queue = await createQueue(type, tags, options)

    if (queue) {
      await client.query("BEGIN")
      numTasks = await addTasks(queue, tasks, options)
      await client.query("COMMIT")
    } else {
      customLogger(
        "error",
        red,
        "createQueue operation did not return a valid queue."
      )
    }
    return { queue, numTasks }
  } catch (err) {
    await deleteQueue(queue)
    await client.query("ROLLBACK")
    customLogger(
      "error",
      red,
      `Error in createQueueAndAddTasks: ${err.message}`
    )
  }
}

const createQueue = async (type, tags, options) => {
  let queue = null
  let tagsArray = null
  if (Array.isArray(tags) && tags.length > 0) {
    tagsArray = tags.map(tag => `${tag}`)
  }

  try {
    const queryStr = `
    INSERT INTO queues (type, tags, options) 
    VALUES ($1, $2,  $3)
    RETURNING id;`
    const queryParams = [
      type,
      tagsArray,
      options !== null ? JSON.stringify(options) : null,
    ]

    queue = await client.query(queryStr, queryParams)
    return queue.rows[0].id
  } catch (err) {
    customLogger("error", red, `Error in createQueue: ${err.stack}`)
  }
}

const addTasks = async (queue, tasks, options) => {
  try {
    const expiryTime = new Date()
    expiryTime.setTime(
      expiryTime.getTime() + (options?.expiryTime ?? 2 * 60 * 1000) // 2 minutes
    )

    const batchSize = 4096
    const totalEntries = Object.entries(tasks)
    const totalBatches = Math.ceil(totalEntries.length / batchSize)

    let totalTimeM, endTimeM, startTimeM
    console.log(`Total Batches: ${totalBatches}`)
    startTimeM = performance.now()
    let successfulBatches = 0

    await client.query("BEGIN")
    for (let i = 0; i < totalBatches; i++) {
      const batchStart = i * batchSize
      const batchEnd = (i + 1) * batchSize
      const batch = totalEntries
        .slice(batchStart, batchEnd)
        .map(([id, data]) => {
          return [data.taskId, data.params, data.priority, expiryTime, queue]
        })
      try {
        await addTasksByBatch(batch)

        successfulBatches++
      } catch (err) {
        await client.query("ROLLBACK")
        customLogger("error", red, `Error adding batch ${i + 1}:`)
      }
      await client.query("COMMIT")
      // perf
      endTimeM = performance.now()
      totalTimeM = (endTimeM - startTimeM) / 1000
    }
    customLogger(
      "info",
      green,
      `Total-time-taken: ${totalTimeM.toFixed(3)} seconds`
    )
    customLogger(
      "info",
      yellow,
      `Total tasks: ${totalEntries.length}, Total batches: ${totalBatches}, Batch size: ${batchSize}`
    )
    if (successfulBatches === totalBatches) {
      return totalEntries.length
    } else {
      return 0
    }
  } catch (err) {
    await deleteQueue(queue)
    customLogger("error", red, `Error in addTasks: ${err.message}`)
  }
}

const addTasksByBatch = async batch => {
  try {
    const queryStr = `
    INSERT INTO tasks (task_id, params, priority , expiry_time, queue_id) 
    VALUES %L
    `
    await client.query(format(queryStr, batch))
  } catch (err) {
    customLogger("error", red, `Error in addTasksByBatch: ${err.stack}`)
  }
}

const deleteQueue = async queue => {
  try {
    const queryStr = `
      DELETE FROM queues
      WHERE id = ${queue} ;
      `
    await client.query(queryStr)
  } catch (err) {
    customLogger("error", red, `Error in createQueue: ${err.stack}`)
  }
}

module.exports = {
  createQueueAndAddTasks,
  addTasks,
  deleteQueue,
}
