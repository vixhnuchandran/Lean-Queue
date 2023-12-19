const { performance } = require("perf_hooks")
const format = require("pg-format")
const { red, customLogger, green, yellow } = require("./utils")

const createQueue = async (type, options = null) => {
  let queue = null
  try {
    const queryStr = `
    INSERT INTO queues (type, options) 
    VALUES ('${type}', '${JSON.stringify(options)}') 
    RETURNING id;
    `
    const result = await client.query(queryStr)
    queue = result.rows[0].id
    return queue
  } catch (err) {
    customLogger("error", red, `Error in createQueue: ${err.stack}`)
  }
}
const deleteQueue = async queue => {
  try {
    const queryStr = `
    DELETE FROM queues
    WHERE id = ${queue} ;
    `
    const result = await client.query(queryStr)
    queue = result.rows[0].id
    return queue
  } catch (err) {
    customLogger("error", red, `Error in createQueue: ${err.stack}`)
  }
}
const addTasks = async (queue, tasks, options, tags) => {
  try {
    const expiryTime = new Date()
    expiryTime.setTime(
      expiryTime.getTime() + (options?.expiryTime ?? 2 * 60 * 1000) // 2 minutes
    )

    const batchSize = 32768
    const totalEntries = Object.entries(tasks)
    const totalBatches = Math.ceil(totalEntries.length / batchSize)

    let totalTimeM, endTimeM, startTimeM
    console.log(`Total Batches: ${totalBatches}`)
    startTimeM = performance.now()
    for (let i = 0; i < totalBatches; i++) {
      const batchStart = i * batchSize
      const batchEnd = (i + 1) * batchSize
      const batch = totalEntries
        .slice(batchStart, batchEnd)
        .map(([taskId, params]) => {
          return [taskId, params, tags, expiryTime, queue]
        })

      await addTasksByBatch(batch)

      //perf
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
    return totalEntries.length
  } catch (err) {
    customLogger("error", red, `Error in addTasks: ${err.message}`)
  }
}
const addTasksByBatch = async batch => {
  try {
    const queryStr = `
    INSERT INTO tasks (task_id, params,tags, expiry_time, queue_id) 
    VALUES %L
    `
    await client.query(format(queryStr, batch))
  } catch (err) {
    customLogger("error", red, `Error in addTasksByBatch: ${err.stack}`)
  }
}
const createQueueAndAddTasks = async (type, options, tasks, tags) => {
  let queue, numTasks
  try {
    queue = await createQueue(type, options)
    if (queue) {
      await client.query("BEGIN")
      numTasks = await addTasks(queue, tasks, options, tags)
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

const getNextAvailableTaskByTag = async tag => {
  let data = null
  try {
    await client.query("BEGIN")
    const queryStr = `
    SELECT tasks.*
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.type = 'addition'
         AND (tasks.status = 'available' 
         OR (tasks.status = 'processing' 
         AND tasks.expiry_time < NOW()))
    ORDER BY (params->>'priority')::int DESC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;
    `
    const result = await client.query(queryStr)

    data = result.rows[0]
    if (!data) {
      customLogger("warn", yellow, "No tasks available right now!")
    } else if (data) {
      const queryStr2 = `
      UPDATE tasks SET status = 'processing', start_time =  CURRENT_TIMESTAMP
      WHERE id = ${data.id};
      `
      await client.query(queryStr2)

      await client.query("COMMIT")
    }
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `Error in getNextTaskByType: ${err.message}`)
  }
  return data
}

const getNextAvailableTaskByQueue = async queue => {
  let data = null
  try {
    await client.query("BEGIN")
    const queryStr = `
    SELECT tasks.*
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.id = '${queue}'
      AND (tasks.status = 'available' OR (tasks.status = 'processing' AND tasks.expiry_time < NOW()))
    
      ORDER BY (params->>'priority')::int DESC
      LIMIT 1
    FOR UPDATE SKIP LOCKED;
    `
    const result = await client.query(queryStr)
    data = result.rows[0]
    if (!data) {
      customLogger("warn", yellow, "No tasks available right now!")
    } else if (data) {
      const queryStr2 = `
      UPDATE tasks
      SET status = 'processing', start_time = CURRENT_TIMESTAMP
      WHERE id = ${data.id};
      `
      await client.query(queryStr2)
    }
    await client.query("COMMIT")
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `Error in getNextTaskByQueue: ${err.message}`)
  }
  return data
}

const getNextAvailableTaskByType = async type => {
  let data = null

  try {
    await client.query("BEGIN")

    const queryStr = `
    SELECT tasks.*
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.type = '${type}'
      AND (tasks.status = 'available' OR (tasks.status = 'processing' AND tasks.expiry_time < NOW()))
      ORDER BY (params->>'priority')::int DESC
      LIMIT 1
    FOR UPDATE SKIP LOCKED;
  `
    const result = await client.query(queryStr)

    data = result.rows[0]

    if (!data) {
      customLogger("warn", yellow, "No tasks available right now!")
    } else {
      const queryStr2 = `
      UPDATE tasks SET status = 'processing', start_time =  CURRENT_TIMESTAMP
      WHERE id = ${data.id};
      `
      await client.query(queryStr2)

      await client.query("COMMIT")
    }
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `Error in getNextTaskByType: ${err.message}`)
  }

  return data
}

const submitResults = async ({ id, result, error }) => {
  try {
    const resultObj = error ? { error } : { result }
    const queryStr = `
      UPDATE tasks 
      SET 
        status = CASE
          WHEN $1::text IS NOT NULL THEN 'error'::task_status
          ELSE 'completed'::task_status
        END,
        end_time = NOW(),
        result = $2::jsonb
      FROM queues
      WHERE tasks.id = $3 AND queues.id = tasks.queue_id
      RETURNING tasks.queue_id, queues.options->>'callback' AS callback_url;
    `

    const response = await client.query(queryStr, [
      error,
      JSON.stringify(resultObj),
      id,
    ])

    const queue = response.rows[0].queue_id
    const callbackUrl = response.rows[0].callback_url
    if (await allTasksCompleted(queue)) {
      customLogger("log", green, "All Tasks Finished")
      if (callbackUrl) {
        await postResults(callbackUrl, await getResults(queue))
      }
    }
  } catch (err) {
    customLogger(
      "error",
      red,
      `Error in const submitResults = async ({ : ${err.stack}`
    )
  }
}

const getStatus = async queue => {
  try {
    const queryStr = `
    SELECT 
        COUNT(task_id) AS total_jobs,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count
    FROM tasks
    WHERE queue_id = ${queue} ;
     `
    const response = await client.query(queryStr)

    return response.rows[0]
  } catch (err) {
    customLogger("error", red, `Error in getStatus: ${err.stack}`)
  }
}

const getResults = async queue => {
  try {
    const queryStr = `
    SELECT task_id, result
    FROM tasks
    WHERE status IN ('completed', 'error') 
      AND queue_id = ${queue};
     `
    const response = await client.query(queryStr)
    const results = {}
    response.rows.forEach(row => {
      results[row.task_id] = row.result
    })

    return { results }
  } catch (err) {
    customLogger("error", red, `Error in getResults: ${err.stack}`)
  }
}

const postResults = async (url, results) => {
  try {
    await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(results),
    })
  } catch (err) {
    customLogger("error", red, `Error in postResults: ${err.stack}`)
  }
}

const totalTaskCountInQueue = async queue => {
  const queryStr = `
  SELECT COUNT(*) FROM tasks 
  WHERE queue_id = ${queue}
  `
  const response = await client.query(queryStr)
  return response
}

const completedTaskCountInQueue = async queue => {
  const queryStr = `
  SELECT COUNT(*) FROM tasks 
  WHERE queue_id = ${queue} 
    AND status IN ('completed', 'error')
  `
  const response = await client.query(queryStr)
  return response
}

const allTasksCompleted = async queue => {
  let areCompleted = false
  if (queue) {
    totalTasks = await totalTaskCountInQueue(queue)
    completedTasks = await completedTaskCountInQueue(queue)
  }
  if (totalTasks.rows[0].count === completedTasks.rows[0].count) {
    areCompleted = true
  }
  return areCompleted
}

module.exports = {
  addTasks,
  createQueueAndAddTasks,
  getNextAvailableTaskByTag,
  getNextAvailableTaskByQueue,
  getNextAvailableTaskByType,
  submitResults,
  getStatus,
  getResults,
  postResults,
  totalTaskCountInQueue,
  completedTaskCountInQueue,
  allTasksCompleted,
}
