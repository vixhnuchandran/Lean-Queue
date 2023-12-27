const { red, customLogger, green, yellow } = require("../utils")
const format = require("pg-format")
const { executeQueriesWithDebug } = require("../debug")

let isDebugMode = false

const getNextAvailableTaskByQueue = async queue => {
  if (isDebugMode) {
    customLogger("info", yellow, "Using getNextAvailableTaskByQueue")
  }

  let data = null
  try {
    await client.query("BEGIN")
    const debugQuery = format(
      ` 
    SELECT tasks.*
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.id = %L`,
      queue
    )

    const mainQuery = format(
      `
    SELECT tasks.*, queues.type as queue_type
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.id = %L
      AND (tasks.status = 'available' OR 
           (tasks.status = 'processing' AND tasks.expiry_time < NOW()))
    ORDER BY (tasks.priority)::int DESC
    LIMIT 1;
  `,
      queue
    )

    const result = await executeQueriesWithDebug(
      isDebugMode,
      debugQuery,
      mainQuery
    )
    data = result.rows[0]
    console.log(data)
    if (!data) {
      customLogger("warn", yellow, "No tasks available right now!")
    } else if (data) {
      const mainQuery = format(
        `
      UPDATE tasks
      SET status = "processing", start_time = CURRENT_TIMESTAMP
      WHERE id = %L;
       `,
        data.id
      )

      await client.query(mainQuery)
    }
    await client.query("COMMIT")
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `Error in getNextTaskByQueue: ${err.message}`)
  }
  return data
}

const getNextAvailableTaskByType = async type => {
  if (isDebugMode) {
    customLogger("info", yellow, "Using getNextAvailableTaskByType")
  }
  let data = null
  try {
    await client.query("BEGIN")

    const debugQuery = format(
      ` 
    SELECT tasks.*
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.type = %L`,
      type
    )

    const mainQuery = format(
      `
      SELECT tasks.*
      FROM tasks
      JOIN queues ON tasks.queue_id = queues.id
      WHERE queues.type = %L
        AND (tasks.status = 'available' OR 
             (tasks.status = 'processing' AND tasks.expiry_time < NOW()))
      ORDER BY (tasks.priority)::int DESC
      LIMIT 1;`,
      type
    )

    const result = await executeQueriesWithDebug(
      isDebugMode,
      debugQuery,
      mainQuery
    )
    data = result.rows[0]
    if (!data) {
      customLogger("warn", yellow, "No tasks available right now!")
    } else {
      const mainQuery = `
        UPDATE tasks SET status = 'processing', start_time =  CURRENT_TIMESTAMP
        WHERE id = ${data.id};
        `
      await client.query(mainQuery)

      await client.query("COMMIT")
    }
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `Error in getNextTaskByType: ${err.message}`)
  }

  return data
}

const getNextAvailableTaskByTags = async tags => {
  if (isDebugMode) {
    customLogger("info", yellow, "Using getNextAvailableTaskByTags")
  }
  let data = null
  try {
    let tagsArray = Array.isArray(tags) ? tags : JSON.parse(tags)
    const tagsCondition = tagsArray.map(tag => tag)

    await client.query("BEGIN")

    const mainQuery = format(
      `
        SELECT tasks.*, queues.type as queue_type
        FROM tasks
        JOIN queues ON tasks.queue_id = queues.id
        WHERE queues.tags @> ARRAY[%L]::VARCHAR(255)[]
          AND (tasks.status = 'available' OR
               (tasks.status = 'processing' AND tasks.expiry_time < NOW()))
        ORDER BY (tasks.priority)::int DESC
        LIMIT 1;
      `,
      tagsCondition
    )

    const debugQuery = format(
      ` 
    SELECT tasks.*
    FROM tasks
    JOIN queues ON tasks.queue_id = queues.id
    WHERE queues.tags @> %L::VARCHAR(255)[]
    `,
      tagsCondition
    )
    const result = await executeQueriesWithDebug(
      isDebugMode,
      debugQuery,
      mainQuery
    )
    data = result.rows[0]
    if (!data) {
      customLogger("warn", yellow, "No tasks available right now!")
    } else if (data) {
      const mainQuery = `
        UPDATE tasks SET status = 'processing', start_time =  CURRENT_TIMESTAMP
        WHERE id = ${data.id};
        `
      await client.query(mainQuery)

      await client.query("COMMIT")
    }
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `Error in getNextTaskByTags: ${err.message}`)
  }
  return data
}

const submitResults = async ({ id, result, error }) => {
  try {
    const resultObj = error ? { error } : { result }
    const mainQuery = `
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

    await client.query("BEGIN")

    const response = await client.query(mainQuery, [
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

    await client.query("COMMIT")
  } catch (err) {
    await client.query("ROLLBACK")
    customLogger("error", red, `error in 'submitResults' : ${err.message}`)
  }
}

const getStatus = async queue => {
  try {
    const mainQuery = `
      SELECT 
          COUNT(task_id) AS total_jobs,
          SUM(CASE WHEN status = 'completed' OR status = 'error' THEN 1 ELSE 0 END) AS completed_count,
          SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count
      FROM tasks
      WHERE queue_id = ${queue} ;
       `
    const response = await client.query(mainQuery)

    return response.rows[0]
  } catch (err) {
    customLogger("error", red, `error in 'getStatus': ${err.message}`)
  }
}

const getResults = async queue => {
  try {
    const mainQuery = `
      SELECT task_id, result
      FROM tasks
      WHERE status IN ('completed', 'error') 
        AND queue_id = ${queue};
       `
    const response = await client.query(mainQuery)
    const results = {}
    response.rows.forEach(row => {
      results[row.task_id] = row.result
    })

    return { results }
  } catch (err) {
    customLogger("error", red, `error in 'getResults': ${err.message}`)
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
    customLogger("error", red, `error in 'postResults': ${err.message}`)
  }
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

const totalTaskCountInQueue = async queue => {
  const mainQuery = `
      SELECT COUNT(*) FROM tasks 
      WHERE queue_id = ${queue}
      `
  const response = await client.query(mainQuery)
  return response
}

const completedTaskCountInQueue = async queue => {
  const mainQuery = `
      SELECT COUNT(*) FROM tasks 
      WHERE queue_id = ${queue} 
        AND status IN ('completed', 'error')
      `
  const response = await client.query(mainQuery)
  return response
}

module.exports = {
  getNextAvailableTaskByType,
  getNextAvailableTaskByQueue,
  getNextAvailableTaskByTags,
  submitResults,
  getStatus,
  getResults,
  postResults,
}
