const { performance } = require("perf_hooks")
const format = require("pg-format")
const { logger } = require("./utils")

class QueryManager {
  constructor(client) {
    this.client = client
  }

  async createQueueAndAddTasks(type, tasks, tags, options) {
    let queue, numTasks

    try {
      queue = await this.createQueue(type, tags, options)

      if (queue) {
        await this.client.query("BEGIN")
        numTasks = await this.addTasks(queue, tasks, options)
        await this.client.query("COMMIT")
      } else {
        logger.error("createQueue operation did not return a valid queue.")
      }
      return { queue, numTasks }
    } catch (err) {
      await this.deleteQueue(queue)
      await this.client.query("ROLLBACK")
      logger.error(`error in 'createQueueAndAddTasks': ${err.message}`)
    }
  }

  async createQueue(type, tags, options) {
    let queue = null
    let tagsArray = null
    if (Array.isArray(tags) && tags.length > 0)
      tagsArray = tags.map(tag => `${tag}`)
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

      queue = await this.client.query(queryStr, queryParams)
      return queue.rows[0].id
    } catch (err) {
      logger.error(`error in 'createQueue': ${err.message}`)
    }
  }

  async addTasks(queue, tasks, options) {
    try {
      const expiryTime = new Date()
      expiryTime.setTime(
        expiryTime.getTime() + (options?.expiryTime ?? 60 * 1000) // 15 seconds
      )

      const batchSize = 4096
      const totalEntries = Object.entries(tasks)
      const totalBatches = Math.ceil(totalEntries.length / batchSize)

      let totalTimeM, endTimeM, startTimeM
      startTimeM = performance.now()
      let successfulBatches = 0

      await this.client.query("BEGIN")
      for (let i = 0; i < totalBatches; i++) {
        const batchStart = i * batchSize
        const batchEnd = (i + 1) * batchSize
        const batch = totalEntries
          .slice(batchStart, batchEnd)
          .map(([id, data]) => {
            return [data.taskId, data.params, data.priority, expiryTime, queue]
          })
        try {
          await this.addTasksByBatch(batch)

          await this.client.query("COMMIT")

          successfulBatches++
        } catch (err) {
          await this.client.query("ROLLBACK")
          logger.error(`Error adding batch ${i + 1}: ${err.message}`)
          return
        }

        // perf
        endTimeM = performance.now()
        totalTimeM = (endTimeM - startTimeM) / 1000
      }
      logger.info(
        `Total tasks: ${totalEntries.length}, Total batches: ${totalBatches}, Batch size: ${batchSize}`
      )
      logger.info(`Total-time-taken: ${totalTimeM.toFixed(3)} seconds`)

      if (successfulBatches === totalBatches) return totalEntries.length
      else return 0
    } catch (err) {
      await this.deleteQueue(queue)
      logger.error(`error in 'addTasks': ${err.message}`)
      return
    }
  }

  async addTasksByBatch(batch) {
    try {
      const queryStr = `
      INSERT INTO tasks (task_id, params, priority , expiry_time, queue_id) 
      VALUES %L
      `
      await this.client.query(format(queryStr, batch))
    } catch (err) {
      logger.error(`error in 'addTasksByBatch': ${err.message}`)
    }
  }

  async deleteEverything() {
    try {
      const queryStr = `
      TRUNCATE TABLE tasks, queues RESTART IDENTITY
        `
      await this.client.query(queryStr)
      logger.log("Deleted everything successfully")
    } catch (err) {
      logger.error(`error in 'deleteEverything': ${err.message}`)
    }
  }

  async deleteQueue(queue) {
    try {
      const queryStr = `
        DELETE FROM queues
        WHERE id = ${queue};
        `
      await this.client.query(queryStr)
    } catch (err) {
      logger.error(`error in 'deleteQueue': ${err.message}`)
    }
  }

  async getNextAvailableTaskByQueue(queue) {
    let data = null
    try {
      await this.client.query("BEGIN")

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

      const result = await this.client.query(mainQuery)

      data = result.rows[0]
      console.log(data)
      if (!data) {
        logger.info("No tasks available right now!")
      } else if (data) {
        const mainQuery = format(
          `
        UPDATE tasks
        SET status = "processing", start_time = CURRENT_TIMESTAMP
        WHERE id = %L;
         `,
          data.id
        )

        await this.client.query(mainQuery)
      }
      await this.client.query("COMMIT")
    } catch (err) {
      await this.client.query("ROLLBACK")
      logger.error(`Error in getNextTaskByQueue: ${err.message}`)
    }
    return data
  }

  async getNextAvailableTaskByType(type) {
    let data = null
    try {
      await this.client.query("BEGIN")

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

      const result = await this.client.query(mainQuery)

      data = result.rows[0]
      if (!data) {
        logger.warn("No tasks available right now!")
      } else {
        const mainQuery = `
          UPDATE tasks SET status = 'processing', start_time =  CURRENT_TIMESTAMP
          WHERE id = ${data.id};
          `
        await this.client.query(mainQuery)

        await this.client.query("COMMIT")
      }
    } catch (err) {
      await this.client.query("ROLLBACK")
      logger.error(`Error in getNextTaskByType: ${err.message}`)
    }

    return data
  }

  async getNextAvailableTaskByTags(tags) {
    let data = null
    try {
      let tagsArray = Array.isArray(tags) ? tags : JSON.parse(tags)
      const tagsCondition = tagsArray.map(tag => tag)

      await this.client.query("BEGIN")

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

      const result = await this.client.query(mainQuery)

      data = result.rows[0]
      if (!data) {
        logger.info("No tasks available right now!")
      } else if (data) {
        const mainQuery = `
          UPDATE tasks SET status = 'processing', start_time =  CURRENT_TIMESTAMP
          WHERE id = ${data.id};
          `
        await this.client.query(mainQuery)

        await this.client.query("COMMIT")
      }
    } catch (err) {
      await this.client.query("ROLLBACK")
      logger.error(`Error in getNextTaskByTags: ${err.message}`)
    }
    return data
  }

  async submitResults({ id, result, error }) {
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

      await this.client.query("BEGIN")

      const response = await this.client.query(mainQuery, [
        error,
        JSON.stringify(resultObj),
        id,
      ])

      const queue = response.rows[0].queue_id
      const callbackUrl = response.rows[0].callback_url
      if (await this.areAllTasksCompleted(queue)) {
        logger.log("All Tasks Finished")
        if (callbackUrl) {
          await this.postResults(callbackUrl, await this.getResults(queue))
        }
      }

      await this.client.query("COMMIT")
    } catch (err) {
      await this.client.query("ROLLBACK")
      logger.error(`error in 'submitResults' : ${err.message}`)
    }
  }

  async getStatus(queue) {
    try {
      const mainQuery = `
        SELECT 
            COUNT(task_id) AS total_jobs,
            SUM(CASE WHEN status = 'completed' OR status = 'error' THEN 1 ELSE 0 END) AS completed_count,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count
        FROM tasks
        WHERE queue_id = ${queue} ;
         `
      const response = await this.client.query(mainQuery)

      return response.rows[0]
    } catch (err) {
      logger.error(`error in 'getStatus': ${err.message}`)
    }
  }

  async getResults(queue) {
    try {
      const mainQuery = `
        SELECT task_id, result
        FROM tasks
        WHERE status IN ('completed', 'error') 
          AND queue_id = ${queue};
         `
      const response = await this.client.query(mainQuery)
      const results = {}
      response.rows.forEach(row => {
        results[row.task_id] = row.result
      })

      return { results }
    } catch (err) {
      logger.error(`error in 'getResults': ${err.message}`)
    }
  }

  async postResults(url, results) {
    try {
      await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(results),
      })
    } catch (err) {
      logger.error(`error in 'postResults': ${err.message}`)
    }
  }

  async areAllTasksCompleted(queue) {
    let areCompleted = false
    let totalTasks, completedTasks
    if (queue) {
      totalTasks = await this.totalTaskCountInQueue(queue)
      completedTasks = await this.completedTaskCountInQueue(queue)
    }
    if (totalTasks.rows[0].count === completedTasks.rows[0].count) {
      areCompleted = true
    }
    return areCompleted
  }

  async totalTaskCountInQueue(queue) {
    const mainQuery = `
        SELECT COUNT(*) FROM tasks 
        WHERE queue_id = ${queue}
        `
    const response = await this.client.query(mainQuery)
    return response
  }

  async completedTaskCountInQueue(queue) {
    const mainQuery = `
        SELECT COUNT(*) FROM tasks 
        WHERE queue_id = ${queue} 
          AND status IN ('completed', 'error')
        `
    const response = await this.client.query(mainQuery)
    return response
  }
}

module.exports = {
  QueryManager,
}
