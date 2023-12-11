const { performance } = require("perf_hooks")
const format = require("pg-format")
const {
  customLogger,
  red,
  green,
  yellow,
  blue,
  magenta,
  cyan,
} = require("./utils")
const pool = require("./db")

class Services {
  constructor() {
    this.pool = pool
    this.client = null
  }
  // ✅
  async createQueueAndAddTasks(type, options, tasks) {
    try {
      this.client = this.pool

      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )
      await this.client.query("BEGIN")

      const queue = await this.createQueue(type, options)
      const numTasks = await this.addTasks(queue, tasks, options)

      await this.client.query("COMMIT")
      return { queue, numTasks }
    } catch (err) {
      await this.client.query("ROLLBACK")
      customLogger("error", red, err.message)
    }
  }

  // ✅
  async createQueue(type, options) {
    this.client = this.pool

    try {
      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )

      const result = await this.client.query(
        `INSERT INTO queues (type, options) 
      VALUES ($1,$2) 
      RETURNING id;`,
        [type, options]
      )
      const queue = result.rows[0].id
      return queue
    } catch (err) {
      customLogger("error", red, err.message)
    }
  }

  // ✅
  async addTasks(queue, tasks, options) {
    try {
      this.client = this.pool

      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )

      const expiryTime = new Date()
      expiryTime.setTime(expiryTime.getTime() + options.expiryTime)
      const batchSize = 5
      const totalEntries = Object.entries(tasks)
      const totalBatches = Math.ceil(totalEntries.length / batchSize)

      for (let i = 0; i < totalBatches; i++) {
        const batchStart = i * batchSize
        const batchEnd = (i + 1) * batchSize
        const batch = totalEntries
          .slice(batchStart, batchEnd)
          .map(([taskId, params]) => {
            return [taskId, params, expiryTime, queue]
          })

        await this.addTasksByBatch(batch, options)
      }
      return totalEntries.length
    } catch (err) {
      customLogger("error", red, err.message)
    }
  }

  // ✅
  async addTasksByBatch(batch) {
    customLogger(
      "info",
      cyan,
      `Total connections in the pool: ${this.pool.totalCount}`
    )

    this.client = this.pool

    try {
      await this.client.query(
        format(
          `INSERT INTO tasks (task_id, params, expiry_time, queue_id) VALUES %L`,
          batch
        )
      )
    } catch (err) {
      customLogger("error", red, `Error in addTasksByBatch: ${err.message}`)
    }
  }

  // ✅
  async getNextAvailableTaskByQueue(queue) {
    let data
    this.client = this.pool

    try {
      await this.client.query("BEGIN")

      const result = await this.client.query(
        `SELECT * FROM tasks 
      WHERE queue_id = ${queue.id}  AND 
        (status = "available" OR (status = "processing" AND expiry_time < NOW())) 
      LIMIT 1 
      FOR UPDATE SKIP LOCKED;`
      )
      data = result.rows[0]
      if (!data) {
        customLogger("info", yellow, "No tasks availabe right now!")
      } else if (data) {
        const startTime = new Date()

        await this.client.query(
          `UPDATE tasks SET status = "processing", start_time = ${startTime} WHERE id = ${data.id}`
        )
      }
      await this.client.query("COMMIT")
    } catch (err) {
      await this.client.query("ROLLBACK")

      customLogger(
        "error",
        red,
        `Error in getNextAvailableTaskByQueue: ${err.message}`
      )
    }

    return data
  }

  // ✅
  async getNextAvailableTaskByType(type) {
    let data

    try {
      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )
      this.client = this.pool

      await this.client.query("BEGIN")
      const result = await this.client.query(
        `
        SELECT tasks.*
        FROM tasks
        JOIN queues ON tasks.queue_id = queues.id
        WHERE queues.type = $1
          AND (tasks.status = 'available' OR (tasks.status = 'processing' AND tasks.expiry_time < NOW()))
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
      `,
        [type]
      )

      data = result.rows[0]

      if (!data) {
        customLogger("info", yellow, "No tasks availabe right now!")
      } else if (data) {
        const startTime = new Date()

        await this.client.query(
          `UPDATE tasks SET status = 'processing', start_time = $1 WHERE id = $2;`,
          [startTime, data.id]
        )

        await this.client.query("COMMIT")
      }
    } catch (error) {
      await this.client.query("ROLLBACK")
      customLogger("error", red, `Error in getResults: ${err.message}`)
    }
    return data
  }

  // ✅
  async submitResults({ id, result, error = null }) {
    this.client = this.pool

    try {
      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )

      const endTime = new Date()

      const resultObj = error ? { sum: null, error } : { sum: result, error }

      const response = await this.client.query(
        `
      UPDATE tasks 
      SET 
        status = CASE
          WHEN $1::jsonb IS NOT NULL THEN 'error'::task_status
          ELSE 'completed'::task_status
        END,
        end_time = $2,
        result = CASE
          WHEN $1::jsonb IS NOT NULL THEN $1::jsonb
          ELSE $3::jsonb
        END
      FROM queues
      WHERE tasks.id = $4 AND queues.id = tasks.queue_id
      RETURNING tasks.queue_id, queues.options->>'callback' AS callback_url;
    `,
        [error, endTime, resultObj, id]
      )

      const queue = response.rows[0].queue_id
      const callbackUrl = response.rows[0].callback_url
      if (await this.allTasksCompleted(queue)) {
        if (callbackUrl) {
          const results = await this.getResults(queue)

          await this.postResults(callbackUrl, results)
        }
      }
      return
    } catch (err) {
      customLogger(
        "error",
        red,
        `Error in getNextAvailableTaskByType: ${err.message}`
      )
    }
  }

  // ✅
  async getResults(queue) {
    try {
      this.client = this.pool
      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )

      const response = await this.client.query(
        `
      SELECT task_id, result
      FROM tasks
      WHERE status IN ('completed', 'error') AND queue_id = ${queue};
       `
      )
      const results = {}
      response.rows.forEach(row => {
        results[row.task_id] = row.result
      })

      return { results }
    } catch (err) {
      customLogger("error", red, `Error in getResults: ${err.message}`)
    }
  }

  // ✅
  async postResults(url, results) {
    try {
      await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(results),
        timeout: 20000,
      })
    } catch (error) {
      customLogger("error", red, `Error in postResults: ${err.message}`)
    }
  }

  // ✅
  async totalTaskCountInQueue(queue) {
    this.client = this.pool

    const response = await this.client.query(
      `SELECT COUNT(*) FROM tasks WHERE queue_id = ${queue}`
    )
    return response
  }

  // ✅
  async completedTaskCountInQueue(queue) {
    this.client = this.pool

    const response = await this.client.query(
      `SELECT COUNT(*) FROM tasks WHERE queue_id = ${queue} AND status IN ('completed', 'error')`
    )
    return response
  }

  // ✅
  async allTasksCompleted(queue) {
    let areCompleted = false,
      totalTasks,
      completedTasks
    if (queue) {
      totalTasks = await this.totalTaskCountInQueue(queue)
      completedTasks = await this.completedTaskCountInQueue(queue)
    }
    if (totalTasks.rows[0].count === completedTasks.rows[0].count) {
      areCompleted = true
    }

    return areCompleted
  }

  // ✅
  async isQueuePresent(queue) {
    let isPresent = false
    try {
      this.client = this.pool
      customLogger(
        "info",
        cyan,
        `Total connections in the pool: ${this.pool.totalCount}`
      )

      const response = await this.client.query(
        `
      SELECT *
      FROM queues
      WHERE id = ${queue};
       `
      )
      if (response.rows[0].id) {
        isPresent = true
      }
      return isPresent
    } catch (err) {
      customLogger("error", red, `Error in isQueuePresent: ${err.message}`)
    }
  }
}

module.exports = new Services()
