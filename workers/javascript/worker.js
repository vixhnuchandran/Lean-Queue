const {
  delay,
  getRandom,
  red,
  green,
  yellow,
  blue,
  magenta,
  cyan,
} = require("./utils")
require("dotenv").config()

const root = "http://127.0.0.1:8383"

const halt_n_execute = true // make it false or lighting fast execution ;)

const executeTask = (num1, num2, type) => {
  switch (type) {
    case "addition":
      return num1 + num2
      break
    default:
      throw new Error("Unsupported operation type: " + type)
  }
}

const getNextTask = async ({ queue, type }) => {
  try {
    if (halt_n_execute) {
      await delay(getRandom(2, 3, "worker") * 1000)
    }
    const requestBody = queue ? { queue } : { type }
    const response = await fetch(root + `/get-next-available-task`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
    })

    const data = await response.json()

    return data
  } catch (error) {
    console.log(error)
    return null
  }
}

const sendResults = async (id, result, endTime, error) => {
  try {
    const response = await fetch(root + `/submit-results`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ id, endTime, result, error }),
    })

    return response
  } catch (error) {
    console.error("Error while sending result:", error.message)
  }
}

const runWorker = async () => {
  while (true) {
    const type = "addition"

    try {
      console.log(magenta(`\n Fetching tasks...`))
      if (halt_n_execute) {
        await delay(getRandom(2, 3, "worker") * 1000)
      }
      const response = await getNextTask({ type })
      if (response.message) {
        console.log(yellow(` No tasks found, worker going sleep mode`))
        await delay(1 * 60 * 1000) // 1 min
        continue
      }
      if (halt_n_execute) {
        await delay(getRandom(2, 3, "worker") * 1000)
      }

      console.log(cyan(` ↳ Task found`))
      console.log(cyan(` ↳ Task details: ${JSON.stringify(response)}`))
      const { id, params } = response
      const { num1, num2 } = params

      const result = executeTask(num1, num2, type)

      console.log(green(` ↳ Task completed successfully`))
      if (result) {
        await sendResults(id, result, null)
        console.log(green(` ↳ Results submitted successfully`))
      } else if (error) {
        await sendResults(id, result, error)
        console.log(green(` ↳ Results submitted successfully`))
      } else {
        console.log(red(` ↳ Error sending results: ${error.message}`))
      }
    } catch (error) {
      console.error("Error in runWorker:", error.message)
    }
  }
}

runWorker()
