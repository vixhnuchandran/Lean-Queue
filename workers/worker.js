const { delay, getRandom, colorize } = require("./utils")
require("dotenv").config()

const executeTask = (num1, num2, type) => {
  switch (type) {
    case "addition":
      return num1 + num2
      break
    case "subtraction":
      return num1 - num2
      break
    case "multiplication":
      return num1 * num2
      break
    case "division":
      return num1 / num2
      break
    default:
      throw new Error("Unsupported operation type: " + type)
  }
}

const getNextTask = async ({ queue, type }) => {
  try {
    await delay(getRandom(5, 7, "worker") * 1000)

    const requestBody = queue ? { queue } : { type }

    const response = await fetch(`http://127.0.0.1:8383/get-available-tasks`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
    })

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`)
    }

    const data = await response.json()

    return data
  } catch (error) {
    return null
  }
}

const sendResults = async (id, result, endTime, error) => {
  try {
    const response = await fetch(`http://127.0.0.1:8383/submit-results`, {
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
      console.log()
      console.log(colorize(`\n Fetching tasks...`, "magenta"))
      await delay(getRandom(5, 7, "worker") * 1000)

      const response = await getNextTask({ type })

      if (response === null) {
        console.log(
          colorize(` No tasks found, worker going sleep mode`, "yellow")
        )
        await delay(10000)
        continue
      }
      await delay(getRandom(4, 5, "worker") * 1000)

      console.log(colorize(` ↳ Task found`, "cyan"))
      console.log(
        colorize(` ↳ Task details: ${JSON.stringify(response)}`, "cyan")
      )
      const { id, params } = response
      const { num1, num2 } = params

      const result = executeTask(num1, num2, type)

      console.log(colorize(` ↳ Task completed successfully`, "green"))
      if (result) {
        await sendResults(id, result, null)
        console.log(colorize(` ↳ Results submitted successfully`, "green"))
      } else if (error) {
        await sendResults(id, result, error)
        console.log(colorize(` ↳ Results submitted successfully`, "green"))
      } else {
        console.log(
          colorize(` ↳ Error sending results: ${error.message}`, "red")
        )
      }
    } catch (error) {
      console.error("Error in runWorker:", error.message)
    }
  }
}

runWorker()
