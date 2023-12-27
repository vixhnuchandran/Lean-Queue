const axios = require("axios");

const root = "https://lean-queue.vercel.app/";
// const root = "http://127.0.0.1:8383/";

let halt_n_execute = true;
const delay = Math.random() * (8 - 3) + 3;

function executeTask(num1, num2, operationType) {
  num1 = parseInt(num1);
  num2 = parseInt(num2);

  if (operationType === "addition") {
    return num1 + num2;
  } else if (operationType === "subtraction") {
    return num1 - num2;
  } else if (operationType === "multiplication") {
    return num1 * num2;
  } else if (operationType === "division") {
    return num1 / num2;
  } else {
    throw new Error("Unsupported operation type: " + operationType);
  }
}

async function getNextTask(using, value, timeout = 10, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const requestBody = {
        [using]: value,
      };

      if (halt_n_execute) {
        await new Promise((resolve) => {
          console.log(
            `\nPress 'Enter' to send req with body ${JSON.stringify(
              requestBody
            )}`
          );
          process.stdin.once("data", () => resolve());
        });
      }

      const response = await axios.post(
        root + "get-next-available-task",
        requestBody,
        { timeout }
      );

      const data = response.data;
      return data;
    } catch (error) {
      if (error.code === "ECONNABORTED") {
        console.log("Timeout occurred. Retrying...");
      } else {
        console.log(`Error: ${error.message}. Retrying...`);
      }
    }
  }

  return null;
}

async function sendResults(taskId, result, error) {
  try {
    const requestBody = { id: taskId, result, error };

    if (halt_n_execute) {
      await new Promise((resolve) => {
        console.log(
          `Press 'Enter' to submit result with body ${JSON.stringify(
            requestBody
          )}`
        );
        process.stdin.once("data", () => resolve());
      });
    }

    const response = await axios.post(root + "submit-results", requestBody);
    return response;
  } catch (error) {
    console.log("Error while sending result:", error.message);
  }
}

async function runWorker() {
  const args = parseArguments();

  while (true) {
    try {
      const response = await getNextTask(args.using, args.value, 10, 3);

      if (response && response.message) {
        console.log("\nNo tasks found, worker going to sleep mode");
        await new Promise((resolve) => setTimeout(resolve, delay * 1000));
        continue;
      }

      console.log(`Task found, details: ${JSON.stringify(response)}`);

      const taskId = response.id;
      const params = response.params;
      const { num1, num2 } = params;

      if (halt_n_execute) {
        await new Promise((resolve) => {
          console.log("Press 'Enter' to execute...");
          process.stdin.once("data", () => resolve());
        });
      }

      const result =
        args.using === "type"
          ? executeTask(num1, num2, args.value)
          : executeTask(num1, num2, response.type);

      console.log(`Task executed successfully, Result is: ${result}`);

      await sendResults(taskId, result, null);
      console.log("Results submitted successfully");
    } catch (error) {
      console.log("Error in runWorker:", error.message);
    }
  }
}

function parseValue(value) {
  try {
    return parseInt(value);
  } catch (error) {
    try {
      return JSON.parse(value);
    } catch (jsonError) {
      return value;
    }
  }
}

function parseArguments() {
  const args = process.argv.slice(2);
  const parsedArgs = { using: null, value: null };

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace(/--/, "");
    const value = args[i + 1];

    if (key === "using") {
      parsedArgs.using = value;
    } else if (key === "value") {
      parsedArgs.value = parseValue(value);
    }
  }

  return parsedArgs;
}

if (require.main === module) {
  runWorker();
}
