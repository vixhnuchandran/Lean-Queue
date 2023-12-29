const colors = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
}

const colorize = (text, color) => {
  return `${colors[color]}${text}${colors.reset}`
}

const red = text => colorize(text, "red")
const green = text => colorize(text, "green")
const yellow = text => colorize(text, "yellow")
const blue = text => colorize(text, "blue")
const magenta = text => colorize(text, "magenta")
const cyan = text => colorize(text, "cyan")

function getCurrentTimestamp() {
  const now = new Date()
  const timestamp = `[${now.toLocaleString()}]`
  return timestamp
}

const logger = {
  info: function (message, ...args) {
    console.info(
      `${getCurrentTimestamp()} [${cyan(`INFO`)}] ${cyan(message, ...args)}`
    )
  },
  log: function (message, ...args) {
    console.log(
      `${getCurrentTimestamp()} [${green(`LOG`)}] ${green(message, ...args)}`
    )
  },
  trace: function (message, ...args) {
    console.trace(
      `${getCurrentTimestamp()} [${magenta(`TRACE`)}] ${magenta(
        message,
        ...args
      )}`
    )
  },
  warn: function (message, ...args) {
    console.warn(
      `${getCurrentTimestamp()} [${yellow(`WARN`)}] ${yellow(message, ...args)}`
    )
  },
  error: function (message, ...args) {
    console.error(
      `${getCurrentTimestamp()} [${red(`ERROR`)}] ${red(message, ...args)}`
    )
  },
}

module.exports = {
  logger,
}
