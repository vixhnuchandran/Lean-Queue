const seedrandom = require("seedrandom")
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

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

const getRandom = (min, max, seed = Date.now()) => {
  const rng = seedrandom(seed)
  const randomValue = rng()
  const range = max - min + 1
  const randomNum = Math.floor(randomValue * range) + min

  return randomNum
}

module.exports = {
  delay,
  getRandom,
  red,
  green,
  yellow,
  blue,
  magenta,
  cyan,
}
