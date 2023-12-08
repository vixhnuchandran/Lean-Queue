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

const getRandomDelay = (min, max, seed) => {
  const rng = seedrandom(seed)
  const randomValue = rng()
  const delayRange = max - min + 1
  const randomDelay = Math.floor(randomValue * delayRange) + min

  return randomDelay
}

module.exports = { delay, colorize, getRandomDelay }
