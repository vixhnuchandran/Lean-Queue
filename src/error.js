class QueueError extends Error {
  constructor(message) {
    super(message)
    this.name = "QueueError"
    this.message = message
  }
}

class ValidationError extends Error {
  constructor(message) {
    super(message)
    this.name = "ValidationError"
    this.message = message
  }
}

module.exports = { QueueError, ValidationError }
