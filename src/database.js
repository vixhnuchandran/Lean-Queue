const { Client } = require("pg")
require("dotenv").config()

const connectionString = process.env.POSTGRES_URL
const client = new Client({
  connectionString,
})

const connectDB = async () => {
  try {
    await client.connect()
    console.log("Connected to the database")
  } catch (error) {
    console.error(`Error connecting to the database: ${error.message}`)
    throw new Error("Failed to connect to the database.")
  }
}

const createTables = async () => {
  try {
    await client.query(`
    CREATE TABLE IF NOT EXISTS queues (
      id SERIAL PRIMARY KEY,
      type VARCHAR(20) NOT NULL UNIQUE
    )
  `)

    await client.query(`
      CREATE TABLE IF NOT EXISTS tasks (
        id SERIAL PRIMARY KEY,
        taskid VARCHAR(255) NOT NULL,
        params JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'available' CHECK (status IN ('available', 'processing', 'completed', 'error')),
        result JSONB DEFAULT NULL,
        startTime TIMESTAMP DEFAULT NULL,
        endTime TIMESTAMP DEFAULT NULL,
        expiryTime TIMESTAMP DEFAULT NULL,
        queueid INTEGER REFERENCES queues(id)
      )
    `)

    console.log("Tables created successfully")
  } catch (error) {
    console.error("Error creating tables", error)
  }
}

// createTables()

module.exports = { connectDB, createTables, client }
