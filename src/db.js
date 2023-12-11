const { Pool } = require("pg")
require("dotenv").config()

const connectionString = process.env.POSTGRES_URL
const pool = new Pool({
  connectionString,
  max: 5,
})

pool.on("connect", () => {
  console.log("Connected to the PostgreSQL database")
})

pool.on("error", err => {
  console.error("Error connecting to the PostgreSQL database:", err.message)
})

module.exports = { pool }
