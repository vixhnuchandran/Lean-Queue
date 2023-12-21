const express = require("express")
const routes = require("./routes")
const { pool } = require("./db")
require("dotenv").config()

const INTERNAL_SERVER_ERROR = "Internal server error"

const app = express()

// middleware
app.use(express.urlencoded({ extended: true }))
app.use(express.json({ limit: "100mb" }))
app.use(async (req, res, next) => {
  const client = await pool.connect()
  req.dbClient = client
  next()
})

// routes
app.get("/", (req, res) => {
  res.sendStatus(200).send(`Lean Queue application`)
})
app.use("/", routes)
app.get("/*", (req, res) => {
  res.sendStatus(400).send(`Error 404`)
})

// error handler
app.use((err, req, res, next) => {
  console.error(err.stack)
  res.sendStatus(500).send({ error: INTERNAL_SERVER_ERROR })
  if (req.dbClient) {
    req.dbClient.release()
  }
})

const PORT = process.env.PORT || 8383
app.listen(PORT, () => {
  console.log(`\nServer listening on port ${PORT} `)
})
