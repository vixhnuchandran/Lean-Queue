const express = require("express")
const routes = require("./routes")
const { pool } = require("./db")
require("dotenv").config()
const {
  INTERNAL_SERVER_ERROR,
  HTTP_OK,
  HTTP_BAD_REQUEST,
  HTTP_INTERNAL_SERVER_ERROR,
} = require("./constants")

const app = express()

// middleware
app.use(express.urlencoded({ extended: true }))
app.use(express.json({ limit: "50mb" }))
app.use(async (req, res, next) => {
  const client = await pool.connect()
  req.dbClient = client
  next()
})

// routes
app.get("/", (req, res) => {
  res.sendStatus(HTTP_OK)
})
app.use("/", routes)
app.get("/*", (req, res) => {
  res.sendStatus(HTTP_BAD_REQUEST)
})

// error handler
app.use((err, req, res, next) => {
  console.error(err.stack)
  res.status(HTTP_INTERNAL_SERVER_ERROR).send({ error: INTERNAL_SERVER_ERROR })
  if (req.dbClient) {
    req.dbClient.release()
    next()
  }
})

const PORT = process.env.PORT || 8383
app.listen(PORT, () => {
  console.log(`\nServer listening on port ${PORT} `)
})
