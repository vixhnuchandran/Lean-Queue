const express = require("express")
require("dotenv").config()
const pool = require("./db")
const routes = require("./routes")

const app = express()

// middleware
app.use(express.urlencoded({ extended: true }))
app.use(express.json())

// database connection
pool.connect()
// routes
app.get("/", (req, res) => {
  res.send({ message: ` Task handler application` })
})

app.use("/", routes)

// error handler
app.use((err, req, res, next) => {
  console.error(err.stack)
  res.sendStatus(500).send({ message: `Something went wrong!` })
})

const PORT = process.env.PORT || 8383
app.listen(PORT, () => {
  console.log(`\nServer listening on port ${PORT} `)
})
