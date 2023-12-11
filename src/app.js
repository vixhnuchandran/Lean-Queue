const express = require("express")
const routes = require("./routes")
require("dotenv").config()

const INTERNAL_SERVER_ERROR = "Internal server error"

const app = express()

// middleware
app.use(express.urlencoded({ extended: true }))
app.use(express.json())

// routes
app.get("/", (req, res) => {
  res.send({ message: ` Task handler application` })
})

app.use("/", routes)

// error handler
app.use((err, req, res, next) => {
  console.error(err.stack)
  res.sendStatus(500).send({ error: INTERNAL_SERVER_ERROR })
})

const PORT = process.env.PORT || 8383
app.listen(PORT, () => {
  console.log(`\nServer listening on port ${PORT} `)
})
