const express = require("express")
const routes = require("./routes")
const { HTTP_OK } = require("./constants")
const {
  attachRequestIdMiddleware,
  attachQueryManagerMiddleware,
  releaseQueryManagerClientMiddleware,
  errorHandlingMiddleware,
} = require("./middleware")
require("dotenv").config()

const app = express()

// middlewares
app.use(express.urlencoded({ extended: true }))
app.use(express.json({ limit: "50mb" }))

app.use(attachRequestIdMiddleware)

app.use(attachQueryManagerMiddleware)

// routes
app.get("/", (req, res) => {
  res.sendStatus(HTTP_OK)
})
app.use("/", routes)

app.use(errorHandlingMiddleware)

app.use(releaseQueryManagerClientMiddleware)

const PORT = process.env.PORT || 8383
app.listen(PORT, () => {
  console.log(`\nServer listening on port ${PORT} `)
})
