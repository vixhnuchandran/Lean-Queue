const fs = require("fs")
const path = require("path")
const pool = require("./db")

createTables = async () => {
  let client = null
  try {
    client = pool.connect()

    const sqlFilePath = path.join(__dirname, "query.sql")
    const sqlQueries = fs.readFileSync(sqlFilePath, "utf-8")

    await client.query(sqlQueries)

    console.log("Tables created successfully")
  } catch (error) {
    console.error("Error creating tables", error)
  } finally {
    if (client) client.release()
  }
}

// createTables()
