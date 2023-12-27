const { performance } = require("perf_hooks")

const executeQueriesWithDebug = async (isDebugMode, debugQuery, mainQuery) => {
  let startTime, endTime, totalTime, data

  if (isDebugMode) {
    console.log(await client.query(debugQuery))
    startTime = performance.now()
    data = await client.query(mainQuery)
    endTime = performance.now()
    totalTime = endTime - startTime
    console.log("Query execution time", totalTime)
    return data
  } else {
    startTime = performance.now()
    data = await client.query(mainQuery)
    endTime = performance.now()
    totalTime = endTime - startTime
    console.log("Query execution time", totalTime)
    return data
  }
}

module.exports = { executeQueriesWithDebug }
