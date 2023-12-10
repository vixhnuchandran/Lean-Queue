# LeanQueue

LeanQueue is a Node.js application designed to manage task queues. It uses Express as a web framework and PostgreSQL for data storage.

## Features

- **Queue Management:** Create and manage task queues dynamically.
- **Task Handling:** Add tasks to queues and retrieve them for processing.
- **Result Submission:** Submit results for completed tasks.

## Built With

- [Node.js](https://nodejs.org/) - JavaScript runtime.
- [Express](https://expressjs.com/) - Web framework for Node.js.
- [PostgreSQL](https://www.postgresql.org/) - Open-source relational database.
- [seedrandom](https://www.npmjs.com/package/seedrandom) - Library for seeding random number generators.

## Getting Started

### Prerequisites

- [Node.js](https://nodejs.org/)
- [npm](https://www.npmjs.com/)
- [PostgreSQL](https://www.postgresql.org/)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/vixhnuchandran/lean-queue
   cd lean-queue
   ```

2. Install dependencies:

   ```bash
   npm install
   ```

3. Create a `.env` file in the project root and define the following environment variables:

   ```plaintext
   POSTGRES_URL=your_postgres_database_url
   ```

   Adjust `your_postgres_database_url` with your PostgreSQL database URL.

### Usage

Run the application using nodemon:

```bash
npm start
```

### Functionality

- **Create Queue:**

  ```plaintext
  POST /create-queue
  ```

  Create a new task queue with specified type and tasks.

- **Add Tasks to Queue:**

  ```plaintext
  POST /add-tasks
  ```

  Add tasks to an existing queue.

- **Get Available Tasks:**

  ```plaintext
  POST /get-available-tasks
  ```

  Get the next available task from the specified queue or type.

- **Submit Results:**

  ```plaintext
  POST /submit-results
  ```

  Submit the results of a task, marking it as completed or with an error.

## License

This project is licensed under the MIT License
