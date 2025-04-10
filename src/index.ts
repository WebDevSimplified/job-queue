import { RedisQueue } from "./job-queue/redis-adapter"
import { Worker } from "./job-queue/worker"

const queue = new RedisQueue<{ email: string; body: string }>(
  "redis://localhost:6379",
  "test-queue",
  {
    retryAttempts: 3,
    retryDelay: 1000,
  }
)

queue.addJob({
  data: { email: "test@test.com", body: "Hi" },
  name: "1",
  options: {
    priority: 1,
  },
})

queue.addJob({
  data: { email: "test@test.com", body: "Bye" },
  name: "2",
  options: {
    priority: 3,
    delay: 1000,
  },
})

queue.addJob({
  data: { email: "test2@test.com", body: "Hello World" },
  name: "3",
  options: {
    priority: 2,
  },
})

queue.addJob({
  data: { email: "test2@test.com", body: "Error" },
  name: "4",
})

setTimeout(() => {
  queue.addJob({
    data: { email: "test@test.com", body: "High Priority" },
    name: "5",
    options: {
      priority: 5,
    },
  })
}, 2000)

const worker = new Worker(
  queue,
  async job => {
    console.log(`Start: W1 ${job.name}`)
    await sendEmail(job.data.email, job.data.body)
  },
  { concurrency: 3 }
)

worker.start()

function sendEmail(email: string, body: string) {
  if (body === "Error") {
    console.log(`Error sending email to ${email}`)
    throw new Error("Simulated error")
  }
  return new Promise(resolve => setTimeout(resolve, 1000))
}
