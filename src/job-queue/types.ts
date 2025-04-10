export type JobOptions = {
  delay: number
  priority: number
  retry: {
    totalAttempts: number
    maxAttempts: number
    delay: number
  }
}

export type Job<T> = {
  id: string
  name: string
  data: T
  options: JobOptions
}
