import { Job, JobOptions } from "./types"

export type QueueOptions = {
  retryAttempts: number
  retryDelay: number
}

export type OptionalJobOptions = Partial<JobOptions> & {
  retry?: Partial<JobOptions["retry"]>
}

export abstract class Queue<T> {
  protected queueName: string
  protected options: QueueOptions

  constructor(
    queueName: string,
    options: QueueOptions = { retryAttempts: 10, retryDelay: 1000 }
  ) {
    this.queueName = queueName
    this.options = options
  }

  async addJob(
    job: Omit<Job<T>, "id" | "options"> & {
      options?: OptionalJobOptions
      id?: string
    }
  ) {
    const id = job.id ?? crypto.randomUUID()
    if (
      (job.options?.retry?.totalAttempts ?? 0) >=
      (job.options?.retry?.maxAttempts ?? 1)
    ) {
      return id
    }

    await this.push({
      ...job,
      id,
      options: {
        delay: job.options?.delay ?? 0,
        priority: job.options?.priority ?? 0,
        retry: {
          totalAttempts: job.options?.retry?.totalAttempts ?? 0,
          maxAttempts:
            job.options?.retry?.maxAttempts ?? this.options.retryAttempts,
          delay: job.options?.retry?.delay ?? this.options.retryDelay,
        },
      },
    })

    return id
  }

  protected abstract push(
    job: Job<T> & { options: { retry: Required<Job<T>["options"]["retry"]> } }
  ): Promise<unknown>
  abstract getOrWaitForJob(): Promise<
    | (Job<T> & { options: { retry: Required<Job<T>["options"]["retry"]> } })
    | null
  >
  abstract get length(): Promise<number>
}
