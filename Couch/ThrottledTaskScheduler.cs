namespace Couch;

public class ThrottledTaskScheduler : TaskScheduler
{
    [ThreadStatic]
    private static bool isCurrentThreadProcessing;

    private readonly LinkedList<Task> tasks = new();

    private int maxDegreeOfParallelism;

    private static int delegatesQueuedOrRunning = 0;

    public ThrottledTaskScheduler(int maxDegreeOfParallelism)
    {
        ThreadPool.SetMaxThreads(maxDegreeOfParallelism / 2 * 10, maxDegreeOfParallelism / 2 * 10);
        ThreadPool.SetMinThreads(maxDegreeOfParallelism / 2 * 10, maxDegreeOfParallelism / 2 * 10);
        
        if (maxDegreeOfParallelism < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));
        }
        this.maxDegreeOfParallelism = maxDegreeOfParallelism;
    }

    // Queues a task to the scheduler.
    protected sealed override void QueueTask(Task task)
    {
        lock (tasks)
        {
            tasks.AddLast(task);
            if (delegatesQueuedOrRunning < maxDegreeOfParallelism)
            {
                ++delegatesQueuedOrRunning;
                NotifyThreadPoolOfPendingWork();
            }
        }
    }

    // Inform the ThreadPool that there's work to be executed for this scheduler.
    private void NotifyThreadPoolOfPendingWork()
    {
        ThreadPool.UnsafeQueueUserWorkItem<object>((_) =>
        {
            isCurrentThreadProcessing = true;
            try
            {
                while (true)
                {
                    Task task;
                    lock (tasks)
                    {
                        if (!tasks.Any())
                        {
                            --delegatesQueuedOrRunning;
                            break;
                        }
                        task = tasks.First();
                        tasks.RemoveFirst();
                    }

                    base.TryExecuteTask(task);
                }
            }
            finally
            {
                isCurrentThreadProcessing = false;
            }
        },
        state: null,
        preferLocal: true);
    }

    // Attempts to execute the specified task on the current thread.
    protected sealed override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
    {
        if (!isCurrentThreadProcessing)
        {
            return false;
        }

        if (taskWasPreviouslyQueued)
        {
            if (TryDequeue(task))
            {
                return base.TryExecuteTask(task);
            }

            return false;
        }

        return base.TryExecuteTask(task);
    }

    protected sealed override bool TryDequeue(Task task)
    {
        lock (tasks)
        {
            return tasks.ToList().Remove(task);
        }
    }

    protected sealed override IEnumerable<Task> GetScheduledTasks()
    {
        var lockTaken = false;
        try
        {
            Monitor.TryEnter(tasks, ref lockTaken);
            if (lockTaken)
            {
                return tasks.Take(1);
            }

            throw new NotSupportedException();
        }
        finally
        {
            if (lockTaken)
            {
                Monitor.Exit(tasks);
            }
        }
    }
}

