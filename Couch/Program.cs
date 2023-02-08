using Couch;
using Couchbase;
using Couchbase.Diagnostics;
using Couchbase.Query;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;

public enum Mode
{
    SdkQuery,       // Synchronously run only Queries using the SDK (this still runs some KV for cluster management)
    SdkKv,          // Synchronously run only KV ops using the SDK
    SdkQueryAsync,  // Asynchronously run only Queries using the SDK (this still runs some KV for cluster management)
    SdkKvAsync,     // Asynchronously run only KV ops using the SDK
    SdkBothAsync,   // Asynchronously run Queries and KV ops using the SDK
    Http,           // Synchronously run queries outside of the SDK using raw HttpClient
    HttpAsync       // Asynchronously run queries outside of the SDK using raw HttpClient
}

public enum TaskComposition
{
    TaskRun,
    TaskRunAsync,
    ParallelForEachAsync
}

internal class Program
{
    #region couchbase config
    private static string couchbaseUserName = "couchbase_user";
    private static string couchbasePassword = "couchbasepassword";
    private static string couchbaseConnStr = "yourdomain.datanode.com";
    private static string testDocIdForKvOpps = "your-docid";
    private static string bucketName = "your-bucket";
    #endregion couchbase config

    #region params
    private static int success = 0;
    private static int degreeOfParallelism = 30000; //Environment.ProcessorCount;
    private static bool useTaskScheduler = true;
    private static int totalReq = 30000;
    private static Mode mode = Mode.SdkBothAsync;
    private static TaskComposition taskComposition = TaskComposition.TaskRunAsync;
    #endregion params

    private static Stopwatch sw = new();

    private static async Task Main(string[] args)
    {


        (bool Query, bool Kv) run;

        switch (mode)
        {
            case Mode.SdkQuery:
                run = (true, false);
                await RunSdk(totalReq, run).ConfigureAwait(false);
                break;
            case Mode.SdkKv:
                run = (false, true);
                await RunSdk(totalReq, run).ConfigureAwait(false);
                break;
            case Mode.SdkQueryAsync:
                run = (true, false);
                await RunSdkAsync(totalReq, run, taskComposition).ConfigureAwait(false);
                break;
            case Mode.SdkKvAsync:
                run = (false, true);
                await RunSdkAsync(totalReq, run, taskComposition).ConfigureAwait(false);
                break;
            case Mode.SdkBothAsync:
                run = (true, true);
                await RunSdkAsync(totalReq, run, taskComposition).ConfigureAwait(false);
                break;
            // HTTP request tests can only be KV
            case Mode.Http:
                await Run(totalReq).ConfigureAwait(false);
                break;
            case Mode.HttpAsync:
                await RunAsync(totalReq).ConfigureAwait(false);
                break;
        }

        sw.Stop();
        Console.WriteLine(sw.ElapsedMilliseconds + " ms");
    }

    private static string GetQuery() => $@"SELECT 1";

    public static async Task RunSdkAsync(int totalReq, (bool Query, bool Kv) run, TaskComposition composition)
    {
        var query = GetQuery();
        var requestPerTask = totalReq / degreeOfParallelism;
        var cb = new CouchbaseService(couchbaseConnStr, couchbaseUserName, couchbasePassword, bucketName);
        var tasks = new List<Task>();

        switch (composition)
        {
            case TaskComposition.TaskRun:
                if (run.Query)
                {
                    tasks.AddRange(Enumerable.Range(1, degreeOfParallelism).Select((idx) => CallCbSdk(cb, requestPerTask, idx.ToString(), isKv: false)).ToArray());
                }
                if (run.Kv)
                {
                    tasks.AddRange(Enumerable.Range(1, degreeOfParallelism).Select((idx) => CallCbSdk(cb, requestPerTask, idx.ToString(), isKv: true)).ToArray());
                }
                break;

            case TaskComposition.ParallelForEachAsync:
                var parallelOptions = new ParallelOptions();

                if (run.Query)
                {
                    var list = Enumerable.Range(1, degreeOfParallelism);

                    await Parallel.ForEachAsync(list, parallelOptions, async (idx, token) =>
                    {
                        await CallCbSdk(cb, requestPerTask, idx.ToString(), isKv: false).ConfigureAwait(false);
                    }).ConfigureAwait(false);
                }
                if (run.Kv)
                {
                    var list = Enumerable.Range(1, degreeOfParallelism);

                    await Parallel.ForEachAsync(list, parallelOptions, async (idx, token) =>
                    {
                        await CallCbSdk(cb, requestPerTask, idx.ToString(), isKv: true).ConfigureAwait(false);
                    }).ConfigureAwait(false);
                }
                break;

            // DOESN'T WORK
            case TaskComposition.TaskRunAsync:
                if (run.Query)
                {
                    tasks.AddRange(Enumerable.Range(1, degreeOfParallelism).Select((idx) =>
                        Task.Run(async () => await CallCbSdk(cb, requestPerTask, idx.ToString(), isKv: false).ConfigureAwait(false))).ToArray());
                    //             ^notice this async await. This puts the job on the thread pool's global queue and results in thread starvation
                }
                if (run.Kv)
                {
                    tasks.AddRange(Enumerable.Range(1, degreeOfParallelism).Select((idx) =>
                        Task.Run(async () => await CallCbSdk(cb, requestPerTask, idx.ToString(), isKv: true).ConfigureAwait(false))).ToArray());
                }
                break;
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public static async Task RunSdk(int requestLimit, (bool Query, bool Kv) run)
    {
        var cb = new CouchbaseService(couchbaseConnStr, couchbaseUserName, couchbasePassword, bucketName);
        var tasks = new List<Task>();
        if (run.Query)
        {
            tasks.Add(CallCbSdk(cb, requestLimit, "0", isKv: false));
        }
        if (run.Kv)
        {
            tasks.Add(CallCbSdk(cb, requestLimit, "0", isKv: true));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public static async Task CallCbSdk(CouchbaseService cb, int requestLimit, string threadId, bool isKv)
    {
        var query = GetQuery();
        var docId = testDocIdForKvOpps;
        var i = 0;
        var start = DateTime.Now;
        var logEvery = Math.Min((requestLimit / 8), 1000);

        sw.Start();

        while (i++ < requestLimit)
        {
            try
            {
                if (isKv)
                {
                    var res = await cb.GetAsync<dynamic>(docId, useTaskScheduler).ConfigureAwait(false);
                    IncrementAndLog(i, logEvery, start, threadId, true);
                }
                else
                {
                    var res = await cb.QueryAsync<dynamic>(query, useTaskScheduler).ConfigureAwait(false);
                    IncrementAndLog(i, logEvery, start, threadId, false);
                }
            }
            catch (Exception ex)
            {
                var count = Volatile.Read(ref success);
                var now = DateTime.Now;
                var ts = now - start;
                Console.WriteLine($"{count}");
                Console.WriteLine($"[{ts}] ({threadId,3}) exception {ex.GetType().Name} * * * *");
            }
        }
    }

    public static async Task RunAsync(int totalReq)
    {
        var requestPerTask = totalReq / degreeOfParallelism;
        var tasks = Enumerable.Range(1, degreeOfParallelism).Select((idx) => CallCbHttp(requestPerTask, idx.ToString())).ToArray();
        //this one doesn't work due to Task.Run(async () => await ...);
        //var tasks = Enumerable.Range(1, degreeOfParallelism).Select((idx) => Task.Run(async () => await CallCbHttp(requestPerTask, idx.ToString()).ConfigureAwait(false))).ToArray();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public static async Task Run(int totalReq)
    {
        await CallCbHttp(totalReq, "single-thread").ConfigureAwait(false);
    }

    public static async Task CallCbHttp(int requestLimit, string threadId)
    {
        // create client
        var client = new HttpClient();
        client.BaseAddress = new Uri("http://rieppecbquery01z2.ppe.onerevint.com:8093/query/service");
        client.Timeout = TimeSpan.FromSeconds(5);
        // auth
        var auth = $"{couchbaseUserName}:{couchbasePassword}";
        var base64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(auth));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", base64);
        var query = GetQuery();
        var json = JsonConvert.SerializeObject(new { Statement = query });
        var i = 0;
        var start = DateTime.Now;
        var logEvery = Math.Min((requestLimit / 8), 1000);

        sw.Start();

        while (i++ < requestLimit)
        {
            try
            {
                var req = new HttpRequestMessage()
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json"),
                    Method = HttpMethod.Post
                };
                var res = await client.SendAsync(req).ConfigureAwait(false);
                res.EnsureSuccessStatusCode();
                IncrementAndLog(i, logEvery, start, threadId, false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{i}");
            }
        }
    }
    public static void IncrementAndLog(int i, int logEvery, DateTime start, string threadId, bool isKv)
    {
        Interlocked.Increment(ref success);
        if (i % Math.Max(1, logEvery) == 0)
        {
            var now = DateTime.Now;
            var ts = now - start;
            var count = Volatile.Read(ref success);
            Console.WriteLine($"[{ts}] ({threadId,3}) ({(isKv ? "KV" : "N1QL"),4}) count: {count,5} rate: {(int)(count / ts.TotalSeconds),8} per second");
        }
    }
}
public class CouchbaseService
{
    private readonly ThrottledTaskScheduler taskScheduler;
    private readonly TaskFactory taskFactory;
    private readonly string bucketName;
    private readonly ICluster cluster;

    public CouchbaseService(string clusterUrl, string username, string password, string bucketName)
    {
        var degreeOfParallelism = Environment.ProcessorCount;
        taskScheduler = new ThrottledTaskScheduler(degreeOfParallelism);
        taskFactory = new TaskFactory(taskScheduler);
        this.bucketName = bucketName;
        cluster = InitCluster(clusterUrl, username, password, bucketName).GetAwaiter().GetResult();
    }

    public static async Task<ICluster> InitCluster(string clusterUrl, string username, string password, string bucketName)
    {

        ClusterOptions clusterOptions = new()
        {
            UserName = username,
            Password = password,
            ConnectionString = clusterUrl,
            //EnableConfigPolling = false,
            //EnableOperationDurationTracing = false,
            //EnableDnsSrvResolution = false,
            //Experiments = experiments,
            //MaxHttpConnections = 5,

            //KvTimeout = TimeSpan.FromSeconds(10)
            //MaxKvConnections = 1,
            //NumKvConnections = 1,
            //HttpConnectionLifetime = TimeSpan.Zero
        };

        Console.WriteLine($"Bootstrapping cluster...");
        ICluster cluster = await Cluster.ConnectAsync(clusterOptions).ConfigureAwait(false);
        await cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds(90), GetWaitUntilReadyOptions()).ConfigureAwait(false);
        Console.WriteLine($"Cluster conected.");
        Console.WriteLine($"****");

        Console.WriteLine($"Bootstrapping bucket...");
        var bucket = await cluster.BucketAsync(bucketName).ConfigureAwait(false);
        await bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(180), GetWaitUntilReadyOptions());
        Console.WriteLine($"Bucket conected.");
        Console.WriteLine($"****");

        Console.WriteLine("Start dotnet-counters or press any key to skip.");
        Console.ReadKey();
        return cluster;
    }

    private static WaitUntilReadyOptions GetWaitUntilReadyOptions()
    {
        var options = new WaitUntilReadyOptions();
        options.ServiceTypes(
            ServiceType.KeyValue,
            ServiceType.Query
        );
        return options;
    }

    public Task<IEnumerable<T>> QueryAsync<T>(string query, bool useTaskScheduler)
    {
        return (useTaskScheduler) 
            ? taskFactory.StartNew(() => InternalQueryAsync<T>(query)).Unwrap()
            :  InternalQueryAsync<T>(query);
    }

    private async Task<IEnumerable<T>> InternalQueryAsync<T>(string query)
    {
        var options = new QueryOptions();
        options.Readonly(true);
        var queryResults = await cluster.QueryAsync<T>(query, options).ConfigureAwait(false);
        var returnedRecords = new List<T>();
        await foreach (var i in queryResults.Rows.ConfigureAwait(false))
        {
            returnedRecords.Add(i);
        }
        return returnedRecords;
    }

    public Task<T> GetAsync<T>(string docId, bool useTaskScheduler)
    {
        return (useTaskScheduler)
            ? taskFactory.StartNew(() => InternalGetAsync<T>(docId)).Unwrap()
            : InternalGetAsync<T>(docId);
    }

    private async Task<T> InternalGetAsync<T>(string docId)
    {
        var bucket = await cluster.BucketAsync(bucketName).ConfigureAwait(false);
        var collection = await bucket.DefaultCollectionAsync().ConfigureAwait(false);
        var result = await collection.GetAsync(docId).ConfigureAwait(false);
        if (result != null)
        {
            return result.ContentAs<T>();
        }
        return default;
    }
}
