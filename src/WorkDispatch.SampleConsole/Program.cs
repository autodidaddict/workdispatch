using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace WorkDispatch.SampleConsole
{
    class Program
    {
        private static ConnectionMultiplexer redis;

        static void Main(string[] args)
        {
            redis = ConnectionMultiplexer.Connect("127.0.0.1");
            redis.PreserveAsyncOrder = false;

            IDatabase db = redis.GetDatabase();
            long batchId = db.StringIncrement("id:batches");

            var batchHash = new HashEntry[] {
                new HashEntry("owner", "Kevin"),
                new HashEntry("created", System.Environment.TickCount),
                new HashEntry("status", "Created"),
                new HashEntry("totalitems", 1000),
                new HashEntry("items_complete", 0),
                new HashEntry("items_failed", 0)
            };

            db.HashSet($"batch:{batchId}", batchHash);

            for (int x = 0; x < 1000; x++)
            {
                long workItemId = db.StringIncrement("id:workitems");
                db.SetAdd($"batch:{batchId}:workitems", workItemId);
                var workItemEntries = new HashEntry[] {
                    new HashEntry("status", "Created"),
                    new HashEntry("input1", "A"),
                    new HashEntry("input2", "B"),
                    new HashEntry("timestamp", System.Environment.TickCount)
                };
                db.HashSet($"workitem:{workItemId}", workItemEntries);
            }

            Console.WriteLine("Created a batch.");

            var backgroundTasks = new[] {
                Task.Run( () => WorkBatch(db, batchId, 1, 200)),
                Task.Run( () => WorkBatch(db, batchId, 201, 500)),
                Task.Run( () => WorkBatch(db, batchId, 501, 1000))
            };

            Task.WaitAll(backgroundTasks);
            Console.WriteLine("Completed all batches.");
        }

        static void WorkBatch(IDatabase db, long batchId, int start, int finish)
        {
            var sub = redis.GetSubscriber();
            for (int i = start; i < finish + 1; i++)
            {
                Thread.Sleep(10);
                HashEntry[] updates;
                HashEntry status;
                string channel = "workitem-succeeded";
                if (i % 5 == 0)
                {
                    channel = "workitem-failed";
                    status = new HashEntry("status", "Failed");
                }
                else
                {
                    status = new HashEntry("status", "Completed");
                }
                var i1 = (string)db.HashGet($"workitem:{i}", "input1");
                var i2 = (string)db.HashGet($"workitem:{i}", "input2");

                updates = new HashEntry[] {
                    status,
                    new HashEntry("result", i1+ " " + i2)
                };
                db.HashSet($"workitem:{i}", updates);
                sub.Publish(channel, $"{batchId}:{i}");
            }
        }
    }
}
