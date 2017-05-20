using System;
using StackExchange.Redis;

namespace WorkDispatch.Monitor
{
    class Program
    {
        private static ConnectionMultiplexer redis;

        static void Main(string[] args)
        {
            redis = ConnectionMultiplexer.Connect("127.0.0.1");
            redis.PreserveAsyncOrder = false;

            ISubscriber sub = redis.GetSubscriber();
            Console.WriteLine("Monitoring batch progress, press [Enter] to stop.");

            sub.Subscribe("workitem-succeeded", (channel, message) => {
                 var ids = ((string)message).Split(':');
                 Console.WriteLine($"Batch {ids[0]} item {ids[1]} succeeded.");
            });
            sub.Subscribe("workitem-failed", (channel, message) => {
                 var ids = ((string)message).Split(':');
                 Console.WriteLine($"Batch {ids[0]} item {ids[1]} FAILED.");
            });

            Console.ReadLine();
        }
    }
}
