using System;
using System.Diagnostics;
using System.Threading;
using Messages;
using NServiceBus;

namespace EndpointV5
{
    public class MyMessageHandler : IHandleMessages<MyMessage>
    {
        public static readonly Stopwatch Stopwatch = new Stopwatch();
        private static int _counter;

        public void Handle(MyMessage message)
        {
            Thread.Sleep(100);

            if (Interlocked.Increment(ref _counter) == 10000)
            {
                Stopwatch.Stop();
                Console.WriteLine($"Receiving 10000 messages took {Stopwatch.Elapsed}");
            }
        }
    }
}