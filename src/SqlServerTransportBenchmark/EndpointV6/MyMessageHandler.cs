using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Messages;
using NServiceBus;

namespace EndpointV6
{
    public class MyMessageHandler : IHandleMessages<MyMessage>
    {
        public static readonly Stopwatch Stopwatch = new Stopwatch();
        private static int _counter;

        public async Task Handle(MyMessage message, IMessageHandlerContext context)
        {
            await Task.Delay(100);

            if (Interlocked.Increment(ref _counter) == 10000)
            {
                Stopwatch.Stop();
                Console.WriteLine($"Receiving 10000 messages took {Stopwatch.Elapsed}");
            }
        }
    }
}