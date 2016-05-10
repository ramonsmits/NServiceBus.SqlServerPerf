using System;
using System.Text;
using NServiceBus;

namespace Messages
{
    public class ProduceChocolateBar : ICommand
    {
        private static string chunk;

        public int LotNumber { get; set; }

        public int MaxLotNumber { get; set; }

        public string Chunk { get; set; }

        public ProduceChocolateBar()
        {
        }

        public ProduceChocolateBar(bool initialize)
        {
            Chunk = chunk;
        }

        public static void Initialize(int messageSizeInBytes)
        {
            if (string.IsNullOrEmpty(chunk))
            {
                var b = new byte[messageSizeInBytes];
                new Random().NextBytes(b);

                chunk = Encoding.UTF8.GetString(b);
            }
        }
    }
}