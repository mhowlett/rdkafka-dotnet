using System;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using RdKafka;

namespace SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var topicNames = Enumerable.Range(1, 100).Select(a => $"tmp_topic_{a}");

            var startTime = DateTime.Now;
            using (Producer producer = new Producer("localhost:9092")) {
                byte[] data = Encoding.UTF8.GetBytes("hello world");

                int cnt = 100000;
                while (cnt-- > 0) {
                    foreach (var t in topicNames) {
                        producer.Send(t, data);
                    }
                }
            }
            Console.WriteLine(DateTime.Now - startTime);
        }
    }
}
