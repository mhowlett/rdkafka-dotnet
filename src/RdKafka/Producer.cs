using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using RdKafka.Internal;

namespace RdKafka
{
    /// <summary>
    /// High-level, asynchronous message producer.
    /// </summary>
    public partial class Producer : Handle
    {
        public Producer(string brokerList) : this(null, brokerList) {}

        public Producer(Config config, string brokerList = null)
        {
            config = config ?? new Config();

            IntPtr cfgPtr = config.handle.Dup();
            LibRdKafka.conf_set_dr_msg_cb(cfgPtr, DeliveryReportDelegate);
            Init(RdKafkaType.Producer, cfgPtr, config.Logger);

            if (brokerList != null)
            {
                handle.AddBrokers(brokerList);
            }
        }

        private Dictionary<string, Topic> _topics = new Dictionary<string, Topic>();

        public Task<RecordMetadata> Send(string topic, byte[] data) {
            Topic topicHandle;
            if (!_topics.TryGetValue(topic, out topicHandle)) {
                topicHandle = new Topic(handle, this, topic, null);
                _topics.Add(topic, topicHandle);
            }
            var recordMetadata = topicHandle.Produce(data);
            return recordMetadata;
        }

        // Explicitly keep reference to delegate so it stays alive
        static LibRdKafka.DeliveryReportCallback DeliveryReportDelegate = DeliveryReportCallback;

        static void DeliveryReportCallback(IntPtr rk,
                ref rd_kafka_message rkmessage, IntPtr opaque)
        {
            // msg_opaque was set by Topic.Produce
            var gch = GCHandle.FromIntPtr(rkmessage._private);
            var deliveryCompletionSource = (TaskCompletionSource<RecordMetadata>) gch.Target;
            gch.Free();

            if (rkmessage.err != 0)
            {
                deliveryCompletionSource.SetException(
                    RdKafkaException.FromErr(
                        rkmessage.err,
                        "Failed to produce message"));
                return;
            }

            deliveryCompletionSource.SetResult(new RecordMetadata() {
                Topic = "Some Topic",
                Offset = rkmessage.offset,
                Partition = rkmessage.partition
            });
        }

        public override void Dispose() {
            foreach (var t in _topics) {
                t.Value.Dispose();
            }
            base.Dispose();
        }
    }
}
