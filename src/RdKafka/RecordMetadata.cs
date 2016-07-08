namespace RdKafka
{
    public struct RecordMetadata
    {
        public long Offset;

        public int Partition;

        public string Topic;
    }
}
