using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MapReduce.Api
{
    public abstract class MapReduce<TIn, TKey, TValue, TOut> : IMapReduce<TIn, TKey, TValue, TOut>, IMapReduce
    {
        public abstract void Map(TIn input, ObjectCollector<TKey, TValue> objectCollector);
        public abstract TOut Reduce(TKey key, IEnumerable<TValue> values);
        public abstract IObjectWriter<TOut> GetOutWriter();
        public abstract IObjectReader<TIn> GetInReader();
        public abstract IObjectSerializer<TKey> GetKeySerializer();
        public abstract IObjectSerializer<TValue> GetValueSerializer();
        public abstract IObjectSerializer<TOut> GetOutSerializer();

        IObjectWriter<TOut> _outWriter;
        IObjectReader<TIn> _inReader;
        IObjectSerializer<TKey> _keySerializer;
        IObjectSerializer<TValue> _valueSerializer;
        IObjectSerializer<TOut> _outSerializer;

        protected MapReduce()
        {
            _outWriter = GetOutWriter();
            _inReader = GetInReader();
            _keySerializer = GetKeySerializer();
            _valueSerializer = GetValueSerializer();
            _outSerializer = GetOutSerializer();
        }

        private ObjectCollector<TKey, TValue> SerializationWrapper(BinaryCollector bc)
        {
            return (key, value) => bc(_keySerializer.Serialize(key), _valueSerializer.Serialize(value));
        }

        public IEnumerable<object> Read(Stream stream)
        {
            return _inReader.Deserialize(stream).Select(i => (object)i);
        }

        public void DoMap(object input, BinaryCollector binaryCollector)
        {
            Map((TIn)input, SerializationWrapper(binaryCollector));
        }

        public byte[] DoReduce(byte[] key, IEnumerable<byte[]> values)
        {
            var result = Reduce(_keySerializer.Deserialize(key), values.Select(vb => _valueSerializer.Deserialize(vb)));
            return _outSerializer.Serialize(result);
        }

        public virtual IEnumerable<TOut> SortOutput(IEnumerable<TOut> results)
        {
            return results;
        }

        public void Write(Stream stream, IEnumerable<byte[]> instances)
        {
            var results = instances.Select(i => _outSerializer.Deserialize(i));
            results = SortOutput(results);
            _outWriter.Serialize(stream, results);
        }
    }
}