using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MapReduce.Api
{
    public delegate void BinaryCollector(byte[] key, byte[] value);
    public delegate void ObjectCollector<TKey,TValue>(TKey key, TValue value);    
    public interface IMapReduce
    {
        IEnumerable<object> Read(Stream stream);
        void DoMap(object input, BinaryCollector binaryCollector);
        object DoReduce(byte[] key, IEnumerable<byte[]> values);
        void Write(Stream stream, IEnumerable<object> instances);
    }

    public interface IMapReduce<TIn, TKey, TValue, TOut>
    {
        void Map(TIn input, ObjectCollector<TKey, TValue> objectCollector);
        TOut Reduce(TKey key, IEnumerable<TValue> values);

        IObjectWriter<TOut> GetOutWriter();
        IObjectReader<TIn> GetInReader();
        IObjectSerializer<TKey> GetKeySerializer();
        IObjectSerializer<TValue> GetValueSerializer();
    }

    public abstract class MapReduce<TIn, TKey, TValue, TOut> : IMapReduce<TIn, TKey, TValue, TOut>, IMapReduce
    {
        public abstract void Map(TIn input, ObjectCollector<TKey, TValue> objectCollector);
        public abstract TOut Reduce(TKey key, IEnumerable<TValue> values);
        public abstract IObjectWriter<TOut> GetOutWriter();
        public abstract IObjectReader<TIn> GetInReader();
        public abstract IObjectSerializer<TKey> GetKeySerializer();
        public abstract IObjectSerializer<TValue> GetValueSerializer();

        IObjectWriter<TOut> _outWriter;
        IObjectReader<TIn> _inReader;
        IObjectSerializer<TKey> _keySerializer;
        IObjectSerializer<TValue> _valueSerializer;
        protected MapReduce()
        {
            _outWriter = GetOutWriter();
            _inReader = GetInReader();
            _keySerializer = GetKeySerializer();
            _valueSerializer = GetValueSerializer();
        }

        private ObjectCollector<TKey, TValue> SerializationWrapper(BinaryCollector bc)
        {
            return (key, value) => bc(_keySerializer.Serialize(key), _valueSerializer.Serialize(value));
        }

        public IEnumerable<object> Read(Stream stream)
        {
            return (IEnumerable<object>) _inReader.Deserialize(stream);
        }

        public void DoMap(object input, BinaryCollector binaryCollector)
        {
            Map((TIn)input, SerializationWrapper(binaryCollector));
        }

        public object DoReduce(byte[] key, IEnumerable<byte[]> values)
        {
            return Reduce(_keySerializer.Deserialize(key), values.Select(vb => _valueSerializer.Deserialize(vb)));
        }

        public void Write(Stream stream, IEnumerable<object> instances)
        {
            _outWriter.Serialize(stream, (IEnumerable<TOut>) instances);
        }
    }
}
