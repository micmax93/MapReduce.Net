using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace MapReduce.Api
{
    public delegate void BinaryCollector(byte[] key, byte[] value);
    public delegate void ObjectCollector<TKey,TValue>(TKey key, TValue value);    
    public interface IMapReduce
    {
        IEnumerable<object> Read(Stream stream);
        void DoMap(object input, BinaryCollector binaryCollector);
        byte[] DoReduce(byte[] key, IEnumerable<byte[]> values);
        void Write(Stream stream, IEnumerable<byte[]> instances);
    }

    public interface IMapReduce<TIn, TKey, TValue, TOut>
    {
        void Map(TIn input, ObjectCollector<TKey, TValue> objectCollector);
        TOut Reduce(TKey key, IEnumerable<TValue> values);

        IObjectWriter<TOut> GetOutWriter();
        IObjectReader<TIn> GetInReader();
        IObjectSerializer<TKey> GetKeySerializer();
        IObjectSerializer<TValue> GetValueSerializer();
        IObjectSerializer<TOut> GetOutSerializer();
    }
}
