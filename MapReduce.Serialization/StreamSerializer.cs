using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using MapReduce.Api;

namespace MapReduce.Serialization
{
    public abstract class StreamBasedSerializer<T> : IObjectSerializer<T>
    {
        public abstract T DeserializeOne(Stream source);
        public abstract void SerializeOne(Stream destination, T instance);
        public abstract bool IsSerializable();

        public byte[] Serialize(T item)
        {
            using (var stream = new MemoryStream())
            {
                SerializeOne(stream, item);
                return stream.ToArray();
            }
        }

        public T Deserialize(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                stream.Seek(0, SeekOrigin.Begin);
                return DeserializeOne(stream);
            }
        }
        public virtual void Serialize(Stream destination, IEnumerable<T> instances)
        {
            foreach (var instance in instances)
            {
                SerializeOne(destination, instance);
            }
        }

        public virtual IEnumerable<T> Deserialize(Stream source)
        {
            while (source.Position < source.Length - 1)
            {
                yield return DeserializeOne(source);
            }
        }
    }
}
