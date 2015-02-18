using System;
using System.Collections.Generic;
using System.IO;

namespace MapReduce.Api
{
    public interface IObjectReader<T>
    {
        IEnumerable<T> Deserialize(Stream source);
        T Deserialize(byte[] data);
    }
    public interface IObjectWriter<T>
    {
        void Serialize(Stream destination, IEnumerable<T> instances);
        byte[] Serialize(T item);
    }

    public interface IObjectSerializer<T> : IObjectWriter<T>, IObjectReader<T>
    {
        bool IsSerializable();
    }
}
