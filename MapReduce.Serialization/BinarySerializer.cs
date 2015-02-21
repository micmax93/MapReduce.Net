using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using MapReduce.Api;
using ProtoBuf;

namespace MapReduce.Serialization
{
    public class DefaultSerializer<T> : StreamBasedSerializer<T>
    {
        readonly IFormatter _formatter = new BinaryFormatter();

        public override T DeserializeOne(Stream source)
        {
            return (T)_formatter.Deserialize(source);
        }

        public override void SerializeOne(Stream destination, T instance)
        {
            _formatter.Serialize(destination, instance);
        }

        public override bool IsSerializable()
        {
            return typeof(T).IsSerializable;
        }
    }

    public class ProtoBufSerializer<T> : StreamBasedSerializer<T>
    {
        public override T DeserializeOne(Stream source)
        {
            return Serializer.DeserializeWithLengthPrefix<T>(source, PrefixStyle.Fixed32);
        }

        public override IEnumerable<T> Deserialize(Stream source)
        {
            return Serializer.DeserializeItems<T>(source, PrefixStyle.Fixed32, 0);
        }

        public override void SerializeOne(Stream destination, T instance)
        {
            Serializer.SerializeWithLengthPrefix(destination, instance, PrefixStyle.Fixed32);
        }

        public override void Serialize(Stream destination, IEnumerable<T> instances)
        {
            foreach (var instance in instances)
            {
                Serializer.SerializeWithLengthPrefix(destination, instance, PrefixStyle.Fixed32);
            }
        }

        public override bool IsSerializable()
        {
            return Serializer.NonGeneric.CanSerialize(typeof(T));
        }
    }
}