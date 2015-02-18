using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml.Serialization;
using MapReduce.Api;

namespace MapReduce.Serialization
{
    public class XmlSerializer<T> : StreamBasedSerializer<T>
    {
        readonly XmlSerializer _xmlSerializer = new XmlSerializer(typeof(T));

        public override T DeserializeOne(Stream source)
        {
            return (T)_xmlSerializer.Deserialize(source);
        }

        public override void SerializeOne(Stream destination, T instance)
        {
            _xmlSerializer.Serialize(destination, instance);
        }

        public override bool IsSerializable()
        {
            return true;
        }
    }

    public class TextLineSerializer : TextLineWriter<string>, IObjectSerializer<string>
    {
        public IEnumerable<string> Deserialize(Stream source)
        {
            var sr = new StreamReader(source);
            while (!sr.EndOfStream)
            {
                yield return sr.ReadLine();
            }
        }

        public string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }
        public bool IsSerializable()
        {
            return true;
        }
    }

    public class CsvSerializer : CsvWriter<string>, IObjectSerializer<string[]>
    {
        public IEnumerable<string[]> Deserialize(Stream source)
        {
            var sr = new StreamReader(source);
            while (!sr.EndOfStream)
            {
                var line = sr.ReadLine();
                if (line != null)
                    yield return line.Split(';');
            }
        }

        public string[] Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data).Split(';');
        }

        public bool IsSerializable()
        {
            return true;
        }

    }
}
