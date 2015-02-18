using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using MapReduce.Api;

namespace MapReduce.Serialization
{
    public class TextLineWriter<T> : IObjectWriter<T>
    {
        public void Serialize(Stream destination, IEnumerable<T> instances)
        {
            StreamWriter writer = new StreamWriter(destination);
            foreach (var instance in instances)
            {
                writer.WriteLine(instance.ToString());
            }
        }

        public byte[] Serialize(T item)
        {
            return Encoding.UTF8.GetBytes(item.ToString());
        }
    }

    public class CsvWriter<T> : IObjectWriter<T[]>
    {
        public void Serialize(Stream destination, IEnumerable<T[]> instances)
        {
            StreamWriter writer = new StreamWriter(destination);
            foreach (var entry in instances)
            {
                writer.WriteLine(String.Join(";", entry));
            }
        }

        public byte[] Serialize(T[] item)
        {
            return Encoding.UTF8.GetBytes(String.Join(";", item));
        }
    }
}
