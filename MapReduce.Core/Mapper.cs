using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MapReduce.Api;

namespace MapReduce.Core
{
    public class Mapper
    {
        private IMapReduce mapReduce;
        private ConcurrentBag<KeyValuePair<byte[], byte[]>> dataCollector;

        public Mapper(IMapReduce implementation)
        {
            mapReduce = implementation;
            dataCollector = new ConcurrentBag<KeyValuePair<byte[], byte[]>>();
        }
        private void AddData(byte[] key, byte[] value)
        {
            dataCollector.Add(new KeyValuePair<byte[], byte[]>(key, value));
        }
        public KeyValuePair<byte[], byte[]>[] GetData()
        {
            return dataCollector.ToArray();
        }

        public void ReadAndMap(Stream stream)
        {
            Parallel.ForEach(mapReduce.Read(stream), o => mapReduce.DoMap(o, AddData));
        }
    }
}
