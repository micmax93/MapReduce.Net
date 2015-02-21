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
    public class Reducer
    {
        private IMapReduce mapReduce;
        public Reducer(IMapReduce implementation)
        {
            mapReduce = implementation;
        }

        public byte[] Reduce(byte[] key, IEnumerable<byte[]> values)
        {
            return mapReduce.DoReduce(key, values);
        }

        public void Write(Stream stream, IEnumerable<byte[]> results)
        {
            mapReduce.Write(stream, results);
        }
    }
}
