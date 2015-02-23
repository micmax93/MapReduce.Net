using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MapReduce.Api;
using MapReduce.Serialization;

namespace WordCount
{
    public class WCTest: MapReduce<string, string, int, KeyValuePair<string, int>>
    {
        public override void Map(string input, ObjectCollector<string, int> objectCollector)
        {
            foreach (var word in input.Split(' ',',','.'))
            {
                objectCollector(word, 1);
            }
        }

        public override KeyValuePair<string, int> Reduce(string key, IEnumerable<int> values)
        {
            int sum = values.Sum();
            return new KeyValuePair<string, int>(key, sum);
        }

        public override IEnumerable<KeyValuePair<string, int>> SortOutput(IEnumerable<KeyValuePair<string, int>> results)
        {
            return results.OrderBy(r => r.Key);
        }


        public override IObjectWriter<KeyValuePair<string, int>> GetOutWriter()
        {
            return new KeyValueWriter<string, int>();
        }

        public override IObjectReader<string> GetInReader()
        {
            return new TextLineSerializer();
        }

        public override IObjectSerializer<string> GetKeySerializer()
        {
            return new TextLineSerializer();
        }

        public override IObjectSerializer<int> GetValueSerializer()
        {
            return new ProtoBufSerializer<int>();
        }

        public override IObjectSerializer<KeyValuePair<string, int>> GetOutSerializer()
        {
            return new DefaultSerializer<KeyValuePair<string, int>>();
        }
    }
}
