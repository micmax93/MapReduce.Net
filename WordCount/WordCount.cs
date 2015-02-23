using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MapReduce.Api;
using MapReduce.Serialization;
using ProtoBuf;

namespace WordCount
{
    [ProtoContract]
    public class Counter
    {
        [ProtoMember(1)]
        public string Word;
        [ProtoMember(2)]
        public int Count;

        public override string ToString()
        {
            return Word + " = " + Count;
        }
    }
    public class WcMapReduce : MapReduce<string, string, int, Counter>
    {
        
        public override void Map(string input, ObjectCollector<string, int> objectCollector)
        {
            foreach (var word in input.Split(' ', ',', '.'))
            {
                objectCollector(word, 1);
            }
        }

        public override Counter Reduce(string key, IEnumerable<int> values)
        {
            int sum = values.Sum();
            return new Counter() {Word = key, Count = sum};
        }

        public override IEnumerable<Counter> SortOutput(IEnumerable<Counter> results)
        {
            return results.OrderBy(c => c.Word);
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

        public override IObjectSerializer<Counter> GetOutSerializer()
        {
            return new ProtoBufSerializer<Counter>();
        }

        public override IObjectWriter<Counter> GetOutWriter()
        {
            return new TextLineWriter<Counter>();
        }
    }
}
