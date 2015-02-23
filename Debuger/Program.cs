using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using MapReduce.Api;
using MapReduce.Core;
using MapReduce.DataAccess;
using MapReduce.Nodes;
using MapReduce.Serialization;
using ProtoBuf;
using StackExchange.Redis;

namespace Debuger
{
    public class Program
    {
        
        static void Main(string[] args)
        {
            ClientNode client = new ClientNode();
            string id = client.SpawnNewJob("micmax93", "TestJob",
                @"F:\workspace\MapReduce.Net\WordCount\bin\Debug\WordCount.WcMapReduce.dll",
                @"F:\workspace\MapReduce.Net\WordCount\bin\Debug\in\",
                @"F:\workspace\MapReduce.Net\WordCount\bin\Debug\out.txt");
            Console.WriteLine(id);
        }
    }
}
