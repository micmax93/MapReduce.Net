using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Xml.Serialization;
using MapReduce.Api;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;

namespace MapReduce.DataAccess
{
    public class RedisDb
    {
        public delegate void CompletionTrigger(string id);
        protected IDatabase db;
        protected IServer server;
        protected string node;
        public RedisDb(string host, string nodeName)
        {
            var conn = ConnectionMultiplexer.Connect(host + ",allowAdmin=true");
            db = conn.GetDatabase();
            server = conn.GetServer(host);
            node = nodeName;
        }

        private object HashGet(RedisKey key, RedisValue field)
        {
            var val = db.HashGet(key, field);
            if (!val.HasValue) return null;
            return val;
        }
        private void HashSet(RedisKey key, RedisValue field, object value)
        {
            if (value == null) db.HashDelete(key, field);
            else db.HashSet(key, field, (RedisValue)value);
        }

        public string JobId
        {
            get { return (string) HashGet("node_" + node, "id"); }
            set { HashSet("node_" + node, "id", value); }
        }
        public byte[] CurrentTask
        {
            get { return (byte[]) HashGet("node_" + node, "task"); }
            set { HashSet("node_" + node, "task", value); }
        }
        public string JobAssembly
        {
            get
            {
                string id = JobId;
                if (id == null) return null;
                return (string) HashGet("job_" + id, "assembly");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "assembly", value);
            }
        }

        public IMapReduce LoadAssembly(string typeName = null)
        {
            string assemblyFile = JobAssembly;
            Assembly dll = Assembly.LoadFile(assemblyFile);
            if (typeName == null)
            {
                typeName = Path.GetFileNameWithoutExtension(assemblyFile);
            }
            Type type = dll.GetType(typeName);
            var method = type.GetConstructor(new Type[] { });
            return (IMapReduce)method.Invoke(new object[] { });
        }

        public string JobOutFile
        {
            get
            {
                string id = JobId;
                if (id == null) return null;
                return (string) HashGet("job_" + id, "outfile");
            }
            set
            {
                string id = JobId;
                if (id == null) return;
                HashSet("job_" + id, "outfile", value);
            }
        }

        public void CreateTask(string id, string assemblyFile, string outPath)
        {
            JobId = id;
            JobAssembly = assemblyFile;
            JobOutFile = outPath;
        }

        public void AddMapTasks(string id, RedisValue[] tasks)
        {
            db.ListRightPush("map_" + id, tasks);
            db.HashSet("counters", "map_" + id, tasks.Length);
            db.ListRightPush("map", id);
        }

        public bool AssignMapTask()
        {
            if (CurrentTask != null) return true;
            string id = db.ListGetByIndex("map", 0);
            var task = db.ListLeftPop("map_" + id);
            if (!task.HasValue) return false;
            JobId = id;
            CurrentTask = task;
            return true;
        }

        public Stream GetDataStream()
        {
            RedisValue path = CurrentTask;
            return File.OpenRead(path);
        }

        public BinaryCollector KeyValueDataCollector
        {
            get
            {
                RedisKey prefix = "kv_" + JobId + "_";
                return (key, value) => db.ListRightPush(prefix.Append(key), value);
            }
        }

        public void FinishMapTask(CompletionTrigger trigger=null)
        {
            string id = JobId;
            var counter = db.HashDecrement("counters", "map_" + id);
            CurrentTask = null;
            if (counter <= 0 && trigger!=null)
            {
                trigger(id);
            }
        }

        public void CreateReduceTasks(string id)
        {
            var keys = server.Keys(pattern: "kv_" + id + "_*");
            var vals = keys.Cast<byte[]>().Select(key => (RedisValue) key).ToArray();
            db.ListRightPush("reduce_" + id, vals);
            db.HashSet("counters", "reduce_" + id, vals.Length);
            db.ListRightPush("reduce", id);
        }
        public bool AssignReduceTask()
        {
            string id = db.ListGetByIndex("reduce", 0);
            var task = db.ListLeftPop("reduce_" + id);
            if (!task.HasValue) return false;
            JobId = id;
            CurrentTask = task;
            return true;
        }

        public KeyValuePair<byte[],IEnumerable<byte[]>> GetReduceTask()
        {
            byte[] key = CurrentTask;
            var vals = db.ListRange(key);
            return new KeyValuePair<byte[], IEnumerable<byte[]>>(key, vals.Cast<byte[]>());
        }

        public void FinishReduceTask(byte[] result, CompletionTrigger trigger = null)
        {
            string id = JobId;
            db.ListRightPush("out_" + id, result);

            byte[] taskKey = CurrentTask;
            db.KeyDelete(taskKey);
            CurrentTask = null;

            var counter = db.HashDecrement("counters", "reduce_" + id);
            if (counter <= 0 && trigger != null)
            {
                trigger(id);
            }
        }
    }
}
