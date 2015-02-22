using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MapReduce.Api;
using StackExchange.Redis;

namespace MapReduce.DataAccess
{
    public partial class RedisDb
    {
        public void CreateMapTasks(string id, string[] tasks)
        {
            db.ListRightPush("map_" + id, tasks.Cast<RedisValue>().ToArray());
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

        public void FinishMapTask(CompletionTrigger trigger = null)
        {
            string id = JobId;
            var counter = db.HashDecrement("counters", "map_" + id);
            CurrentTask = null;
            if (counter <= 0 && trigger != null)
            {
                trigger(id);
            }
        }

        public void CloseMap(string id)
        {
            HashSet("counters", "map_" + id, null);
            if (db.ListGetByIndex("map", 0) == id)
            {
                string _id = db.ListLeftPop("map");
                if (_id != id) db.ListLeftPush("map", _id);
            }
            db.KeyDelete("map_" + id);
        }
    }
}
