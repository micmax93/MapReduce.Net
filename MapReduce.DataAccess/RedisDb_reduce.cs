using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using StackExchange.Redis;

namespace MapReduce.DataAccess
{
    public partial class RedisDb
    {
        public void CreateReduceTasks(string id)
        {
            var keys = server.Keys(pattern: "kv_" + id + "_*");
            var vals = keys.Cast<byte[]>().Select(key => (RedisValue)key).ToArray();
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

        public KeyValuePair<byte[], IEnumerable<byte[]>> GetReduceTask()
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
