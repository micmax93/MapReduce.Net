﻿using System;
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
            string prefix = "kv_" + id + "_";
            var keys = server.Keys(pattern: prefix + "*");
            keys = keys.Select(k => ((string) k).Remove(0, prefix.Length)).Select(k => (RedisKey) k);
            var vals = keys.Select(k => (byte[])k).Select(key => (RedisValue)key).ToArray();
            db.ListRightPush("reduce_" + id, vals);
            db.HashSet("counters", "reduce_" + id, vals.Length);
            db.ListRightPush("reduce", id);
            db.Publish("new_reduce", id);
        }
        public bool AssignReduceTask()
        {
            string id = db.ListGetByIndex("reduce", 0);
            var task = db.ListLeftPop("reduce_" + id);
            if (!task.HasValue) return false;
            JobId = id;
            CurrentTask = task;
            JobStatus = "reduce";
            return true;
        }

        public KeyValuePair<byte[], IEnumerable<byte[]>> GetReduceTask()
        {
            RedisKey key = CurrentTask;
            string prefix = "kv_" + JobId + "_";
            var vals = db.ListRange(key.Prepend(prefix));
            return new KeyValuePair<byte[], IEnumerable<byte[]>>(key, vals.Select(v => (byte[]) v));
        }

        public void FinishReduceTask(byte[] result, CompletionTrigger trigger = null)
        {
            string id = JobId;
            db.ListRightPush("out_" + id, result);

            RedisKey taskKey = CurrentTask;
            string prefix = "kv_" + JobId + "_";
            db.KeyDelete(taskKey.Prepend(prefix));
            CurrentTask = null;

            var counter = db.HashDecrement("counters", "reduce_" + id);
            if (counter <= 0 && trigger != null)
            {
                trigger(id);
            }
        }

        public IEnumerable<byte[]> GetOutData(string id)
        {
            return db.ListRange("out_" + id).Select(v => (byte[])v);
        }
        public void CloseOutData(string id)
        {
            db.KeyDelete("out_" + id);
        }

        public void CloseReduce(string id)
        {
            HashSet("counters", "reduce_" + id, (string)null);
            if (db.ListGetByIndex("reduce", 0) == id)
            {
                string _id = db.ListLeftPop("reduce");
                if (_id != id) db.ListLeftPush("reduce", _id);
            }
            db.KeyDelete("reduce_" + id);

            var next = db.ListGetByIndex("reduce", 0);
            if (next.HasValue) db.Publish("new_reduce", (string)next);
        }
    }
}
