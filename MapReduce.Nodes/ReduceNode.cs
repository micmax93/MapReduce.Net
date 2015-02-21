using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MapReduce.Api;
using MapReduce.Core;
using MapReduce.DataAccess;

namespace MapReduce.Nodes
{
    public class ReduceNode : Node
    {
        private RedisDb db;

        public void ReduceDone(string id)
        {

        }

        public override bool TryExecuteTask()
        {
            bool taskAvailable = db.AssignReduceTask();
            if (!taskAvailable) return false;
            var task = db.GetReduceTask();
            IMapReduce mapReduce = db.LoadAssembly();
            Reducer reducer = new Reducer(mapReduce);
            var result = reducer.Reduce(task.Key, task.Value.Cast<byte[]>());
            db.FinishReduceTask(result, ReduceDone);
            return true;
        }
    }
}
