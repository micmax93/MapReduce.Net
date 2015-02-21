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

        public void ReduceDone(string id)
        {
            db.Publish("reducer", id);
        }

        public override void OnStart()
        {
            db.Subscribe("new_reduce", (ch, val) => Signal());
        }

        public override void OnStop()
        {
            db.Unsubscribe("new_reduce");
        }

        public override bool TryExecuteTask()
        {
            bool taskAvailable = db.AssignReduceTask();
            if (!taskAvailable) return false;
            var task = db.GetReduceTask();
            IMapReduce mapReduce = db.LoadAssembly();
            Reducer reducer = new Reducer(mapReduce);
            var result = reducer.Reduce(task.Key, task.Value);
            db.FinishReduceTask(result, ReduceDone);
            return true;
        }
    }
}
