using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MapReduce.Nodes
{
    public abstract class Node
    {
        private object _lock = new object();
        private bool _active = true;
        public bool Active
        {
            get { return _active; }
            set { _active = value; Signal(); }
        }


        public void Wait()
        {
            lock (_lock)
            {
                Monitor.Wait(_lock);
            }
        }

        public void Signal()
        {
            lock (_lock)
            {
                Monitor.Pulse(_lock);
            }
        }

        public abstract bool TryExecuteTask();

        public void Run()
        {
            while (Active)
            {
                lock (_lock)
                {
                    bool ok = TryExecuteTask();
                    if (!ok) Wait();
                }
            }
        }
    }
}
