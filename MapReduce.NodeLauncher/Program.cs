using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MapReduce.Nodes;

namespace MapReduce.NodeLauncher
{
    class Program
    {
        static public Node GetNodeInteractive()
        {
            Node node;
            string nodeName = "";
            while (nodeName == "")
            {
                Console.Write("Please enter node name: ");
                nodeName = Console.ReadLine();
            }

            while (true)
            {
                Console.Write("Please enter node mode (map or reduce): ");
                var line = Console.ReadLine();
                if (line == "map")
                {
                    node = new MapNode(nodeName);
                    break;
                }
                else if (line == "reduce")
                {
                    node = new ReduceNode(nodeName);
                    break;
                }
            }
            return node;
        }

        static public Node GetNode(string[] args)
        {
            Node node;
            string nodeName = args[0];
            var mode = args[1];
            if (mode == "map")
            {
                node = new MapNode(nodeName);
            }
            else if (mode == "reduce")
            {
                node = new ReduceNode(nodeName);
            }
            else
            {
                Console.WriteLine("Invalid mode.");
                throw new Exception("Invalid mode");
            }
            return node;
        }

        static void Main(string[] args)
        {
            Node node;
            if (args.Length == 0) node = GetNodeInteractive();
            else node = GetNode(args);

            Thread thread = new Thread(node.Run);
            thread.Start();
            Console.WriteLine("Node started.");

            while (Console.ReadKey().Key != ConsoleKey.Escape) { }
            node.Active = false;
            Console.WriteLine("Exiting.");
            thread.Join();
        }
    }
}
