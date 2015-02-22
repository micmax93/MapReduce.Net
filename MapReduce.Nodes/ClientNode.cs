using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MapReduce.DataAccess;

namespace MapReduce.Nodes
{
    public class ClientNode
    {
        RedisDb db;
        public ClientNode(string nodeName="client")
        {
            var host = ConfigurationManager.ConnectionStrings["redisDb"].ConnectionString;
            db = new RedisDb(host, nodeName);
        }
        public void SpawnNewJob(string user, string jobName, string assemblyFile, string inputPath, string outputFile)
        {
            
            if (Directory.Exists(inputPath))
            {
                SpawnNewJob(user, jobName, assemblyFile, Directory.GetFiles(inputPath), outputFile);
            }
            else
            {
                SpawnNewJob(user, jobName, assemblyFile, new[] {inputPath}, outputFile);
            }
        }
        public void SpawnNewJob(string user, string jobName, string assemblyFile, IEnumerable<string> inputFiles, string outputFile)
        {
            string id = db.CreateJob(user, jobName, assemblyFile, outputFile);
            db.CreateMapTasks(id, inputFiles);
            db.Publish("new_map", id);
        }

        public IEnumerable<KeyValuePair<string, string>> GetJobsInfo(string user)
        {
            return db.ListJobs(user).Select(j => new KeyValuePair<string, string>(j, db.GetJobStatus(j)));
        }

        public void RemoveJob(string id)
        {
            db.RemoveJob(id);
        }
    }
}
