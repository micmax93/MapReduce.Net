using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web;
using MapReduce.Api;

namespace MapReduce.Web.Models
{
    [Serializable]
    public class JobRequest
    {
        public string JobName;
        public string DllPath;
        public string InPath;
        public string OutFile;

        public bool TestDll()
        {
            try
            {
                Assembly dll = Assembly.Load(File.ReadAllBytes(DllPath));
                string typeName = Path.GetFileNameWithoutExtension(DllPath);
                Type type = dll.GetType(typeName);
                type.GetConstructor(new Type[] {});
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool Validate()
        {
            if (!TestDll()) return false;
            if (!Directory.Exists(InPath)) return false;
            if (Directory.GetFiles(InPath).Length==0) return false;
            
            try {File.Create(OutFile).Close();}
            catch (Exception) {return false;}
            
            return true;
        }
    }
}