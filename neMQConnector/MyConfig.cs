using System;
using System.Collections.Generic;
using System.Text;

namespace neMQConnector
{
    public class MyConfig
    {
        public string exchange { get; set; }
        public string hostname { get; set; }
        public string user { get; set; }
        public string pass { get; set; }

        /// <summary>
        /// This limits the number of Observer Tasks running concurrently
        /// </summary>
        public int maxTasks { get; set; }

        public MyConfig()
        {
            maxTasks = 8;
        }

    }
}
