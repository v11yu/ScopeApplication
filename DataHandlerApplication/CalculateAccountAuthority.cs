using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
    class CalculateAccountAuthority
    {
        public static void process(string input, string output)
        {
            using (StreamReader sr = new StreamReader(input))
            {
                string line;
                while (null != (line = sr.ReadLine()))
                { 

                }
            }
        }
    }
}
