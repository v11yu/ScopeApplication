using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   static class RandomNumberOfRecord
   {

      public static void GetRandomFile(string input, string output,int N)
      {
         string[] res = new string[N];
         int n = 1;
         Random rng = new Random();
         using (StreamReader sr = new StreamReader(input))
         {
            string line;
            while(null != (line = sr.ReadLine()))
            {
               if (n <= N) res[n - 1] = line;
               else
               {
                  int k = rng.Next(n);
                  if (k < N)
                  {
                     res[k] = line;
                  }
               }
               n++;
            }
            StreamWriter sw = new StreamWriter(output);
            foreach(string item in res)
            {
               sw.WriteLine(item);
            }
            sw.Close();
            sr.Close();
         }
      }
   }
}
