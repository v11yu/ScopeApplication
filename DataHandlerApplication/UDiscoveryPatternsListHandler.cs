using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   class UDiscoveryPatternsListHandler
   {

      public static void GetSourceName(string input, string output)
      {
         Dictionary<string, int> dic = new Dictionary<string, int>();
         int cnt = 0;
         using (StreamReader sr = new StreamReader(input))
         {
            string line = null;
            while( null != (line = sr.ReadLine())){
               char spiltChar1 = ';';
               string[] tmp = line.Split(spiltChar1);
               if (tmp.Length < 2)
               {
                  Console.WriteLine("a");
                  continue;
               }
               string preStr = tmp[0];
               string[] tmp2 = preStr.Split(',');
               string[] urls = tmp2[0].Split('.');
               foreach (string url in urls)
               {
                  MyUtility.AddMap(url, dic);
               }
               string sourceName = tmp2[1].Split('=')[1];
               MyUtility.AddMap(sourceName, dic);
               urls = sourceName.Split('.');
               foreach (string url in urls)
               {
                  MyUtility.AddMap(url, dic);
               }
               if (cnt++ % 1000 == 0) Console.WriteLine(cnt);
            }
            MyUtility.OutputMapBySort(dic, output);
            Console.WriteLine(dic.Count);
            sr.Close();
         }
      }
   }
}
