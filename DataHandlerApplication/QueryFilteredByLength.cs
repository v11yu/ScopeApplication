using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace DataHandlerApplication
{
   class QueryFilteredByLength
   {
      public static void filterQuery(string inputPath,string outputPath)
      {
         char splitChar = ' ';
         StreamWriter sw = new StreamWriter(outputPath);
         using (StreamReader sr = new StreamReader(inputPath))
         {
            string items;
            while(null != (items = sr.ReadLine()))
            {
               string[] words = items.Split(splitChar);
               if (words.Length > 4) continue;
               sw.WriteLine(items);
            }
            sr.Close();
         }
         sw.Close();
      }
   }
}
