using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   class MyUtility
   {
      public static void AddMap(string key, Dictionary<string,int> dic)
      {
         key = key.ToLower();
         if (dic.ContainsKey(key))
         {
            dic[key] = dic[key] + 1;
         }
         else
         {
            dic.Add(key, 1);
         }
      }

      public static Dictionary<string,int> SortedMap(Dictionary<string, int> dic)
      {
         return dic.OrderBy(x => -x.Value).ToDictionary(x => x.Key, x => x.Value);
      }

      public static void OutputMapBySort(Dictionary<string, int> dic, string output)
      {
         dic = SortedMap(dic);
         using (StreamWriter sw = new StreamWriter(output))
         {
            foreach (KeyValuePair<string, int> entity in dic)
            {
               sw.WriteLine(entity.Key+"\t"+entity.Value);
            }
            sw.Close();
         }
      }
   }
}
