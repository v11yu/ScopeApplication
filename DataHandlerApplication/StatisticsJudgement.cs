using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   class StatisticsJudgement
   {
      public static void statisticsJudgement(string input, int N, int colNumberForUserName, int colNumberForRelevance)
      {
         string output = input + ".output";
         Dictionary<string, int> dic = new Dictionary<string, int>();
         Dictionary<string, int> udic = new Dictionary<string, int>();
         using (StreamReader sr = new StreamReader(input))
         {
            int cnt = 0;
            string line;
            char c = '	';
            int[] nums = new int[N + 1];
            for (int i = 0; i <= N; i++) nums[i] = 0;

            while ((line = sr.ReadLine()) != null)
            {
               if (cnt++ == 0) continue;
               string[] cols = line.Split(c);
               string relev = cols[colNumberForRelevance];
               string name = cols[colNumberForUserName];
               if (relev.Equals("Relevant"))
               {
                  MyUtility.AddMap(name, dic);
               }
               else
               {
                  MyUtility.AddMap(name, udic);
               }
            }
            StreamWriter sw = new StreamWriter(output);
            sw.WriteLine("total: " + cnt / N);

            foreach (KeyValuePair<string, int> entity in dic)
            {
               int x = entity.Value;
               nums[x]++;
            }

            int kk = cnt / 3;
            for (int i = N; i >= 1; i--)
            {
               kk -= nums[i];
               sw.WriteLine("Reve" + i + ": " + nums[i]);
            }
            sw.WriteLine("Reve" + 0 + ": " + kk);
            sw.WriteLine("Reve0 list show below:");
            foreach (KeyValuePair<string, int> entity in udic)
            {
               if(entity.Value == N)
               {
                  sw.WriteLine(entity.Key);
               }
            }
            sw.Close();
         }
      }
   }
}
