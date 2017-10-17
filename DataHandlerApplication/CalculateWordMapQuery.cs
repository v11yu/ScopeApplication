using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   public class CalculateWordMapQuery
   {
      public static void calculateWordMapQuery(string[] args)
      {
         string path = @"D:\News_Team\Query-author-in-Twitter\";
         string query = path + "QuerySets.tsv";
         string wordmap = path + "word-map-tweets.tsv";
         string output = path + "word-map-query-30000.tsv";
         Dictionary<string, int> map = new Dictionary<string, int>();
         using(StreamReader sr = new StreamReader(wordmap))
         {
            string line = null;
            while(null!=(line = sr.ReadLine()))
            {
               string[] cols = line.Split('\t');
               string[] words = cols[0].Split(' ');
               foreach(string word in words)
               {
                  if (int.Parse(cols[1])>30000&&!map.ContainsKey(word)) map[word] = 0;
               }
            }
            sr.Close();
         }

         using (StreamReader sr = new StreamReader(query))
         {
            string line = null;
            while (null != (line = sr.ReadLine()))
            {
               string[] cols = line.Split('\t');
               string[] words = cols[0].Split(' ');
               foreach (string word in words)
               {
                  if (map.ContainsKey(word)) map[word] = map[word]+1;
               }
            }
            sr.Close();
         }

         map = map.OrderBy(x => -x.Value).ToDictionary(x => x.Key, x => x.Value);
         using (StreamWriter sw = new StreamWriter(output))
         {
            foreach (KeyValuePair<string, int> entity in map)
            {
               sw.WriteLine(entity.Key + "\t" + entity.Value);
            }
            sw.Close();
         }
      }
   }
}
