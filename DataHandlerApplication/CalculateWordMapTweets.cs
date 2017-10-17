using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   public class CalculateWordMapTweets
   {
      static HashSet<string> onlyOneWord = null;
      static void AddMap(Dictionary<string, int> map, string k)
      {
         if (map.ContainsKey(k))
         {
            map[k] = map[k] + 1;
         }
      }
      public static HashSet<string> GetOnlyOneWord(string path)
      {
         if(onlyOneWord == null)
         {
            onlyOneWord = new HashSet<string>();
            using (StreamReader sr = new StreamReader(path))
            {
               string line = null;
               while (null != (line = sr.ReadLine()))
               {
                  string[] words = line.Split(' ');
                  int tmp = 0;
                  if (words.Length == 1) onlyOneWord.Add(words[0]);
               }
               sr.Close();
            }
         }
         return onlyOneWord;
      }
      public static void calculateWordMapTweets(string[] args)
      {
         Dictionary<string, int> map = new Dictionary<string, int>();
         HashSet<string> stopSet = new HashSet<string>();
         
         if (args.Length != 5)
         {
            throw new Exception("need 2 params");
         }
         string query = args[0];
         string tweets = args[1];
         string stopword = args[2];
         string output = args[3];
         int threshold = 30000;
         if (!int.TryParse(args[4], out threshold)) threshold = 30000;

         /*

         string path = @"D:\News_Team\Query-author-in-Twitter";
         string query = path+"QuerySets.tsv";
         string tweets = path+"recent-filtered-tweets.tsv";
         string stopword = path+ "StopWord.tsv";
         string output = path + "wordCount.tsv";
         int threshold = 30000;
         */
         using (StreamReader sr = new StreamReader(stopword))
         {
            string line = null;
            while (null != (line = sr.ReadLine()))
            {
               string[] words = line.Split(' ');
               foreach (string word in words)
               {
                  stopSet.Add(word);
               }
            }
            sr.Close();
         }
         using (StreamReader sr = new StreamReader(query))
         {
            string line = null;
            while (null != (line = sr.ReadLine()))
            {
               string[] words = line.Split(' ');
               int tmp = 0;
               if (words.Length > 4) continue;
               foreach (string word in words)
               {
                  if (!stopSet.Contains(word) && !map.ContainsKey(word) && !int.TryParse(word, out tmp) && word.Length > 1)
                  {
                     map.Add(word, 0);
                  }
               }
            }
            sr.Close();
         }
         Console.WriteLine("map size:" + map.Count + " set size:" + stopSet.Count +" threshold:"+ threshold);
         using (StreamReader sr = new StreamReader(tweets))
         {
            int cnt = 0;
            string line = null;
            while (null != (line = sr.ReadLine()))
            {

               string[] cols = line.Split('\t');
               if (cols.Length < 5) continue;
               string[] words = cols[3].Split(' ');
               foreach (string word in words)
               {
                  AddMap(map, word);
               }
               if (cnt++ % 1000 == 0)
               {
                  Console.WriteLine(cnt);
               }
            }
            sr.Close();
         }
         map = map.OrderBy(x => -x.Value).ToDictionary(x => x.Key, x => x.Value);
         using (StreamWriter sw = new StreamWriter(output))
         {
            foreach (KeyValuePair<string, int> entity in map)
            {
               if (entity.Value < threshold)
               {
                  sw.WriteLine(entity.Key);
               }
            }
            sw.Close();
         }
      }
   }
}
