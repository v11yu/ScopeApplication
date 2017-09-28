using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;



public class TwitterPublisherUserAgg_StringString : Aggregate2<string, string, bool>
{
   bool isPublisherUser;
   Dictionary<string, int> siteMap;
   int thresholds = 20;
   int count = 0;
   string[] blackList = { "youtube.com", "twitter.com", "facebook.com" };
   HashSet<string> blackSet = new HashSet<string>();

   public override void Initialize()
   {
      isPublisherUser = false;
      siteMap = new Dictionary<string, int>();
      count = 0;
      blackSet = new HashSet<string>();
      foreach (string w in blackList) blackSet.Add(w);
   }
   void AddMap(string key)
   {
      if (siteMap.ContainsKey(key))
      {
         siteMap[key] = siteMap[key] + 1;
      }
      else
      {
         siteMap.Add(key, 1);
      }
   }
   public override void Add(string text, string domain)
   {
      
      char splitChar2 = '.';
      /*
      char splitChar1 = ' ';
      string[] words = text.Split(splitChar1);
      foreach (string word in words)
      {
         string[] site = word.Split(splitChar2);
         if (site.Length > 1)
         {
            AddMap(site[0]);
         }
      }*/
      if (domain.Split(splitChar2).Length > 1 && !blackSet.Contains(domain))
      {
         AddMap(domain);
      }
      count++;
   }

   public override bool Finalize()
   {
      foreach (KeyValuePair<string, int> entry in siteMap)
      {
         //if ((1.0*entry.Value/(1.0*count)) >= thresholds) isPublisherUser = true;
         if (entry.Value >= thresholds) isPublisherUser = true;
      }
      return isPublisherUser;
   }
}

public class FilterPublisherByUserNameProcessor : Processor
{
   public static HashSet<string> wordSet = null;
   HashSet<string> GetWordSet(string path)
   {
      if (wordSet == null)
      {
         wordSet = new HashSet<string>();
         string[] querys = File.ReadAllLines(path);
         foreach (string query in querys)
         {
            string site = query.Split('\t')[1];
            wordSet.Add(site);
            wordSet.Add(site.Split('.')[0]);
         }
      }
      return wordSet;
   }
   public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
   {
      var output_schema = input_schema.Clone();
      var newcol = new ColumnInfo("isSiteUser", typeof(bool));
      output_schema.Add(newcol);
      return output_schema;
   }
   public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
   {
      foreach (Row input_row in input_rowset.Rows)
      {
         input_row.CopyTo(output_row);
         string uname = input_row[1].String;
         if (GetWordSet(args[0]).Contains(uname))
         {
            output_row[3].Set(true);
         }
         else
         {
            output_row[3].Set(false);
         }

         if (GetWordSet(args[0]).Contains(uname) || input_row[2].Boolean)
         {
            // if hashSet contain the uname, it means user may be publisher user.
            yield return output_row;
         }
      }
   }
}