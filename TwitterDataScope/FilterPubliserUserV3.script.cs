using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;



public class TwitterPublisherUserAgg_StringString : Aggregate2<string,string, bool>
{
   bool isPublisherUser;
   Dictionary<string, int> siteMap;
   int thresholds = 2;

   public override void Initialize()
   {
      isPublisherUser = false;
      siteMap = new Dictionary<string, int>();
   }

   public override void Add(string text, string domain)
   {
      char splitChar1 = ' ';
      char splitChar2 = '.';
      string[] words = text.Split(splitChar1);
      foreach (string word in words)
      {
         string[] site = word.Split(splitChar2);
         if (site.Length > 1)
         {
            siteMap.Add(site[0], siteMap.ContainsKey(site[0]) ? siteMap[site[0]] + 1 : 1);
         }
      }
      if (domain.Split(splitChar2).Length > 1)
      {
         siteMap.Add(domain, siteMap.ContainsKey(domain) ? siteMap[domain] + 1 : 1);
      }
   }

   public override bool Finalize()
   {
      foreach (KeyValuePair<string, int> entry in siteMap)
      {
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
            // if hashSet contain the uname, it means user may be publisher user.
            yield return output_row;
         }
      }
   }
}