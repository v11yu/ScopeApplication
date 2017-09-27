using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class TwitterProcessor : Processor
{
   public static HashSet<string> wordSet = null;
   public static HashSet<string> stopWordSet = null;
   HashSet<string> GetWordSet(string path)
   {
      if (wordSet == null)
      {
         wordSet = new HashSet<string>();
         string[] querys = File.ReadAllLines(path);
         foreach (string query in querys)
         {
            string[] words = query.Split(' ');
            foreach (string word in words)
            {
               wordSet.Add(word);
            }
         }
      }
      return wordSet;
   }
   HashSet<string> GetStopWordSet(string path)
   {
      if (stopWordSet == null)
      {
         stopWordSet = new HashSet<string>();
         string[] stopWords = File.ReadAllLines(path);
         foreach (string wordline in stopWords)
         {
            string[] words = wordline.Split(' ');
            foreach (string word in words)
            {
               stopWordSet.Add(word);
            }
         }
      }
      return stopWordSet;
   }
   public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
   {
      var output_schema = input_schema.Clone();
      return output_schema;
   }
   public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
   {
      char splitChar = ' ';
      foreach (Row input_row in input_rowset.Rows)
      {
         input_row.CopyTo(output_row);
         bool isInQuery = false;
         int tmpNum = 0;
         string[] words = input_row[3].String.Split(splitChar);
         foreach (string word in words)
         {
            if (!GetStopWordSet(args[1]).Contains(word)
               && !int.TryParse(word, out tmpNum)) isInQuery = isInQuery || GetWordSet(args[0]).Contains(word);
         }
         if (isInQuery) { yield return output_row; }
      }
   }
}