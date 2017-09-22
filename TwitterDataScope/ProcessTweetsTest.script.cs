using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class TwitterProcessor : Processor
{
   public static HashSet<string> wordSet = null;
   HashSet<string> GetWordSet(string path)
   {
      if(wordSet == null)
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
         string[] words = input_row[3].String.Split(splitChar);
         foreach(string word in words){
            isInQuery = isInQuery || GetWordSet(args[0]).Contains(word);
         }
         if (isInQuery) { yield return output_row; }
      }
   }
}

public class MyTsvExtractor : Extractor
{
   public override Schema Produces(string[] requested_columns, string[] args)
   {
      return new Schema(requested_columns);
   }

   public override IEnumerable<Row> Extract(StreamReader reader, Row output_row, string[] args)
   {
      char delimiter = '\t';
      string line;
      while ((line = reader.ReadLine()) != null)
      {
         var tokens = line.Split(delimiter);
         for (int i = 0; i < tokens.Length; ++i)
         {
            output_row[i].UnsafeSet(tokens[i]);
         }
         yield return output_row;
      }
   }
}