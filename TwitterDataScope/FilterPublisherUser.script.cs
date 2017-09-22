using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class TextProcessor : Processor
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
         string uname = input_row[4].String;
         if (String.Compare(GetWordSet(args[0]).Contains(uname).ToString(),args[1],true) == 0)
         {
            // if hashSet contain the uname, it means user may be publisher user.
            yield return output_row;
         }
      }
   }
}