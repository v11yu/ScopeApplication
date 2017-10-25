using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
public class CategoryProcessor : Processor
{
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
         string tmp = output_row[0] + " " + output_row[1];
         output_row[1].Set(tmp);
         yield return output_row;
      }
   }
}