using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class BreaknessReducer : Reducer
{
   public override Schema Produces(string[] requestedColumns, string[] args, Schema inputSchema)
   {
      return new Schema("uID, c1, c2, c3");
   }

   public override IEnumerable<Row> Reduce(RowSet input, Row outputRow, string[] args)
   {
      string uID = "";
      int cnt = 0;

      foreach (Row row in input.Rows)
      {
         uID = row["uID"].String;
         cnt++;
      }

      outputRow[0].Set(uID);
      outputRow[1].Set(cnt + "");
      outputRow[2].Set(cnt + "");
      outputRow[3].Set(cnt + "");

      yield return outputRow;
   }
}