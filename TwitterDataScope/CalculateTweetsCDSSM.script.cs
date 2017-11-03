using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using DeepSemanticLearning;

public class TwitterProcessor : Processor
{
   private static DeepSemanticModel docModel = null;
   private DeepSemanticModel GetDocModel(string docModelFile, string stopWordsFile)
   {
      if (docModel == null)
      {
         docModel = new DeepSemanticModel();
         if (!docModel.ModelLoader(docModelFile))
         {
            Console.WriteLine("Error: CDSSM document-side model load failed ...");
            docModel = null;
            return docModel;
         }
         if (!TextNormalizer.StopWordLoader(stopWordsFile))
         {
            Console.WriteLine("Error: stop words list load failed ...");
            docModel = null;
            return docModel;
         }
      }
      return docModel;
   }
   public string GetCDSSM(string doc, string docModelFile, string stopWordsFile)
   {
      List<float> CDSSMFeature = new List<float>();
      DeepSemanticModel dm = GetDocModel(docModelFile, stopWordsFile);
      if (null == dm)
      {
         return null;
      }
      string normalizedDoc = TextNormalizer.Normalizer(TextNormalizer.StopWordsFilter(doc.Replace('-', ' ').Replace('\'', ' ').ToLowerInvariant().Trim()));
      if (!docModel.SemanticFeatureEmbedding(normalizedDoc, out CDSSMFeature))
      {
         Console.WriteLine("Error: document {0} embedding failed ...", doc);
         return null;
      }
      return string.Join(",", CDSSMFeature);
   }
   static int cnt = 0;
   public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
   {
      var output_schema = input_schema.Clone();
      var newcol = new ColumnInfo("tCDSSM", typeof(string));
      output_schema.Add(newcol);
      return output_schema;
   }
   public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
   {
      foreach (Row input_row in input_rowset.Rows)
      {
         cnt++;
         if (cnt % 1000 == 0) Console.WriteLine(cnt);
         input_row.CopyTo(output_row);
         string doc = input_row["tText"].String;
         string CDSSM = GetCDSSM(doc, args[0], args[1]);
         if (CDSSM != null)
         {
            output_row["tCDSSM"].Set(CDSSM);
            yield return output_row;
         }
      }
   }
}