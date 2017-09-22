using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using DeepSemanticLearning;

namespace DataHandlerApplication
{
   class CalculateCDSSM
   {
      private static DeepSemanticModel docModel = null;
      private static DeepSemanticModel GetDocModel(string docModelFile,string stopWordsFile)
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
            }
         }
         return docModel;
      }
      public static string GetCDSSM(string doc, string docModelFile, string stopWordsFile)
      {
         List<float> CDSSMFeature = new List<float>();
         DeepSemanticModel dm = GetDocModel(docModelFile, stopWordsFile);
         string normalizedDoc = TextNormalizer.Normalizer(TextNormalizer.StopWordsFilter(doc.Replace('-', ' ').Replace('\'', ' ').ToLowerInvariant().Trim()));
         if (!docModel.SemanticFeatureEmbedding(normalizedDoc, out CDSSMFeature))
         {
            Console.WriteLine("Error: document {0} embedding failed ...", doc);
            return null;
         }
         return string.Join(",", CDSSMFeature);
      }
   }
}
