using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using DeepSemanticLearning;

namespace DataHandlerApplication
{
   class CDSSMSemanticVectorGenerator
   {
      private static double GetCosSimilarity(List<float> a, List<float> b)
      {
         double num1 = 0, num2 = 0, num3 = 0;
         if (a.Count != b.Count)
         {
            return 0;
         }
         for (int i = 0; i < a.Count; i++)
         {
            num1 += a[i] * b[i];
            num2 += a[i] * a[i];
            num3 += b[i] * b[i];
         }
         return num1 / (System.Math.Sqrt(num2) * System.Math.Sqrt(num3));
      }

      /*
       * @param0 documentFile input
       * @param1 documentVectorFile output
       * @param2
       * @param3
       * @param4
       */
      public static void CalculateTweetsCDSSM(string documentFile,
         string documentVectorFile,
         string queryModelFile,
         string docModelFile,
         string stopWordsFile)
      {

         /*
         string documentFile = //args[0];
            @"D:\News_Team\Query-author-in-Twitter\recent-filtered-tweets.tsv";
         string documentVectorFile = //args[1];
            @"D:\News_Team\Query-author-in-Twitter\recent-filtered-CDSSM-tweets.tsv";
         string queryModelFile = //args[2];
            @"D:\News_Team\Query-author-in-Twitter\SemanticVectorGenerator\Model\QuerySideCDSSM.txt";
         string docModelFile = //args[3];
            @"D:\News_Team\Query-author-in-Twitter\SemanticVectorGenerator\Model\DocSideCDSSM.txt";
         string stopWordsFile = //args[4];
            @"D:\News_Team\Query-author-in-Twitter\SemanticVectorGenerator\StopWords.txt";
          */


         DeepSemanticModel docModel = new DeepSemanticModel();



         if (!docModel.ModelLoader(docModelFile))
         {
            Console.WriteLine("Error: CDSSM document-side model load failed ...");
            return;
         }

         if (!TextNormalizer.StopWordLoader(stopWordsFile))
         {
            Console.WriteLine("Error: stop words list load failed ...");
            return;
         }

         StreamWriter sw = new StreamWriter(documentVectorFile);
         //sw.AutoFlush = true;
         int count = 0, preSize = -1;
         using (StreamReader sr = new StreamReader(documentFile))
         {
            string items;
            while (null != (items = sr.ReadLine()))
            {
               string[] cols = items.Split('\t');
               if (cols.Length < 8) continue;
               string doc = cols[3];
               List<float> CDSSMFeature = new List<float>();
               string normalizedDoc = TextNormalizer.Normalizer(TextNormalizer.StopWordsFilter(doc.Replace('-', ' ').Replace('\'', ' ').ToLowerInvariant().Trim()));

               if (!docModel.SemanticFeatureEmbedding(normalizedDoc, out CDSSMFeature))
               {
                  Console.WriteLine("Error: document {0} embedding failed ...", doc);
                  continue;
               }
               else
               {
                  sw.WriteLine("{0}\t{1}", items, string.Join(",", CDSSMFeature));
                  if (count % 100 == 0)
                  {
                     Console.WriteLine("{0}", count);
                  }
                  if (preSize != -1 && preSize != CDSSMFeature.Count) Console.WriteLine(preSize + " " + CDSSMFeature.Count);
                  preSize = CDSSMFeature.Count;
               }
               count++;
            }
            sr.Close();
         }
         Console.WriteLine("CDSSM count {0}", preSize);
         sw.Close();
      }
   }
}
