using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   class Program
   {
      public static void queryFilter()
      {
         string input = @"D:\News_Team\Query-author-in-Twitter\QueryTopSites.txt";
         string output = @"D:\News_Team\Query-author-in-Twitter\QueryTopSites.tsv";
         new FileToValidTSV().convertToValidTSV(input, output, 4);
      }
      public static void filterQueryByLength()
      {
         string input = @"D:\News_Team\Query-author-in-Twitter\QuerySets.tsv";
         string output = @"D:\News_Team\Query-author-in-Twitter\FilteredQuerySets.tsv";
         QueryFilteredByLength.filterQuery(input, output);
      }

      public static void testCDSSM()
      {
         string DocSideCDSSM = @"D:\News_Team\Query-author-in-Twitter\SemanticVectorGenerator\Model\DocSideCDSSM.txt";
         //string QuerySideCDSSM = @"D:\News_Team\Query-author-in-Twitter\SemanticVectorGenerator\Model\QuerySideCDSSM.txt";
         string StopWord = @"D:\News_Team\Query-author-in-Twitter\SemanticVectorGenerator\Model\StopWords.txt";
         string doc = "Sadiq Khan says carry on as normal You can still get drive thru	KTHopkins";
         string val = CalculateCDSSM.GetCDSSM(doc, DocSideCDSSM, StopWord);
         Console.WriteLine(doc + "\t" + val);
         Console.ReadLine();
      }
      static void Main(string[] args)
      {
         testCDSSM();
      }
   }
}
