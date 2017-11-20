using System;
using System.Collections.Generic;
using System.Security.Cryptography;
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
            string doc = "China";
            string doc1 = "This is why China has so few female leaders";
            List<float> val = CalculateCDSSM.GetCDSSM(doc, DocSideCDSSM, StopWord);
            List<float> val1 = CalculateCDSSM.GetCDSSM(doc1, DocSideCDSSM, StopWord);
            Console.WriteLine(doc + "\t" + string.Join(",", val));
            Console.WriteLine(doc1 + "\t" + string.Join(",", val1));
            Console.WriteLine(CalculateCDSSM.GetCosSimilarity(val, val1));
            Console.ReadLine();
        }

        public static string GetMD5String(string url)
        {
            MD5 md5 = new MD5CryptoServiceProvider();
            byte[] result = md5.ComputeHash(System.Text.Encoding.Default.GetBytes(url));
            return BitConverter.ToString(result).Replace("-", "");
        }

        static void Main(string[] args)
        {
            string path = @"D:\News_Team\Query-author-in-Twitter\";
            string[] myargs = {path+ "UDiscoveryPatternsList_Int.csv",
            path + "UDiscoveryPatternsList_Int-keyword.tsv",
         "100"};
            //GetQueryFilter.getQuerySet(args);
            //CalculateWordMapTweets.calculateWordMapTweets(args);
            //UDiscoveryPatternsListHandler.GetSourceName(myargs[0], myargs[1]);
            //StatisticsJudgement.statisticsJudgement(@"D:\News_Team\Judgement\UHRS_Task_Round 2.1.tsv", 3, 15, 17);
            //CategoryGenerator.GeneratorCategoryList(@"D:\News_Team\Bing-click-data\NewsArticleFeaturesV2-category-1.tsv", @"D:\News_Team\Bing-click-data\category-list.tsv");
            //CategoryGenerator.GeneratorCategoryList(args[0],args[1]);
            
            Console.WriteLine(GetMD5String(string.Concat("http://www.sacbee.com/news/business/article184317858.html","tweccexp")));
            Console.ReadLine();
        }
    }
}
