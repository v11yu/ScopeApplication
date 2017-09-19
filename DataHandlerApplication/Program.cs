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
      static void Main(string[] args)
      {
         queryFilter();
      }
   }
}
