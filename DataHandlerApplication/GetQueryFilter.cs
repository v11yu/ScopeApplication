using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace DataHandlerApplication
{
   public class GetQueryFilter
   {
      public static void getQuerySet(string[] args)
      {
         string intput = args[0];
         string output = args[1];
         int result1 = 30000;
         if (args.Length>2 && !int.TryParse(args[2], out result1))
            result1 = 30000;
         HashSet<string> stringSet = new HashSet<string>();
         using (StreamReader streamReader = new StreamReader(intput))
         {
            StreamWriter streamWriter = new StreamWriter(output);
            string str2;
            while ((str2 = streamReader.ReadLine()) != null)
            {
               string[] strArray = str2.Split('\t');
               int result2 = 0;
               if (int.TryParse(strArray[1], out result2) && result2 < result1)
                  streamWriter.WriteLine(strArray[0]);
            }
            streamWriter.Close();
            streamReader.Close();
         }
      }
   }
}
