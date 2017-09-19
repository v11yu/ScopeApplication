using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

namespace DataHandlerApplication
{
   public class FileToValidTSV
   {
      public void convertToValidTSV(string inputPath,string outputPath,int colNums)
      {
         int originalLineNum = 0, generateLineNum = 0;
         StreamWriter sw = new StreamWriter(outputPath);
         var content = File.ReadAllLines(inputPath);
         foreach (var items in content)
         {
            originalLineNum++;
            string[] cols = items.Split('\t');
            if (cols.Length != colNums || !OtherVerifyCondition(items)) continue;
            sw.WriteLine(items);
            generateLineNum++;
         }
         Console.WriteLine("original line number:" + originalLineNum + " generate line number:" + generateLineNum);
         Console.ReadLine();
         sw.Close();
      }
      public virtual bool OtherVerifyCondition(string line) { return true; }
   }
}
