using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace DataHandlerApplication
{
    class CategoryGenerator
    {
        public static void GeneratorCategoryList(string input, string output)
        {
            using (StreamReader sr = new StreamReader(input))
            {
                string line = "";
                StreamWriter sw = new StreamWriter(output);
                while (null != (line = sr.ReadLine()))
                {
                    if (line.Length < 4) continue;
                    HashSet<string> set = new HashSet<string>();
                    string[] parts = line.Split(' ');
                    foreach (string part in parts)
                    {
                        string[] words = part.Split('_');
                        foreach (string word in words)
                        {
                            if (!word.Equals("rt") && !set.Contains(word))
                            {
                                sw.Write(word + " ");
                                set.Add(word);
                            }
                        }
                    }
                    sw.WriteLine();
                }
                sr.Close();
                sw.Close();
            }
        }
    }
}
