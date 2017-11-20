using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using DeepSemanticLearning;

public class CategoryReducer : Reducer
{
    private int CNT = 0;
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
    public List<float> GetCDSSM(string doc, string docModelFile, string stopWordsFile)
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
        return CDSSMFeature;
    }

    public static double GetCosSimilarity(List<float> a, List<float> b)
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

    private static Dictionary<string, string> categoryToColName = null;
    private static Dictionary<string, List<float>> categoryCDSSM = null;

    private Dictionary<string, string> GetCategoryToColName()
    {
        if (categoryToColName == null)
        {
            categoryToColName = new Dictionary<string, string>();
            categoryToColName["Sports"] = "rt_Sports";
            categoryToColName["Science And Technology"] = "rt_ScienceAndTechnology";
            categoryToColName["Business"] = "rt_Business";
            categoryToColName["Entertainment"] = "rt_Entertainment";
            categoryToColName["Education"] = "rt_Education";
        }
        return categoryToColName;
    }

    private Dictionary<string, List<float>> GetCategoryCDSSM(string docModelFile, string stopWordsFile, string categoryFile)
    {
        if(categoryCDSSM == null)
        {
            categoryCDSSM = new Dictionary<string, List<float>>();
            using (StreamReader sr = new StreamReader(categoryFile))
            {
                string line = null;
                while((line = sr.ReadLine()) != null)
                {
                    if (line.Length < 1) continue;
                    categoryCDSSM[line] = GetCDSSM(line, docModelFile, stopWordsFile);
                }
                sr.Close();
            }
                
        }
        return categoryCDSSM;
    }

    public override Schema Produces(string[] requestedColumns, string[] args, Schema inputSchema)
    {
        return new Schema("uID, uScreenName, uFavoritesCount, uFollowersCount, uFriendsCount, rt_Sports, rt_ScienceAndTechnology, rt_Business, rt_Entertainment, rt_Education");
    }

    /*
     * args0 docModelFile
     * args1 stopWordsFile
     * args2 categoryFile
     * args3 random N tweets for per account
     * args4 similiar threshold
     */
    public override IEnumerable<Row> Reduce(RowSet input, Row outputRow, string[] args)
    {

        int cnt = 0;
        int randomN = int.Parse(args[3]);
        double threshold = double.Parse(args[4]);
        List<List<float>> textList = new List<List<float>>();

        foreach (Row row in input.Rows)
        {
            if (cnt == 0)
            {
                outputRow["uID"].Set(row["uID"].String);
                outputRow["uScreenName"].Set(row["uScreenName"].String);
                outputRow["uFavoritesCount"].Set(row["uFavoritesCount"].String);
                outputRow["uFollowersCount"].Set(row["uFollowersCount"].String);
                outputRow["uFriendsCount"].Set(row["uFriendsCount"].String);
            }
            string tText = row["tText"].String;
            textList.Add(GetCDSSM(tText, args[0], args[1]));
            if(CNT++%100 == 0) Console.WriteLine(CNT);
            if (cnt++ >= randomN) break;
        }

        foreach (KeyValuePair<string, List<float>> entity in GetCategoryCDSSM(args[0], args[1], args[2]))
        {
            double sum = 0;
            int cc = 0;
            foreach (List<float> tTextVector in textList)
            {
                if (tTextVector == null || entity.Value == null)
                {
                    continue;
                }
                sum += GetCosSimilarity(tTextVector, entity.Value);
                cc++;
            }
            sum = sum / cc;
            if(sum>threshold)
            {
                outputRow[GetCategoryToColName()[entity.Key]].Set("true");
            }
            else
            {
                outputRow[GetCategoryToColName()[entity.Key]].Set("false");
            }
        }

        yield return outputRow;
    }
}