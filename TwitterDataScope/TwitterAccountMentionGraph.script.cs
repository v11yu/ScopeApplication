using Microsoft.SCOPE.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Collections;

public class RelationReducer : Reducer
{
    public static string ToDebugString<TKey, TValue>(IDictionary<TKey, TValue> dictionary)
    {
        return "{" + string.Join(",", dictionary.Select(kv => kv.Key + "=" + kv.Value).ToArray()) + "}";
    }

    public static string ListToDebugString(List<string> ls)
    {
        return "{" + string.Join(",", ls) + "}";
    }

    public Dictionary<string,int> Sorted(Dictionary<string,int> dic)
    {
        dic = dic.OrderBy(x => -x.Value).ToDictionary(x => x.Key, x => x.Value);
        return dic;
    }

    public override Schema Produces(string[] requestedColumns, string[] args, Schema inputSchema)
    {
        return new Schema("uScreenName, rNames");
    }

    public override IEnumerable<Row> Reduce(RowSet input, Row outputRow, string[] args)
    {

        string uScreenName = null;
        Dictionary<string, int> sDic = new Dictionary<string, int>();
        Dictionary<string, int> mDic = new Dictionary<string, int>();
        foreach (Row row in input.Rows)
        {
            if (uScreenName == null)
            {
                uScreenName = row["uScreenName"].String;
            }

            string toname = row["toname"].String;

            Dictionary<string, int> tmpDic = mDic;
            if (row["relation"].String.Equals("s"))
            {
                tmpDic = sDic;
            }

            if (tmpDic.ContainsKey(toname))
            {
                tmpDic[toname] = tmpDic[toname] + 1;
            }
            else
            {
                tmpDic[toname] = 1;
            }
        }
        sDic = Sorted(sDic);
        mDic = Sorted(mDic);
        List<string> res = new List<string>();
        foreach (KeyValuePair<string, int> entity in mDic)
        {
            if (res.Count == 20) break;
            res.Add(entity.Key);
        }
        outputRow["uScreenName"].Set(uScreenName);
        outputRow["rNames"].Set(ListToDebugString(res));

        yield return outputRow;
    }
}


public class RelationProcessor : Processor
{
    public static HashSet<string> verifyUser = null;

    public static HashSet<string> GetVerifyUser(string path)
    {
        if(verifyUser == null)
        {
            verifyUser = new HashSet<string>();
            using (StreamReader sr = new StreamReader(path))
            {
                string line = null;
                while ((line = sr.ReadLine()) != null)
                {
                    if (line.Length < 1) continue;
                    verifyUser.Add(NormStr(line));
                }
                sr.Close();
            }
        }
        return verifyUser;
    }

    public static string NormStr(string s)
    {
        return s.Trim().ToLower();
    }

    public bool IsVerifyUser(string name,string path)
    {
        return GetVerifyUser(path).Contains(name);
    }

    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        return new Schema("uScreenName, toname, relation");
    }
    public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
    {
        foreach (Row input_row in input_rowset.Rows)
        {
            string doc = NormStr(input_row["tText"].String);
            List<string> ls = new List<string>();

            foreach (string word in doc.Split(' '))
            {
                
                if (word.Length > 2 && word[0] == '@' && IsVerifyUser(NormStr(word.Substring(1)), args[0]))
                {
                    string checkName = NormStr(word.Substring(1));
                    ls.Add(checkName);
                    output_row["uScreenName"].Set(NormStr(input_row["uScreenName"].String));
                    output_row["toname"].Set(checkName);
                    output_row["relation"].Set("s");
                    yield return output_row;
                }
            }

            foreach (string name in ls)
            {
                foreach (string toname in ls)
                {
                    if(!name.Equals(toname))
                    {
                        output_row["uScreenName"].Set(name);
                        output_row["toname"].Set(toname);
                        output_row["relation"].Set("m");
                        yield return output_row;
                    }
                }
            }
        }
    }
}