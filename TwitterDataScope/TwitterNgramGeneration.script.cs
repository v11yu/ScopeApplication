using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Globalization;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class Functions
{
   public static string ComposeTimeStamp(string date, string hour, string minute)
   {
      return date + " " + hour + ":" + minute + ":" + "00";
   }

   public static int MinuteGap(string tCreatedAt, DateTime datetimeUTC)
   {
      try
      {
         DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
         DateTime createdAtDate = unixEpoch.AddMilliseconds(Convert.ToDouble(TextFunctions.Strip(tCreatedAt)));
         return (int)(datetimeUTC - createdAtDate).TotalMinutes;
      }
      catch (Exception e)
      {
         return int.MinValue;
      }
   }
}

public class PreGeneratedEntityV2 : Processor
{
   public override Schema Produces(string[] requestedColumns, string[] args, Schema inputSchema)
   {
      return new Schema("dateCreation:string, hourCreation:string, minuteCreation:string, entity:string, entityPrintable:string, entitytype:string, entitysubtypetype:string, url:string, targeturl:string, domain:string, thumbnail:string, satoriid:string, isnewsurl:int, tID:string, uID:string, uScreenName:string, uName:string, userAgeInDays:int, sentiment:float, tIsRetweet:string, tweettext:string, entitySource:string, tPublishTime:string, tRetweetCount:string, tText:string, tRawText:string, tTextFragments:string, tSpamScore:string, uAuthScore:string, uFavoritesCount:string, uFollowersCount:string, uFriendsCount:string, uProfile:string, uVerified:string");
   }

   public class Entity
   {
      public string entity;
      public string entityPrintable;
      public string entitytype;
      public string entitysubtypetype;
      public string entitySource;
   }

   public bool IsEntityExtractedFromDomain(string targeturl, string entityOriginal)
   {
      int domainPos = -1;
      int domainLength = -1;
      string tmpUrl = "";

      if (targeturl.Length > 0)
      {
         domainLength = targeturl.IndexOf("/");
         if (domainLength == -1)
            domainLength = targeturl.Length;
         tmpUrl = " " + targeturl.Replace("/", " ").Replace(".", " ").Replace("-", " ").Replace("_", " ") + " ";
         domainPos = tmpUrl.IndexOf(entityOriginal.ToLower());
         if (domainPos == -1 && entityOriginal.Contains(" "))
         {
            domainPos = tmpUrl.IndexOf(entityOriginal.Replace(" ", "").ToLower());
         }
      }

      if (domainPos >= 0 && domainPos <= domainLength)
         return true;
      return false;
   }

   public override IEnumerable<Row> Process(RowSet input, Row outputRow, string[] args)
   {
      foreach (Row row in input.Rows)
      {
         String ddate;
         DateTime dateObject;
         String currentdate = "";
         String currenthour = "";
         String currentminute = "";
         string entityPrintable = "";
         CultureInfo cultureInfo = System.Threading.Thread.CurrentThread.CurrentCulture;
         TextInfo textInfo = cultureInfo.TextInfo;
         string entityOriginal = "";
         int userAgeInDays = 0;

         //Parse Tweet's createdTime
         try
         {
            ddate = TextFunctions.DateFormatUtc(TextFunctions.Strip(row["tCreatedAt"].ToString()), "0", 0.0);
            dateObject = DateTime.Parse(ddate);
            currentdate = dateObject.Date.ToString("yyyy-MM-dd");
            currenthour = dateObject.ToString("HH");
            currentminute = dateObject.ToString("mm");
         }
         catch (Exception e)
         {
            continue;
         }

         //Parse User's createdTime
         try
         {
            userAgeInDays = Convert.ToInt32((dateObject - DateTime.Parse(TextFunctions.DateFormatUtc(TextFunctions.Strip(row["uCreatedAt"].ToString()), "0", 0.0))).TotalDays);

            if (userAgeInDays < 30)
               continue;
         }
         catch (Exception e)
         {
            continue;
         }

         string targeturl = TextFunctions.StripChar(row["tUrl"].String).Trim();
         string printableUrl = targeturl;
         if (targeturl != "")
         {
            if (targeturl.Contains("http://www."))
               targeturl = targeturl.Replace("http://www.", "");
            else if (targeturl.Contains("https://www."))
               targeturl = targeturl.Replace("https://www.", "");
            else if (targeturl.Contains("http://"))
               targeturl = targeturl.Replace("http://", "");
            else if (targeturl.Contains("https://"))
               targeturl = targeturl.Replace("https://", "");
            targeturl = TextFunctions.removeURLAfterHash(targeturl);
         }

         if (targeturl.Contains("itunes.apple.com"))
            continue;
         if (targeturl.Contains("store.apple.com"))
            continue;

         int isNews = 0;
         if (row["tLinkAnnotations"].String.Contains("newsclassifier.is_news_domain\",\"Value\":\"1\"") && !targeturl.Contains("mtv.com") && !targeturl.Contains("popsugar.com"))
            isNews = 1;

         HashSet<string> alreadyPrinted = new HashSet<string>();
         string title = TextFunctions.cleanString(TextFunctions.StripChar(row["tTitle"].String)).Trim();
         String[] entities = new String[] { row["tNamedEntityFirst"].String.Trim(), row["tNamedEntitySecond"].String.Trim(), row["tNamedEntityThird"].String.Trim() };
         String[] entityTypes = new String[] { row["tNamedEntityCategoryFirst"].String.Trim(), row["tNamedEntityCategorySecond"].String.Trim(), row["tNamedEntityCategoryThird"].String.Trim() };
         int ctr = -1;

         //Process explicit entities
         List<Entity> entityList = new List<Entity>();
         foreach (string entity in entities)
         {
            ctr++;
            if (entity == "")
               continue;
            if (entity.Length < 3)
               continue;
            if (entityTypes[ctr] == "DATE" || entityTypes[ctr] == "TIME-POINT" || entityTypes[ctr] == "ZIP")
               continue;
            entityOriginal = entity.Replace(" _ ", " ").Trim();
            entityPrintable = entityOriginal;
            if ((entityOriginal.ToLower() == entityOriginal))
               entityPrintable = textInfo.ToTitleCase(entityOriginal.ToLower());
            if (alreadyPrinted.Contains(entityOriginal.ToLower()))
               continue;
            alreadyPrinted.Add(entityOriginal.ToLower());

            if (IsEntityExtractedFromDomain(targeturl, entityOriginal))
               continue;

            Entity explictEntity = new Entity() { entity = entityOriginal.ToLower(), entityPrintable = entityPrintable, entitytype = "entity", entitysubtypetype = entityTypes[ctr], entitySource = "Original" };
            entityList.Add(explictEntity);
         }

         //Process ngrams
         string textNgrams = TextFunctions.generateNgrams(row["tText"].String, 1, 4);
         string[] tokens = textNgrams.Trim().Split(';');
         string typeOfNgramType = "";
         string previousEntity = "";

         foreach (string token in tokens)
         {
            if (token.Length < 3)
               continue;

            if (token[0] == '#')
               typeOfNgramType = "hashtag";
            else if (token[0] == '@')
               typeOfNgramType = "username";
            else if (("@" + token) == previousEntity)
               typeOfNgramType = "username";
            else
               typeOfNgramType = "ngram";

            if (alreadyPrinted.Contains(token.ToLower()))
               continue;

            if (IsEntityExtractedFromDomain(targeturl, token))
               continue;

            previousEntity = token;
            alreadyPrinted.Add(token.ToLower());

            Entity tweetTextNGram = new Entity() { entity = token.ToLower(), entityPrintable = token, entitytype = typeOfNgramType, entitysubtypetype = string.Empty, entitySource = "Text" };
            entityList.Add(tweetTextNGram);
         }

         /////////////////////////////HEADLINE/////////////////////// (should be news document's title)
         string textNgrams1 = "";
         if (isNews == 1)
            textNgrams1 = TextFunctions.generateNgrams(title, 1, 4);
         string[] tokens1 = textNgrams1.Trim().Split(';');
         foreach (string token in tokens1)
         {
            if (token.Length < 3)
               continue;

            if (token[0] == '#')
               typeOfNgramType = "hashtag";
            else if (token[0] == '@')
               typeOfNgramType = "username";
            else if (("@" + token) == previousEntity)
               typeOfNgramType = "username";
            else
               typeOfNgramType = "ngram";

            if (alreadyPrinted.Contains(token.ToLower()))
               continue;

            if (IsEntityExtractedFromDomain(targeturl, token))
               continue;

            previousEntity = token;
            alreadyPrinted.Add(token.ToLower());

            Entity titleTextNGram = new Entity() { entity = token.ToLower(), entityPrintable = token, entitytype = typeOfNgramType, entitysubtypetype = string.Empty, entitySource = "Title" };
            entityList.Add(titleTextNGram);
         }

         outputRow["dateCreation"].Set(currentdate);
         outputRow["hourCreation"].Set(currenthour);
         outputRow["minuteCreation"].Set(currentminute);
         outputRow["targeturl"].Set(targeturl);
         outputRow["domain"].Set(row["tDomain"].String.Trim());
         outputRow["thumbnail"].Set(TextFunctions.ExtractThumbnail(row["tLinkAnnotations"].String));
         outputRow["url"].Set(printableUrl);
         outputRow["isnewsurl"].Set(isNews);
         outputRow["tID"].Set(row["tID"].String);
         outputRow["uID"].Set(row["uID"].String);
         outputRow["uScreenName"].Set(row["uScreenName"].String);
         outputRow["uName"].Set(row["uName"].String);
         outputRow["userAgeInDays"].Set(userAgeInDays);
         outputRow["sentiment"].Set(0.0);
         if (isNews == 1)
         {
            outputRow["tweettext"].Set(TextFunctions.cleanString(title));
         }
         else if (row["tDomain"].String == "youtube.com")
            outputRow["tweettext"].Set(TextFunctions.cleanString(title));
         else
            outputRow["tweettext"].Set("");
         outputRow["tIsRetweet"].Set(row["tIsRetweet"].String);

         /////////////////////////new columns/////////////////////////
         string text = TextFunctions.normalizedText(TextFunctions.cleanText(row["tText"].String));
         outputRow["tText"].Set(text);
         row["tText"].CopyTo(outputRow["tRawText"]);
         outputRow["tTextFragments"].Set(row["tTextFragments"].String);
         outputRow["tPublishTime"].Set(TextFunctions.DateFormatFileUtc(TextFunctions.Strip(row["tCreatedAt"].ToString()), "0", 0.0));
         outputRow["tRetweetCount"].Set(row["tRetweetCount"].String);
         outputRow["tSpamScore"].Set(row["tSpamScore"].String);
         outputRow["uAuthScore"].Set(row["uAuthScore"].String);
         outputRow["uFavoritesCount"].Set(row["uFavoritesCount"].String);
         outputRow["uFollowersCount"].Set(row["uFollowersCount"].String);
         outputRow["uFriendsCount"].Set(row["uFriendsCount"].String);
         outputRow["uProfile"].Set(TextFunctions.generateProfile(row["uID"].String, row["uName"].String, row["uScreenName"].String, row["uProfilePage"].String, row["uProfileImageUrl"].String, row["uVerified"].Boolean));
         outputRow["uVerified"].Set(row["uVerified"].String);
         /////////////////////////new columns end/////////////////////////

         foreach (Entity entity in entityList)
         {
            outputRow["entity"].Set(entity.entity);
            outputRow["entityPrintable"].Set(entity.entityPrintable);
            outputRow["entitytype"].Set(entity.entitytype);
            outputRow["entitysubtypetype"].Set(entity.entitysubtypetype);
            outputRow["entitySource"].Set(entity.entitySource);//new column
            yield return outputRow;
         }
      }
   }
}

public class TextFunctions
{
   public static string[] arrayUnigram = { "absolutely", "http", "followback", "relevant", "feelin", "least", "named", "lol", "u", "forget", "vote", "meant", "didnt", "doesnt", "isnt", "cunt", "havent", "yall", "goes", "youre", "gettin", "thats", "almost", "feels", "especially", "seriously", "serious", "feelings", "retweet", "tweet", "follows", "retweets", "following", "followers", "tweets", "quote", "saying", "deserve", "anymore", "together", "understand", "bring", "swear", "hello", "later", "welcome", "called", "bitches", "y'all", "happen", "enjoy", "different", "see", "either", "bout", "came", "hahaha", "hahahaha", "hahah", "hahahahahaha", "hahahah", "soooo", "soo", "sooo", "sooooo", "pleaseee", "pleasee", "please", "aint", "can", "get", "whoever", "wont", "can", "lasted", "if", "you", "fave", "let", "came", "truly", "actual", "damn", "cause", "gotta", "posted", "remember", "liked", "guess", "might", "went", "tired", "probably", "part", "niggas", "literally", "latest", "cant", "half", "told", "early", "found", "thinking", "forever", "used", "hear", "waiting", "wants", "open", "excited", "coming", "okay", "perfect", "haha", "read", "cool", "favorite", "says", "wrong", "soon", "anything", "gets", "left", "since", "making", "change", "crazy", "without", "also", "believe", "funny", "sometimes", "else", "though", "talking", "meet", "seen", "maybe", "feeling", "needs", "full", "finally", "playing", "leave", "gone", "lost", "awesome", "enough", "whole", "turn", "miss", "play", "talk", "amazing", "mean", "many", "bitch", "call", "start", "away", "now", "gain", "nothing", "looking", "years", "nigga", "ready", "another", "TRUE", "pretty", "sure", "yeah", "care", "high", "lmao", "ain't", "times", "around", "stay", "trying", "must", "post", "looks", "thought", "watching", "already", "anyone", "sorry", "just", "like", "love", "https", "watch", "good", "will", "want", "know", "people", "time", "life", "back", "need", "make", "today", "happy", "much", "never", "best", "really", "think", "right", "still", "going", "night", "ever", "someone", "shit", "always", "video", "even", "look", "world", "everyone", "take", "birthday", "girl", "fuck", "come", "last", "great", "first", "game", "tonight", "feel", "real", "thanks", "thank", "better", "home", "wanna", "give", "year", "well", "hate", "news", "every", "work", "stop", "something", "free", "hope", "things", "next", "getting", "live", "thing", "show", "everything", "hard", "tell", "keep", "actually", "said", "makes", "done", "made", "wait", "fucking", "little", "find", "long", "dont", "cute", "help", "beautiful", "nice", "photo", "check", "wish", "tomorrow", "a", "about", "gonna", "please", "above", "after", "again", "follow", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours??", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves", "introduce", "introducing" };
   public static HashSet<string> hashUnigram = new HashSet<string>(arrayUnigram);

   public static string[] arrayBigram = { "rt fav", "rts pleaseee", "rt followback", "rt amp", "rt quote", "can get", "god bless", "can go", "go get", "can win", "retweets amp", "let go", "of the", "at the", "in the", "for the", "for the", "on the", "follow me", "us on", "gonna be", "way to", "less than", "in less", "from your", "you were" };
   public static HashSet<string> hashBigram = new HashSet<string>(arrayBigram);

   public static string cleanString(string text)
   {
      if (text.Length <= 1)
         return "";
      if (text.Contains("#"))
      {
         StringBuilder newText = new StringBuilder(text);
         newText.Replace("        ", " ");
         newText.Replace("&amp;", "&");
         newText.Replace("&#8220;", "\"");
         newText.Replace("&#8221;", "\"");
         newText.Replace("&#x27;", "'");
         newText.Replace("&#39;", "'");
         newText.Replace("&quot;", "\"");
         newText.Replace("&#8217;", "'");
         newText.Replace("&#10;", " ");
         newText.Replace("&#039;", "'");
         newText.Replace("&#8211;", "-");
         newText.Replace("&gt;", ">");
         newText.Replace("&lt;", "<");
         newText.Replace("&#8216;", "'");
         newText.Replace("&#8217;", "'");
         return newText.ToString();
      }
      return text;
   }

   public class jTextFragment
   {
      public string Text;
      public string Url;
      public string Domain;
   }

   public class jProfile
   {
      public string Id;
      public string Name;
      public string Alias;
      public string Url;
      public jProfile_Thumbnail Thumbnail;
      public bool IsVerified;
   }

   public class jProfile_Thumbnail
   {
      public string Url;
   }

   public static string generateProfile(string id, string name, string alias, string url, string thumbnail, bool isVerified)
   {
      jProfile obj = new jProfile()
      {
         Id = id,
         Name = name,
         Alias = alias,
         Url = TextFunctions.StripChar(url),
         Thumbnail = new jProfile_Thumbnail() { Url = TextFunctions.StripChar(thumbnail) },
         IsVerified = isVerified
      };
      return JsonConvert.SerializeObject(obj);
   }

   public static string cleanText(string text)
   {
      if (text.Length <= 1)
         return "";
      StringBuilder newText = new StringBuilder(text);
      newText.Replace(@"\n", "\n");
      newText.Replace(@"\t", "\t");
      newText.Replace("        ", " ");
      newText.Replace("&amp;", "&");
      newText.Replace("&#8220;", "\"");
      newText.Replace("&#8221;", "\"");
      newText.Replace("&#x27;", "'");
      newText.Replace("&#39;", "'");
      newText.Replace("&quot;", "\"");
      newText.Replace("&#8217;", "'");
      newText.Replace("&#10;", " ");
      newText.Replace("&#039;", "'");
      newText.Replace("&#8211;", "-");
      newText.Replace("&gt;", ">");
      newText.Replace("&lt;", "<");
      newText.Replace("&#8216;", "'");
      newText.Replace("&#8217;", "'");
      return newText.ToString();
   }

   public static string generateTextFragments(string text, string linksJsonString)
   {
      //clean text
      text = cleanText(text);

      string result = "";
      //delete "RT @alias: "
      string pattern = @"^RT @(\w+): ";
      if (Regex.Match(text, pattern).Success)
         text = Regex.Replace(text, pattern, "");

      //replace all link with display url
      //return all link
      List<jTextFragment> fragments = new List<jTextFragment>();
      string pattern2 = @"https*:\\/\\/(\S+)";
      Regex reg = new Regex(pattern2);
      Match m = reg.Match(text);
      int count = reg.Matches(text).Count;

      if (string.IsNullOrEmpty(linksJsonString))
      {
         if (count != 0)
            return String.Empty;

         fragments.Add(new jTextFragment()
         {
            Text = TextFunctions.StripChar(text),
            Url = "",
            Domain = ""
         });
         return JsonConvert.SerializeObject(fragments);
      }

      var linksObj = JArray.Parse(linksJsonString);
      if (count != linksObj.Count)
         return String.Empty;

      int i = 0;
      int last = 0;
      while (m.Success)
      {
         result = m.Groups[0].ToString();
         int start = m.Groups[0].Captures[0].Index;
         int end = start + result.Length;
         if (last < start)
         {
            fragments.Add(new jTextFragment()
            {
               Text = TextFunctions.StripChar(text.Substring(last, start - last)),
               Url = "",
               Domain = ""
            });
         }
         fragments.Add(new jTextFragment()
         {
            Text = TextFunctions.StripChar(linksObj[i]["DisplayUrl"].ToObject<string>()),
            Url = TextFunctions.StripChar(result),
            Domain = linksObj[i]["Domain"].ToObject<string>()
         });

         m = m.NextMatch();
         i++;
         last = end;
      }
      if (last < text.Length)
      {
         fragments.Add(new jTextFragment()
         {
            Text = TextFunctions.StripChar(text.Substring(last, text.Length - last)),
            Url = "",
            Domain = ""
         });
      }

      return JsonConvert.SerializeObject(fragments);
   }

   public static string breakString(string text, char delimiter, int position)
   {
      if (position < 0)
         position = 1;


      return text.Trim().Split(delimiter)[position - 1];
   }

   public static string getPrintableDate(string text)
   {
      String ddate;
      DateTime dateObject;
      String currentdate = "";
      String currenthour = "";
      String currentminute = "";
      try
      {
         ddate = TextFunctions.DateFormatUtc(TextFunctions.Strip(text), "0", 0.0);
         dateObject = DateTime.Parse(ddate);
         currentdate = dateObject.Date.ToString("yyyy-MM-dd");
         currenthour = dateObject.ToString("HH");
         currentminute = dateObject.ToString("mm");
      }
      catch (Exception e)
      {
         //                continue;
      }
      return currentdate + " " + currenthour + ":" + currentminute;
   }

   public static string replaceHTMLEncodings(string text)
   {
      if (text.Length <= 1)
         return "";
      if (!text.Contains("&"))
         return text;
      return text.Replace("&#039;", "'")
              .Replace("&#8217;", "'")
              .Replace("&#39;", "'")
              .Replace("&amp;", "&")
              .Replace("&quot;", "\"")
              .Replace("&nbsp;", " ")
              .Replace("&#8216;", "'")
              .Replace("&#x20;", " ")
              .Replace("&#x27;", "'")
              .Replace("&apos;", "'")
              .Replace("&#8211;", "-")
              .Replace("&#034;", "\"")
              .Replace("&laquo;", "<")
              .Replace("&rsquo;", "'")
              .Replace("&raquo;", ">")
              .Replace("&#8220;", "\"")
              .Replace("&#8221;", "\"")
              .Replace("&mdash;", "-")
              .Replace("&#8230;", "...")
              .Replace("&#8212;", "-")
              .Replace("&#34;", "\"")
              .Replace("&#038;", "&")
              .Replace("&lsquo;", "'")
              .Replace("&#x3A;", ":")
              .Replace("&ldquo;", "\"")
              .Replace("&rdquo;", "\"")
              .Replace("&#45;", "-")
              .Replace("&#8226;", ".")
              .Replace("&lsaquo;", "<")
              .Replace("&rsaquo;", ">");
   }


   public static string generateNgrams(string text, int minimum, int maximum)
   {
      string line = normalizedText(cleanText(text));
      if (line.Length < 3)
         return "";


      string[] tokens = line.ToLower().Split(' ');
      string[] tokensOriginal = line.Split(' ');
      string tokenizedStr = "";

      if (minimum <= 1 && maximum >= 1)
      {
         for (int i = 0; i < tokens.Length; ++i)
         {
            if (tokens[i].Length > 3 && !hashUnigram.Contains(tokens[i]))
               tokenizedStr += tokensOriginal[i] + ";";
            if (tokens[i].Length > 4 && (tokens[i])[0] == '@')
               tokenizedStr += tokensOriginal[i].Substring(1) + ";";
         }
      }


      if (minimum <= 2 && maximum >= 2)
      {
         for (int i = 0; i < tokens.Length - 1; ++i)
         {
            if (!hashUnigram.Contains(tokens[i]) && !hashUnigram.Contains(tokens[i + 1]) && ((tokens[i].Length > 1 && tokens[i][0] != '#' && tokens[i][0] != '@') || tokens[i].Length == 1) && ((tokens[i + 1].Length > 1 && tokens[i + 1][0] != '#' && tokens[i + 1][0] != '@') || tokens[i + 1].Length == 1))
            {
               if (!hashBigram.Contains(tokens[i] + ' ' + tokens[i + 1]) && (tokens[i] + ' ' + tokens[i + 1]).Length > 5)
                  tokenizedStr += tokensOriginal[i] + " " + tokensOriginal[i + 1] + ';';
            }
         }
      }
      if (minimum <= 3 && maximum >= 3)
      {
         for (int i = 0; i < tokens.Length - 2; ++i)
         {
            if (!hashUnigram.Contains(tokens[i]) && !hashUnigram.Contains(tokens[i + 1]) && !hashUnigram.Contains(tokens[i + 2]) && ((tokens[i].Length > 1 && tokens[i][0] != '#' && tokens[i][0] != '@') || tokens[i].Length == 1) && ((tokens[i + 1].Length > 1 && tokens[i + 1][0] != '#' && tokens[i + 1][0] != '@') || tokens[i + 1].Length == 1) && ((tokens[i + 2].Length > 1 && tokens[i + 2][0] != '#' && tokens[i + 2][0] != '@') || tokens[i + 2].Length == 1))
            {
               if (!hashBigram.Contains(tokens[i] + ' ' + tokens[i + 1] + ' ' + tokens[i + 2]) && (tokens[i] + ' ' + tokens[i + 1] + ' ' + tokens[i + 2]).Length > 10)
                  tokenizedStr += tokensOriginal[i] + ' ' + tokensOriginal[i + 1] + ' ' + tokensOriginal[i + 2] + ';';
            }
         }
      }
      if (minimum <= 4 && maximum >= 4)
      {
         for (int i = 0; i < tokens.Length - 3; ++i)
         {
            if (!hashUnigram.Contains(tokens[i]) && !hashUnigram.Contains(tokens[i + 1]) && !hashUnigram.Contains(tokens[i + 2]) && !hashUnigram.Contains(tokens[i + 3]) && ((tokens[i].Length > 1 && tokens[i][0] != '#' && tokens[i][0] != '@') || tokens[i].Length == 1) && ((tokens[i + 1].Length > 1 && tokens[i + 1][0] != '#' && tokens[i + 1][0] != '@') || tokens[i + 1].Length == 1) && ((tokens[i + 2].Length > 1 && tokens[i + 2][0] != '#' && tokens[i + 2][0] != '@') || tokens[i + 2].Length == 1) && ((tokens[i + 3].Length > 1 && tokens[i + 3][0] != '#' && tokens[i + 3][0] != '@') || tokens[i + 3].Length == 1))
            {
               if ((tokens[i] + ' ' + tokens[i + 1] + ' ' + tokens[i + 2] + ' ' + tokens[i + 3]).Length > 15)
                  tokenizedStr += tokens[i] + ' ' + tokens[i + 1] + ' ' + tokens[i + 2] + ' ' + tokens[i + 3] + ';';
            }
         }
      }
      if (minimum <= 5 && maximum >= 5)
      {
         for (int i = 0; i < tokens.Length - 4; ++i)
         {
            if (!hashUnigram.Contains(tokens[i]) && !hashUnigram.Contains(tokens[i + 1]) && !hashUnigram.Contains(tokens[i + 2]) && !hashUnigram.Contains(tokens[i + 3]) && !hashUnigram.Contains(tokens[i + 4]) && ((tokens[i].Length > 1 && tokens[i][0] != '#' && tokens[i][0] != '@') || tokens[i].Length == 1) && ((tokens[i + 1].Length > 1 && tokens[i + 1][0] != '#' && tokens[i + 1][0] != '@') || tokens[i + 1].Length == 1) && ((tokens[i + 2].Length > 1 && tokens[i + 2][0] != '#' && tokens[i + 2][0] != '@') || tokens[i + 2].Length == 1) && ((tokens[i + 3].Length > 1 && tokens[i + 3][0] != '#' && tokens[i + 3][0] != '@') || tokens[i + 3].Length == 1) && ((tokens[i + 4].Length > 1 && tokens[i + 4][0] != '#' && tokens[i + 4][0] != '@') || tokens[i + 4].Length == 1))
            {
               if ((tokens[i] + ' ' + tokens[i + 1] + ' ' + tokens[i + 2] + ' ' + tokens[i + 3] + ' ' + tokens[i + 4]).Length > 15)
                  tokenizedStr += tokens[i] + ' ' + tokens[i + 1] + ' ' + tokens[i + 2] + ' ' + tokens[i + 3] + ' ' + tokens[i + 4] + ';';
            }
         }
      }

      //if (tokenizedStr.Length > 2)
      //     return tokenizedStr.Substring(0, tokenizedStr.Length - 1);
      return tokenizedStr;
   }

   public static string normalizedText(string text)
   {
      string line = "";
      text = " " + text + "      ";
      int pos1 = 0;
      int pos2 = 0;
      for (int i = 0; i < text.Length - 6; ++i)
      {
         if (text[i] == '\'' && Char.IsLetterOrDigit(text[i + 1]) && Char.IsLetterOrDigit(text[i - 1]))
         {
            line += "'";
         }
         else if (text[i] == 'h' && text[i + 1] == 't' && text[i + 2] == 't' && text[i + 3] == 'p')
         {
            if ((text[i + 4] == ':') || (text[i + 4] == 's' && text[i + 5] == ':'))
            {
               pos1 = text.IndexOf(' ', i + 1);
               pos2 = text.IndexOf(')', i + 1);
               if (pos2 > 0 && pos2 < pos1)
                  pos1 = pos2;
               i = pos1 - 1;
            }
         }
         else if (Char.IsLetterOrDigit(text[i]) || text[i] == '_' || text[i] == '@' || text[i] == '#')
            line += text[i];
         else
            line += ' ';
      }
      line = line.Replace(" _ ", " ");
      return line.Trim().Replace("  ", " ").Replace("  ", " ");
   }


   public static string removeURLAfterHash(string url)
   {
      int pos = url.IndexOf("#");
      if (pos < 5)
         return url;
      return url.Substring(0, pos);
   }

   public static string ExtractThumbnail(string blob)
   {
      int pos1 = 0, pos2 = 0;
      string url = "";
      try
      {
         pos1 = blob.IndexOf("og:image", pos2 + 1);
         if (pos1 < 3)
            return "";
         pos2 = blob.IndexOf("\"", pos1 + 24);
         if (pos2 < 3)
            return "";

         url = blob.Substring(pos1 + 23, pos2 - pos1 - 24);
         if (url.Length < 5)
            return "";

      }
      catch (Exception e) { }

      return StripChar(url);
   }

   public static string ExtractSatoriID(string blob)
   {
      string val = "";
      int pos1 = 0, pos2 = 0;
      try
      {
         pos1 = blob.IndexOf("\"Satori\",", pos2 + 1);
         if (pos1 < 3)
            return "";
         pos2 = blob.IndexOf("\"", pos1 + 25);
         if (pos2 < 3)
            return "";

         val = blob.Substring(pos1 + 18, pos2 - pos1 - 18);
         if (val.Length < 5)
            return "";

      }
      catch (Exception e) { }

      return val;
   }

   public static string GetDomainFromUrl(string link)
   {
      String[] parts = link.Split('/');
      return parts[0];
   }

   public static bool notFiltered(string uFollowersCount, string tSpamScore)
   {
      int followerCount = int.Parse(uFollowersCount);
      if (followerCount < 10)
         return false;
      double spamScore = double.Parse(tSpamScore);
      if (spamScore > 0.2)
         return false;
      return true;
   }

   public static int convertNumToInt(string num)
   {
      return int.Parse(num);
   }

   public static string StripChar(string text)
   {
      string txt = text;
      txt = txt.Replace("\\/", "/");
      return txt;
   }

   public static string ExtractURL(string blob)
   {
      string val = "";
      if (blob.IndexOf("TargetUrl") != (-1))
      {
         int pos1 = blob.IndexOf("TargetUrl");
         int pos2 = blob.IndexOf("\",\"", pos1 + 3);
         val = blob.Substring(pos1 + 12, pos2 - pos1 - 12);
         val = StripChar(val);
      }
      val = val.Replace("http://www.", "");
      val = val.Replace("https://www.", "");
      val = val.Replace("http://", "");
      val = val.Replace("https://", "");
      return val;
   }

   public static string ExtractPreGeneratedEntities(string blob)
   {
      string val = "";
      int pos1 = 0, pos2 = 0;
      string category = "";
      string entity = "";
      try
      {
         while (blob.IndexOf("SurfaceForm", pos2 + 10) != (-1))
         {
            pos1 = blob.IndexOf("\"Category\"", pos2 + 1);
            pos2 = blob.IndexOf("\",\"", pos1 + 13);
            category = blob.Substring(pos1 + 12, pos2 - pos1 - 12);
            pos1 = blob.IndexOf("SurfaceForm", pos2 + 1);
            pos2 = blob.IndexOf("\",\"", pos1 + 15);
            entity = StripChar(blob.Substring(pos1 + 14, pos2 - pos1 - 14));
            if (category != "DATE" && category != "TIME-POINT" && category != "ZIP" && category != "LOC-STREET" && val.IndexOf(entity) == (-1))
            {
               val += entity.Trim() + "#";
            }
         }
      }
      catch (Exception e) { }

      return val.Replace("??", "e");
   }

   public static string ExtractCategories(string blob)
   {
      int pos1 = 0, pos2 = 0;
      string category = "", categoryStr = "", categoryName = "", categoryValue = "";
      var hash = new HashSet<String>();
      try
      {
         pos1 = blob.IndexOf("OpenDirectoryProject\",\"Value\":\"{\\\"Categories");
         if (pos1 > -1)
         {
            pos2 = blob.IndexOf("]", pos1 + 2);
            if (pos2 > pos1)
               categoryStr = blob.Substring(pos1 + 13, pos2 - pos1 - 13);
         }
         else
            return "";
         //            return categoryStr;
         pos1 = 0;
         pos2 = 0;
         while (pos2 != (-1) && categoryStr.IndexOf("Name\\\":\\\"", pos2 + 1) != (-1))
         {
            pos1 = categoryStr.IndexOf("Name\\\":\\\"", pos2 + 2);
            if (pos1 == (-1))
               continue;
            pos2 = categoryStr.IndexOf("\\\",\\\"Score", pos1 + 1);
            if (pos2 == (-1))
               continue;
            categoryName = categoryStr.Substring(pos1 + 9, pos2 - pos1 - 9).Replace("\\/", "->");
            //Adding MSN Specific Categories
            if (categoryName == "Business->Automotive" || categoryName == "Recreation->Autos" || categoryName == "Recreation->Motorcycles" || categoryName == "Shopping->Vehicles")
               categoryName = "Autos";
            if (categoryName == "Computers->Systems" || categoryName == "Computers->Software" || categoryName == "Computers->Security" || categoryName == "Computers->Robotics" || categoryName == "Computers->Internet" || categoryName == "Computers->Hardware")
               categoryName = "Computers";

            if (categoryName.IndexOf("Business") >= 0)
               categoryName = "Money";
            if (categoryName.IndexOf("Adult") >= 0)
               continue;
            pos1 = pos2;
            //                categoryValue = "0.5";
            categoryValue = categoryStr.Substring(pos1 + 13, 3);
            if (categoryValue == "0.0" || categoryValue == "0.1" || categoryValue == "0.2" || categoryValue == "0.3")
               continue;
            if (hash.Contains(categoryName))
               continue;
            hash.Add(categoryName);
            if (categoryName == "Autos" && hash.Contains("Recreation"))
               category = category.Replace("Recreation#", "");
            if (categoryName == "Recreation" && hash.Contains("Autos"))
               continue;
            category += categoryName + "#";
            pos1 = categoryName.IndexOf("->");
            if (pos1 < 0)
               continue;

            //Adding Top Level category
            categoryName = categoryName.Substring(0, pos1);
            if (!hash.Contains(categoryName))
            {
               hash.Add(categoryName);
               category += categoryName + "#";
            }
         }
      }
      catch (Exception e) { }
      //        foreach(string cat in hash)
      //        {
      //            if (cat == "Recreation" && hash.Contains("Autos"))
      //                continue;
      //            category += categoryName + "#";
      //        }
      if (category.Length < 3)
         category = "";
      return category.Replace("\\/", "->");
   }


   public static int ExtractRepostingRatePerHour(string blob)
   {
      string val = "0";
      if (blob.IndexOf("PeakHourlyHits") != (-1))
      {
         int pos1 = blob.IndexOf("PeakHourlyHits");
         int pos2 = blob.IndexOf(",\"", pos1 + 3);
         val = blob.Substring(pos1 + 16, pos2 - pos1 - 16);
         val = StripChar(val);
      }
      return int.Parse(val);
   }

   public static string RemoveSpecialCharacters(string text)
   {
      int len = text.Length;
      string output = "";
      int val = 0;
      for (int i = 0; i < len; ++i)
      {
         val = (int)text[i];
         if ((val >= 65 && val <= 90) ||
             (val >= 97 && val <= 122) ||
             (text[i] == '\'' && i > 0 && i < (len - 1)) ||
             text[i] == '#')
            output = output + text[i];
         else
            output = output + ' ';
      }
      output = output.Replace("''", "'");
      output = output.Replace("' ", " ");
      output = output.Replace(" '", " ");
      output = output.Replace("'s ", " ");
      return output.Trim();
   }

   public static string ExtractURLTitle(string blob)
   {
      string val = "";
      if (blob.IndexOf("Title") != (-1))
      {
         int pos1 = blob.IndexOf("Title");
         int pos2 = blob.IndexOf(",\"", pos1 + 3);
         val = blob.Substring(pos1 + 8, pos2 - pos1 - 9);
         val = StripChar(val);
      }
      return val;
   }

   public static string DateFormat(string createdAt)
   {
      DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
      DateTime createdAtDate = unixEpoch.AddMilliseconds(Convert.ToDouble(createdAt));
      //        return createdAtDate.ToString("YYYY/MM/dd HH:mm");
      return createdAtDate.ToString("yyyy-MM-dd HH:mm:ss");
   }

   public static string DateFormatUtc(string createdAt, string utcOffset, double delta)
   {
      double utcCreatedAt = Convert.ToDouble(createdAt) + Convert.ToDouble(utcOffset) + delta;
      return DateFormat(utcCreatedAt + "");
   }

   public static string DateFormatFileUtc(string createdAt, string utcOffset, double delta)
   {
      double utcCreatedAt = Convert.ToDouble(createdAt) + Convert.ToDouble(utcOffset) + delta;
      DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
      DateTime createdAtDate = unixEpoch.AddMilliseconds(Convert.ToDouble(utcCreatedAt));
      return createdAtDate.ToFileTimeUtc() + "";
   }

   public static string Strip(string tCreatedAt)
   {
      string createdAt = tCreatedAt;
      createdAt = createdAt.Replace("\\/Date(", "");
      createdAt = createdAt.Replace(")\\/", "");
      return createdAt;
   }

}

public class ColumnMerger
{
   public static int IsHighQuality(int retweetCount, int retweetCountThreshold, bool uverified)
   {
      int highQualityValue = 0;
      if (retweetCount >= retweetCountThreshold)
      {
         highQualityValue |= 2;
      }
      if (uverified)
      {
         highQualityValue |= 4;
      }
      return highQualityValue;
   }
}

public class RandomSampler : Processor
{
   public override Schema Produces(string[] requestedColumns, string[] args, Schema input)
   {
      return input.Clone();
   }
   public override IEnumerable<Row> Process(RowSet input, Row outputRow, string[] args)
   {
      double sampleRate = double.Parse(args[0]);
      Random r = new Random();

      foreach (Row inputRow in input.Rows)
      {
         int IsHighQuality = (int)inputRow["IsHighQuality"].Value;
         if (IsHighQuality == 0)
         {
            double randomValue = r.NextDouble();
            if (randomValue > sampleRate)
            {
               continue;
            }
         }

         foreach (ColumnInfo columnInfo in input.Schema.Columns)
         {
            inputRow[columnInfo.Name].CopyTo(outputRow[columnInfo.Name]);
         }
         yield return outputRow;
      }
   }
}