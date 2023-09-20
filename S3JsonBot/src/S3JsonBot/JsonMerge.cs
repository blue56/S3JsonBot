using System.Text.Json;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System.Linq;
using System.Text.Json.Nodes;
using Newtonsoft.Json.Linq;
using System.Text;

namespace S3JsonBot;

public class JsonMerge
{
    public void Run(JsonMergeRequest Request)
    {
        // Read source content from S3 file
        string sourceContent = GetFileContent(Request.Region,
            Request.Bucketname, Request.Source);

        JContainer sourceDocument = (JContainer)JToken.Parse(sourceContent);

        // Read target content from S3 file
        string targetContent = GetFileContent(Request.Region,
            Request.Bucketname, Request.Target);

        JContainer targetDocument = (JContainer)JToken.Parse(targetContent);

        List<string> fieldList = new List<string>();
        fieldList.AddRange(Request.Fields);

        var sourceResults = sourceDocument.Descendants()
                         .OfType<JObject>()
                         .Where(x => fieldList.Any(t1 => x.ContainsKey(t1)));

        var targetResults = targetDocument.Descendants()
                         .OfType<JObject>()
                         .Where(x => fieldList.Any(t1 => x.ContainsKey(t1)));

        // List of fields to match on
        List<string> matchFields = new List<string>();
        matchFields.AddRange(Request.Fields);

        // Call the method to get matching pairs
        List<Tuple<JObject, JObject>> matchingPairs = GetMatchingPairs(
                targetResults.ToList(),
                sourceResults.ToList(),
                matchFields);

        // Print the matching pairs
        foreach (var pair in matchingPairs)
        {
            pair.Item1.Merge(pair.Item2, new JsonMergeSettings
            {
                // union array values together to avoid duplicates
                MergeArrayHandling = MergeArrayHandling.Union
            });
        }

        var uu = targetDocument;

        // Write the targetDocument to file

        var stream = new MemoryStream(Encoding.UTF8.GetBytes(uu.ToString()));

        SaveFile(Request.Region, Request.Bucketname, Request.Target, stream, "application/json");
    }

    // Method to get matching pairs of JObjects
    public static List<Tuple<JObject, JObject>> GetMatchingPairs(List<JObject> list1, List<JObject> list2, List<string> matchFields)
    {
        // Create a dictionary for the first list based on the match fields
        Dictionary<string, JObject> dict1 = list1.ToDictionary(obj => GetKey(obj, matchFields));

        // Create a list to store matching pairs
        List<Tuple<JObject, JObject>> matchingPairs = new List<Tuple<JObject, JObject>>();

        // Iterate through the second list and find matching JObjects
        foreach (var obj2 in list2)
        {
            string key2 = GetKey(obj2, matchFields);
            if (dict1.ContainsKey(key2))
            {
                matchingPairs.Add(new Tuple<JObject, JObject>(dict1[key2], obj2));
            }
        }

        return matchingPairs;
    }

    // Helper function to generate a key based on the match fields
    public static string GetKey(JObject obj, List<string> matchFields)
    {
        return string.Join(",", matchFields.Select(field => obj[field].ToString()));
    }

    public string GetFileContent(string Region, string Bucketname, string Key)
    {
        var region = RegionEndpoint.GetBySystemName(Region);

        var _client = new AmazonS3Client(region);

        var request = new GetObjectRequest();
        request.BucketName = Bucketname;
        request.Key = Key;

        GetObjectResponse response = _client.GetObjectAsync(request).Result;
        StreamReader reader = new StreamReader(response.ResponseStream);
        string content = reader.ReadToEnd();
        return content;
    }

    public Stream GetFile(string Bucketname, string Key)
    {
        AmazonS3Client _client = new AmazonS3Client();

        var request = new GetObjectRequest();
        request.BucketName = Bucketname;
        request.Key = Key;

        GetObjectResponse response = _client.GetObjectAsync(request).Result;
        return response.ResponseStream;
    }

    public void SaveFile(string Region, string Bucketname, 
        string Key, Stream Stream, string ContentType)
    {
        var region = RegionEndpoint.GetBySystemName(Region);

        var _client = new AmazonS3Client(region);

        var putRequest = new PutObjectRequest
        {
            BucketName = Bucketname,
            Key = Key,
            ContentType = ContentType,
            InputStream = Stream
        };

        _client.PutObjectAsync(putRequest).Wait();
    }
}