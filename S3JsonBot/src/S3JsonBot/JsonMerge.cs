using System.Text.Json;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System.Linq;
using System.Text.Json.Nodes;
using Newtonsoft.Json.Linq;
using System.Text;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Buffers;

namespace S3JsonBot;

public class JsonMerge
{
    public void Run(JsonMergeRequest Request)
    {
        //string targetContent = "{\"test-key\": \"test-value\"}";
        //string targetContent = "{}";
        string targetContent = null;

        // Check if target exists
        if (IsS3FileExists(Request.Region, Request.Bucketname,
            Request.Target, "").Result)
        {
            // Read target content from S3 file
            targetContent = GetFileContent(Request.Region,
                Request.Bucketname, Request.Target);
        }

        //JContainer targetDocument = (JContainer)JToken.Parse(targetContent);

        List<string> sourceList = new List<string>();

        if (!string.IsNullOrEmpty(Request.SourcePrefix))
        {
            // Find the files in S3 for the prefix
            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = Request.Bucketname; //Amazon Bucket Name
            request.Prefix = Request.SourcePrefix; //Amazon S3 Folder path           

            var region = RegionEndpoint.GetBySystemName(Request.Region);
            var _client = new AmazonS3Client(region);

            ListObjectsResponse response = _client.ListObjectsAsync(request).Result;//_client - AmazonS3Client
        
            foreach (var t in response.S3Objects) 
            {
                if (t.Key.EndsWith(".json"))
                    sourceList.Add(t.Key);
            }
        }
        else
        {
            sourceList.AddRange(Request.Sources);
        }


        foreach (var source in sourceList)
        {
            // Read source content from S3 file
            string sourceContent = GetFileContent(Request.Region,
                Request.Bucketname, source);

            JContainer sourceDocument = (JContainer)JToken.Parse(sourceContent);

            if (Request.Fields.Length == 0)
            {
                // No fields to match on
                // do a normal merge of the documents

                //                targetDocument.Merge(sourceDocument,);
                //sourceDocument.Merge(targetDocument);

                var JsonMerged = JsonMerge.Merge(sourceContent, targetContent);

                targetContent = JsonMerged;

                Debug.WriteLine(targetContent);
            }
            else
            {
                List<string> fieldList = new List<string>();
                fieldList.AddRange(Request.Fields);

                var sourceResults = sourceDocument.Descendants()
                                 .OfType<JObject>()
                                 .Where(x => fieldList.Any(t1 => x.ContainsKey(t1)));

                JContainer targetDocument = (JContainer)JToken.Parse(targetContent);

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
                        matchFields,
                        Request.AllowMultipleMatches);

                // Print the matching pairs
                foreach (var pair in matchingPairs)
                {
                    pair.Item1.Merge(pair.Item2, new JsonMergeSettings
                    {
                        // union array values together to avoid duplicates
                        MergeArrayHandling = MergeArrayHandling.Union
                    });
                }

                targetContent = targetDocument.ToString();
            }
        }

        //var uu = targetDocument;

        // Write the targetDocument to file

        var stream = new MemoryStream(Encoding.UTF8.GetBytes(targetContent));

        SaveFile(Request.Region, Request.Bucketname, Request.Target, stream, "application/json");
    }

    public static string Merge(string originalJson, string newContent)
    {
        if (originalJson == null)
        {
            return newContent;
        }
        else if (newContent == null)
        {
            return originalJson;
        }

        var outputBuffer = new ArrayBufferWriter<byte>();

        using (JsonDocument jDoc1 = JsonDocument.Parse(originalJson))
        using (JsonDocument jDoc2 = JsonDocument.Parse(newContent))
        using (var jsonWriter = new Utf8JsonWriter(outputBuffer, new JsonWriterOptions { Indented = true }))
        {
            JsonElement root1 = jDoc1.RootElement;
            JsonElement root2 = jDoc2.RootElement;

            if (root1.ValueKind != JsonValueKind.Array && root1.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException($"The original JSON document to merge new content into must be a container type. Instead it is {root1.ValueKind}.");
            }

            if (root1.ValueKind == JsonValueKind.Array
                && root2.ValueKind == JsonValueKind.Object)
            {
                MergeObjectAndArray(jsonWriter, root2, root1);
            }
            else if (root1.ValueKind == JsonValueKind.Object
                && root2.ValueKind == JsonValueKind.Array)
            {
                MergeObjectAndArray(jsonWriter, root1, root2);
            }
            else if (root1.ValueKind == JsonValueKind.Array &&
                root2.ValueKind == JsonValueKind.Array)
            {
                MergeArrays(jsonWriter, root1, root2);
            }
            else if (root1.ValueKind == JsonValueKind.Object &&
                root2.ValueKind == JsonValueKind.Object)
            {
                MergeObjects(jsonWriter, root1, root2);
            }
            else
            {
                return originalJson;
            }
        }

        return Encoding.UTF8.GetString(outputBuffer.WrittenSpan);
    }

    private static void MergeObjectAndArray(Utf8JsonWriter jsonWriter, JsonElement objectElement, JsonElement arrayElement)
    {
        Debug.Assert(objectElement.ValueKind == JsonValueKind.Object);
        Debug.Assert(arrayElement.ValueKind == JsonValueKind.Array);

        jsonWriter.WriteStartObject();

        // Write all properties of the object
        foreach (JsonProperty property in objectElement.EnumerateObject())
        {
            property.WriteTo(jsonWriter);
        }

        // Write all elements of the array
        jsonWriter.WritePropertyName("arrayProperty");
        jsonWriter.WriteStartArray();

        foreach (JsonElement arrayItem in arrayElement.EnumerateArray())
        {
            arrayItem.WriteTo(jsonWriter);
        }

        jsonWriter.WriteEndArray();

        jsonWriter.WriteEndObject();
    }

    private static void MergeObjects(Utf8JsonWriter jsonWriter, JsonElement root1, JsonElement root2)
    {
        Debug.Assert(root1.ValueKind == JsonValueKind.Object);
        Debug.Assert(root2.ValueKind == JsonValueKind.Object);

        jsonWriter.WriteStartObject();

        // Write all the properties of the first document.
        // If a property exists in both documents, either:
        // * Merge them, if the value kinds match (e.g. both are objects or arrays),
        // * Completely override the value of the first with the one from the second, if the value kind mismatches (e.g. one is object, while the other is an array or string),
        // * Or favor the value of the first (regardless of what it may be), if the second one is null (i.e. don't override the first).
        foreach (JsonProperty property in root1.EnumerateObject())
        {
            string propertyName = property.Name;

            JsonValueKind newValueKind;

            if (root2.TryGetProperty(propertyName, out JsonElement newValue) && (newValueKind = newValue.ValueKind) != JsonValueKind.Null)
            {
                jsonWriter.WritePropertyName(propertyName);

                JsonElement originalValue = property.Value;
                JsonValueKind originalValueKind = originalValue.ValueKind;

                if (newValueKind == JsonValueKind.Object && originalValueKind == JsonValueKind.Object)
                {
                    MergeObjects(jsonWriter, originalValue, newValue); // Recursive call
                }
                else if (newValueKind == JsonValueKind.Array && originalValueKind == JsonValueKind.Array)
                {
                    MergeArrays(jsonWriter, originalValue, newValue);
                }
                else
                {
                    newValue.WriteTo(jsonWriter);
                }
            }
            else
            {
                property.WriteTo(jsonWriter);
            }
        }

        // Write all the properties of the second document that are unique to it.
        foreach (JsonProperty property in root2.EnumerateObject())
        {
            if (!root1.TryGetProperty(property.Name, out _))
            {
                property.WriteTo(jsonWriter);
            }
        }

        jsonWriter.WriteEndObject();
    }

    private static void MergeArrays(Utf8JsonWriter jsonWriter, JsonElement root1, JsonElement root2)
    {
        Debug.Assert(root1.ValueKind == JsonValueKind.Array);
        Debug.Assert(root2.ValueKind == JsonValueKind.Array);

        jsonWriter.WriteStartArray();

        // Write all the elements from both JSON arrays
        foreach (JsonElement element in root1.EnumerateArray())
        {
            element.WriteTo(jsonWriter);
        }
        foreach (JsonElement element in root2.EnumerateArray())
        {
            element.WriteTo(jsonWriter);
        }

        jsonWriter.WriteEndArray();
    }

    // Method to get matching pairs of JObjects with or without multiple matches
    static List<Tuple<JObject, JObject>> GetMatchingPairs(List<JObject> list1, List<JObject> list2, List<string> matchFields, bool allowMultipleMatches)
    {
        // Create a list to store matching pairs
        List<Tuple<JObject, JObject>> matchingPairs = new List<Tuple<JObject, JObject>>();

        // Create a dictionary for the second list based on the match fields
        Dictionary<string, List<JObject>> dict2 = new Dictionary<string, List<JObject>>();
        foreach (var obj2 in list2)
        {
            string key2 = GetKey(obj2, matchFields);
            if (!dict2.ContainsKey(key2))
            {
                dict2[key2] = new List<JObject>();
            }
            dict2[key2].Add(obj2);
        }

        // Iterate through the first list and find matching JObjects
        foreach (var obj1 in list1)
        {
            string key1 = GetKey(obj1, matchFields);
            if (dict2.ContainsKey(key1))
            {
                if (allowMultipleMatches)
                {
                    // Add all matching pairs if multiple matches are allowed
                    foreach (var obj2 in dict2[key1])
                    {
                        matchingPairs.Add(new Tuple<JObject, JObject>(obj1, obj2));
                    }
                }
                else
                {
                    // Add the first matching pair if only single matches are allowed
                    matchingPairs.Add(new Tuple<JObject, JObject>(obj1, dict2[key1].First()));
                }
            }
        }

        return matchingPairs;
    }

    // Helper function to generate a key based on the match fields
    static string GetKey(JObject obj, List<string> matchFields)
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

    private async Task<bool> IsS3FileExists(string Region, string bucketName, string fileName, string versionId)
    {
        var region = RegionEndpoint.GetBySystemName(Region);

        var s3Client = new AmazonS3Client(region);

        try
        {
            var request = new GetObjectMetadataRequest()
            {
                BucketName = bucketName,
                Key = fileName,
                VersionId = !string.IsNullOrEmpty(versionId) ? versionId : null
            };

            var response = await s3Client.GetObjectMetadataAsync(request);

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"IsFileExists: Error during checking if file exists in s3 bucket: {JsonConvert.SerializeObject(ex)}");
            return false;
        }
    }
}