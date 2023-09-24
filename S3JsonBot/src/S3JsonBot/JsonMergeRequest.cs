namespace S3JsonBot;

public class JsonMergeRequest
{
    public string Region { get; set; }
    public string Bucketname { get; set; }
    public string Source { get; set; }
    public string Target { get; set; }
    public string[] Fields { get; set; }
    public bool AllowMultipleMatches { get; set; }
}