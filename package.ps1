$date = Get-Date
$version = $date.ToString("yyyy-dd-M--HH-mm-ss")
$filename = "S3JsonBot-" + $version + ".zip"
cd .\S3JsonBot\src\S3JsonBot
dotnet lambda package ..\..\..\Packages\$filename --configuration Release -frun dotnet6 -farch arm64
cd ..\..\..