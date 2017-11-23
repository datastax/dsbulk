Add-Type -AssemblyName System.IO.Compression.FileSystem

$dep_dir="C:\Users\appveyor\deps"
If (!(Test-Path $dep_dir)) {
  Write-Host "Creating $($dep_dir)"
  New-Item -Path $dep_dir -ItemType Directory -Force
}

$openssl_platform = "Win32"
$vc_platform = "x86"
If ($env:PLATFORM -eq "X64") {
  $vc_platform = "x64"
}

$env:JAVA_HOME="C:\Program Files\Java\jdk$($env:java_version)"
# The configured java version to test with.

$maven_base = "$($dep_dir)\maven"
$maven_path = "$($maven_base)\apache-maven-3.2.5"
If (!(Test-Path $maven_path)) {
  Write-Host "Installing Maven"
  $maven_url = "https://www.dropbox.com/s/fh9kffmexprsmha/apache-maven-3.2.5-bin.zip?dl=1"
  $maven_zip = "C:\Users\appveyor\apache-maven-3.2.5-bin.zip"
  (new-object System.Net.WebClient).DownloadFile($maven_url, $maven_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($maven_zip, $maven_base)
}
$env:M2_HOME="$($maven_path)"
$env:PATH="$($maven_path)\bin;$($env:PATH)"
