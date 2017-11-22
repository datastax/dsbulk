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

# Install Ant and Maven
$ant_base = "$($dep_dir)\ant"
$ant_path = "$($ant_base)\apache-ant-1.9.7"
If (!(Test-Path $ant_path)) {
  Write-Host "Installing Ant"
  $ant_url = "https://www.dropbox.com/s/lgx95x1jr6s787l/apache-ant-1.9.7-bin.zip?dl=1"
  $ant_zip = "C:\Users\appveyor\apache-ant-1.9.7-bin.zip"
  (new-object System.Net.WebClient).DownloadFile($ant_url, $ant_zip)
  [System.IO.Compression.ZipFile]::ExtractToDirectory($ant_zip, $ant_base)
}
$env:PATH="$($ant_path)\bin;$($env:PATH)"

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
