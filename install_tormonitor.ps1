param(
    [string]$PythonVersion = "3.12.8",
    [ValidateSet("Auto", "CurrentUser", "AllUsers")]
    [string]$InstallScope = "Auto",
    [string]$ProjectRoot = $PSScriptRoot,
    [switch]$GlobalPackageInstall,
    [switch]$ReinstallPython
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$MinimumPythonVersion = [Version]"3.12.0"
$MaximumPythonVersion = [Version]"3.13.0"
$FallbackRequirements = @("paramiko>=3.4,<4")

function Write-Status {
    param([string]$Message)
    Write-Host "[tormonitor] $Message"
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-PythonVersionSupported {
    param([string]$VersionText)

    try {
        $version = [Version]$VersionText
    }
    catch {
        return $false
    }

    return $version -ge $MinimumPythonVersion -and $version -lt $MaximumPythonVersion
}

function Get-PythonVersionText {
    param([string]$PythonPath)

    try {
        return (& $PythonPath -c "import sys; print('.'.join(map(str, sys.version_info[:3])))" 2>$null).Trim()
    }
    catch {
        return ""
    }
}

function Resolve-PythonCandidates {
    param([string]$MajorMinorTag)

    $candidates = [System.Collections.Generic.List[string]]::new()

    $pyLauncher = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        try {
            $resolved = (& $pyLauncher.Source "-$MajorMinorTag" -c "import sys; print(sys.executable)" 2>$null).Trim()
            if ($resolved) {
                [void]$candidates.Add($resolved)
            }
        }
        catch {
        }
    }

    $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        [void]$candidates.Add($pythonCommand.Source)
    }

    $digits = $MajorMinorTag.Replace(".", "")
    $commonPaths = @(
        (Join-Path $env:LocalAppData "Programs\Python\Python$digits\python.exe"),
        (Join-Path $env:ProgramFiles "Python$digits\python.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Python$digits\python.exe")
    )

    foreach ($path in $commonPaths) {
        if ($path) {
            [void]$candidates.Add($path)
        }
    }

    return $candidates | Where-Object { $_ -and (Test-Path $_) } | Select-Object -Unique
}

function Resolve-UsablePython {
    param([string]$MajorMinorTag)

    foreach ($candidate in Resolve-PythonCandidates -MajorMinorTag $MajorMinorTag) {
        $versionText = Get-PythonVersionText -PythonPath $candidate
        if (Test-PythonVersionSupported -VersionText $versionText) {
            return $candidate
        }
    }

    return $null
}

function Install-Python {
    param(
        [string]$RequestedVersion,
        [string]$RequestedScope
    )

    $majorMinorTag = ([Version]$RequestedVersion).ToString(2)
    $installerUrl = "https://www.python.org/ftp/python/$RequestedVersion/python-$RequestedVersion-amd64.exe"
    $installerPath = Join-Path $env:TEMP "python-$RequestedVersion-amd64.exe"

    Write-Status "Downloading Python $RequestedVersion from $installerUrl"
    Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath

    $effectiveScope = $RequestedScope
    if ($effectiveScope -eq "Auto") {
        $effectiveScope = if (Test-IsAdministrator) { "AllUsers" } else { "CurrentUser" }
    }

    $installerArgs = @(
        "/quiet",
        "Include_pip=1",
        "Include_launcher=1",
        "InstallLauncherAllUsers=1",
        "AssociateFiles=1",
        "Shortcuts=0",
        "Include_test=0",
        "SimpleInstall=1",
        "PrependPath=1"
    )

    if ($effectiveScope -eq "AllUsers") {
        $installerArgs += "InstallAllUsers=1"
    }
    else {
        $installerArgs += "InstallAllUsers=0"
    }

    Write-Status "Installing Python $RequestedVersion for scope $effectiveScope"
    $process = Start-Process -FilePath $installerPath -ArgumentList $installerArgs -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        throw "Python installer exited with code $($process.ExitCode)."
    }

    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue

    $pythonPath = Resolve-UsablePython -MajorMinorTag $majorMinorTag
    if (-not $pythonPath) {
        throw "Python $RequestedVersion appears to be installed, but no usable interpreter was found afterward."
    }

    return $pythonPath
}

function Get-Requirements {
    param([string]$RootPath)

    $requirementsPath = Join-Path $RootPath "requirements.txt"
    if (-not (Test-Path $requirementsPath)) {
        return [PSCustomObject]@{
            RequirementsPath = $null
            Packages = $FallbackRequirements
        }
    }

    return [PSCustomObject]@{
        RequirementsPath = $requirementsPath
        Packages = @()
    }
}

function Invoke-CheckedPythonCommand {
    param(
        [string]$PythonPath,
        [string[]]$Arguments,
        [string]$Description
    )

    & $PythonPath @Arguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Install-Dependencies {
    param(
        [string]$PythonPath,
        [string]$RootPath,
        [switch]$UseGlobalInterpreter
    )

    if ($UseGlobalInterpreter) {
        $targetPython = $PythonPath
        Write-Status "Installing packages into the global Python interpreter"
    }
    else {
        $venvPath = Join-Path $RootPath ".venv"
        $venvPython = Join-Path $venvPath "Scripts\python.exe"
        if (-not (Test-Path $venvPython)) {
            Write-Status "Creating virtual environment at $venvPath"
            Invoke-CheckedPythonCommand -PythonPath $PythonPath -Arguments @("-m", "venv", $venvPath) -Description "Virtual environment creation"
        }
        $targetPython = $venvPython
        Write-Status "Installing packages into $venvPath"
    }

    Invoke-CheckedPythonCommand -PythonPath $targetPython -Arguments @("-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel") -Description "Base package installation"

    $requirements = Get-Requirements -RootPath $RootPath
    if ($requirements.RequirementsPath) {
        Invoke-CheckedPythonCommand -PythonPath $targetPython -Arguments @("-m", "pip", "install", "-r", $requirements.RequirementsPath) -Description "Requirements installation"
    }
    else {
        Invoke-CheckedPythonCommand -PythonPath $targetPython -Arguments (@("-m", "pip", "install") + $requirements.Packages) -Description "Fallback package installation"
    }

    Invoke-CheckedPythonCommand -PythonPath $targetPython -Arguments @("-c", "import paramiko") -Description "Paramiko import verification"
    return $targetPython
}

try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    $projectPath = (Resolve-Path $ProjectRoot).Path
    $majorMinorTag = ([Version]$PythonVersion).ToString(2)
    $pythonPath = $null

    if (-not $ReinstallPython) {
        $pythonPath = Resolve-UsablePython -MajorMinorTag $majorMinorTag
        if ($pythonPath) {
            $detectedVersion = Get-PythonVersionText -PythonPath $pythonPath
            Write-Status "Found compatible Python $detectedVersion at $pythonPath"
        }
    }

    if (-not $pythonPath) {
        $pythonPath = Install-Python -RequestedVersion $PythonVersion -RequestedScope $InstallScope
    }

    $runtimePython = Install-Dependencies -PythonPath $pythonPath -RootPath $projectPath -UseGlobalInterpreter:$GlobalPackageInstall

    Write-Status "Setup complete."
    Write-Host ""
    Write-Host "Python interpreter: $runtimePython"
    if ($GlobalPackageInstall) {
        Write-Host "Run monitor with: & '$runtimePython' '.\monitor_qb.py'"
        Write-Host "Run uploader with: & '$runtimePython' '.\upload_qb.py' --mode full"
    }
    else {
        Write-Host "Run monitor with: & '$runtimePython' '.\monitor_qb.py'"
        Write-Host "Run uploader with: & '$runtimePython' '.\upload_qb.py' --mode full"
    }
}
catch {
    Write-Error $_
    exit 1
}