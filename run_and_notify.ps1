# Requires: PowerShell 5+
$ErrorActionPreference = "Stop"

# --- Forza UTF-8 per evitare UnicodeEncodeError ---
$env:PYTHONIOENCODING = "utf-8"
try {
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
} catch {}

# === TELEGRAM ===
$TG_TOKEN   = "8429976502:AAGApe-HkCmmgEEcbwzEqPZnZzwBsyyobFM"
$TG_CHAT_ID = "302113556"

# === COMANDO SCREENER ===
$cmd = @(
  "python", "test3.py",
  "--seed-from", "perps",
  "--workers", "12",
  "--min-liq", "200000",
  "--max-tickers-scan", "40",
  "--dominance", "0.30",
  "--skip-unchanged-days", "0",
  "--rps-cg", "0.5",
  "--rps-ds", "2.0",
  "--funnel-show", "100"
)

Write-Host ">> Running screener..."
$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName  = $cmd[0]
$startInfo.Arguments = ($cmd[1..($cmd.Count-1)] -join " ")
$startInfo.RedirectStandardOutput = $true
$startInfo.RedirectStandardError  = $true
$startInfo.UseShellExecute        = $false
$startInfo.CreateNoWindow         = $true

$proc = New-Object System.Diagnostics.Process
$proc.StartInfo = $startInfo
[void]$proc.Start()
$stdout = $proc.StandardOutput.ReadToEnd()
$stderr = $proc.StandardError.ReadToEnd()
$proc.WaitForExit()
$code = $proc.ExitCode

if ([string]::IsNullOrWhiteSpace($stdout) -and -not [string]::IsNullOrWhiteSpace($stderr)) {
  $output = $stderr
} elseif ([string]::IsNullOrWhiteSpace($stdout) -and [string]::IsNullOrWhiteSpace($stderr)) {
  $output = "(nessun output dallo screener)"
} else {
  $output = $stdout + (if ($stderr) {"`n--- STDERR ---`n$stderr"} else {""})
}

# Troncamento per Telegram
$MAX = 3900
if ($output.Length -gt $MAX) {
  $output = $output.Substring(0, $MAX) + "`n`n…(troncato)"
}

$header = "✅ Screener OK"
if ($code -ne 0) { $header = "❌ Screener ERROR ($code)" }
$message = "$header`n`n$output"

$uri = "https://api.telegram.org/bot$TG_TOKEN/sendMessage"
$body = @{
  chat_id = $TG_CHAT_ID
  text    = $message
}

try {
  Invoke-RestMethod -Uri $uri -Method Post -Body $body -ContentType "application/x-www-form-urlencoded" | Out-Null
  Write-Host ">> Messaggio inviato a Telegram."
} catch {
  Write-Host ">> ERRORE invio Telegram: $($_.Exception.Message)"
  if ($_.Exception.Response) {
    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
    $respText = $reader.ReadToEnd()
    Write-Host ">> Response: $respText"
  }
  exit 1
}
