$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "Starting Telegram bot stack from $root" -ForegroundColor Cyan
Write-Host "Rotating old log files first..." -ForegroundColor DarkGray
node .\rotate-logs.mjs

Write-Host "Launching bot.js + worker-bot.js through start-render.mjs" -ForegroundColor DarkGray

node .\start-render.mjs
