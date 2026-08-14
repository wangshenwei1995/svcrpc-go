# 生成 gRPC Go 代码（proto 变更后运行）。
# 用法: powershell -ExecutionPolicy Bypass -File scripts\gen.ps1
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$protoc = Join-Path $root '.tools\protoc\bin\protoc.exe'

if (-not (Test-Path $protoc)) {
    $c = Get-Command protoc -ErrorAction SilentlyContinue
    if (-not $c) {
        Write-Error "protoc 未找到。请安装 protoc 并放入 PATH，或下载到 .tools\protoc\bin\protoc.exe。"
        exit 1
    }
    $protoc = $c.Source
}

$plugins = "$env:USERPROFILE\go\bin"
if (-not (Test-Path "$plugins\protoc-gen-go.exe")) {
    Write-Host "installing protoc-gen-go / protoc-gen-go-grpc ..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
}

$env:PATH = "$plugins;$env:PATH"
Push-Location $root
try {
    & $protoc --proto_path=proto --go_out=. --go_opt=module=svcrpc `
        --go-grpc_out=. --go-grpc_opt=module=svcrpc proto/invoke.proto
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Write-Host "OK: gen/invoke/ 已更新"
} finally {
    Pop-Location
}
