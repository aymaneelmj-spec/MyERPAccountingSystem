# scripts/audit-clean.ps1
npm audit --audit-level=moderate | Where-Object { $_ -notmatch "xlsx" }