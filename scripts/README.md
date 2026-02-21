# Generate sample target JSON files

This script creates sample target files under `config/targets/` for the 8 modules used by the UI.

Usage:

```powershell
node scripts/generate-sample-targets.js
```

It writes files like `khachhang.json`, `sanpham.json`, etc. Each file contains:

- `nguon.table` and `dich.table`
- `mapping` array
- `options` for batch sizes
- `samples` small array of example rows for testing UI

The service will pick these files up automatically from `config/targets/`.
