// generate-sample-targets.js
// Tạo các file JSON mẫu trong config/targets dựa trên danh sách module.
// Chạy: node scripts/generate-sample-targets.js

const fs = require('fs');
const path = require('path');

const OUT_DIR = path.resolve(__dirname, '..', 'config', 'targets');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const MODULES = [
  { id: 0, name: 'KhachHang', slug: 'khachhang', table: 'customers', total: 12400 },
  { id: 1, name: 'SanPham', slug: 'sanpham', table: 'products', total: 8600 },
  { id: 2, name: 'DonHang', slug: 'donhang', table: 'orders', total: 15200 },
  { id: 3, name: 'KhoHang', slug: 'khohang', table: 'inventory', total: 4800 },
  { id: 4, name: 'NhanVien', slug: 'nhanvien', table: 'employees', total: 1200 },
  { id: 5, name: 'NCC', slug: 'nhacungcap', table: 'suppliers', total: 980 },
  { id: 6, name: 'ThanhToan', slug: 'thanhtoan', table: 'payments', total: 3600 },
  { id: 7, name: 'BaoCao', slug: 'baocao', table: 'reports', total: 1420 },
];

function randInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

for (const m of MODULES) {
  const fileName = `${m.slug}.json`;
  const filePath = path.join(OUT_DIR, fileName);

  // mapping: simple example mapping
  const mapping = [
    { nguon: 'Id', dich: 'id_bak', type: 'int' },
    { nguon: 'Name', dich: 'name', type: 'nvarchar' },
    { nguon: 'Status', dich: 'status', type: 'nvarchar', rule: { type: 'map', map: { active: 1, inactive: 0 }, default: 0 } }
  ];

  // sample records (small set for demo)
  const samples = [];
  const sampleCount = Math.min(10, Math.max(3, Math.floor(m.total / 1000)));
  for (let i = 0; i < sampleCount; i++) {
    samples.push({
      Id: `${m.slug.toUpperCase()}_${1000 + i}`,
      Name: `${m.name} mẫu ${i + 1}`,
      Status: (Math.random() > 0.2) ? 'active' : 'inactive',
      Extra: `demo-${randInt(1,999)}`
    });
  }

  const targetObj = {
    nguon: { table: m.table },
    dich: { table: `${m.table}_bak` },
    mapping,
    options: { batchSize: 500, testLimit: 0 },
    samples
  };

  fs.writeFileSync(filePath, JSON.stringify(targetObj, null, 2), 'utf8');
  console.log('Wrote', filePath);
}

console.log('Done. Created sample target files in', OUT_DIR);
