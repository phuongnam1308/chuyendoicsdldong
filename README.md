# DataSync - Khung chuyển đổi dữ liệu (NestJS)

Dự án mẫu bằng tiếng Việt để chuyển dữ liệu giữa hai SQL Server trong cùng mạng. Người dùng chỉnh sửa `config/mapping.json` để định nghĩa nguồn, đích và mapping trường.

Endpoints:

- `POST /api/chuyendoi/tao-mapping` => gửi mô tả model (tên + thuộc tính) để tạo mapping và file target (mỗi bảng 1 file)

Chạy trên Windows PowerShell:

Ví dụ tạo mapping qua API:

POST /api/chuyendoi/tao-mapping
Body JSON:

```json
{
  "modelName": "khachhang",
  "nguonTable": "KHACHHANG",
  "dichTable": "KHACHHANG_DST",
  "attributes": [
    { "name": "Id", "type": "int", "rename": "id_bak" },
    { "name": "FullName", "type": "nvarchar", "rename": "ten" }
  ]
}
```

Kết quả: sẽ sinh `config/targets/khachhang.json` và cập nhật `config/mapping.json` mapping chung.

```powershell
# Cài dependencies
npm install

# Chạy dev
npm run start:dev

# Mở trình duyệt vào http://localhost:3000
```

Ghi chú:

- Tạo thư mục `config` và tệp `mapping.json` nếu muốn cấu hình sẵn.
- Dự án là khung mẫu; bạn cần cung cấp thông tin kết nối SQL Server đúng để chạy.

Sử dụng biến môi trường (tùy chọn):

Bạn có thể thiết lập cấu hình qua biến môi trường (dùng `.env`):

- `SRC_JSON` và `DST_JSON` chứa JSON mô tả kết nối (server,user,password,database,table).
- `MAPPING_JSON` chứa mảng mapping.
- `MODULE_NAME` tên module tuỳ chỉnh trong `src/modules/<module>.ts`.

Ví dụ bạn có thể sao chép `.env.example` thành `.env` và sửa lại.

Tạo module kế thừa (ví dụ):

1. Tạo file `src/modules/ten_module.chuyendoi.ts`.
2. Export một lớp kế thừa `ChuyenDoiBase` và override `mapRow(row: any)` để trả về object đích có trường `id_bak`.

Ví dụ ngắn (copy từ `src/modules/vi_du.chuyendoi.ts`):

```ts
import { ChuyenDoiBase } from "../core/chuyendoi.base";

export class TenModuleChuyenDoi extends ChuyenDoiBase {
  mapRow(row: any) {
    // mapping tuỳ chỉnh
    const mapped: any = {};
    for (const m of this.config.mapping) {
      mapped[m.dich] = row[m.nguon];
    }
    return mapped;
  }
}
```

3. Đặt `moduleName` trong `config/mapping.json` hoặc `MODULE_NAME` trong env là tên file (không kèm đường dẫn), ví dụ `vi_du.chuyendoi`.

Hỗ trợ nhiều bảng - mỗi file một bảng:

- Thay vì đặt tất cả targets trong một file, bạn có thể tạo thư mục `config/targets/` và thêm một file JSON cho mỗi bảng muốn chuyển.
- Mỗi file chỉ cần phần override, ví dụ:

`config/targets/target-bang1.json`:

```json
{ "nguon": { "table": "BangNguon" }, "dich": { "table": "BangDich" } }
```

Service sẽ tự load tất cả file `.json` trong `config/targets/` và chạy tuần tự.

Lưu ý: cấu hình chung (`config/mapping.json` hoặc env `SRC_JSON`/`DST_JSON`) không còn chứa trường `table`. Trường `table` phải được khai báo trong mỗi file trong `config/targets/`.

Transform rules (chuyển đổi giá trị) trong `mapping`:

- rule.type = "map": map giá trị text sang số/chuỗi. Ví dụ: { "type": "map", "map": { "Mới": 0, "Hoạt động": 1 }, "default": 0 }
- rule.type = "toInt": cố gắng convert sang integer, có thể dùng "default" khi không parse được
- rule.type = "toFloat": convert sang float, xử lý dấu phẩy và ký tự không số
- rule.type = "toBool": convert sang boolean từ '1','0','true','false','yes','no'
- rule.type = "trim": loại khoảng trắng hai đầu
- rule.type = "date": convert sang Date() (nếu invalid dùng default nếu có)
- rule.default: giá trị mặc định nếu không thể chuyển đổi

Ví dụ map chữ -> số (trong `mau-khachhang.json`):

```json
{
  "nguon": "StatusText",
  "dich": "status",
  "type": "int",
  "rule": {
    "type": "map",
    "map": { "Mới": 0, "Hoạt động": 1, "Đã xóa": 2 },
    "default": 0
  }
}
```

Hệ thống sẽ áp dụng `rule` nếu mapping định nghĩa trường `rule`. Nếu không có rule, giá trị nguồn được copy thẳng sang cột đích.
