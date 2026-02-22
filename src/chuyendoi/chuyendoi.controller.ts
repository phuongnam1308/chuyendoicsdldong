import { Controller, Get, Post, Body, Query, Header } from '@nestjs/common';
import { ChuyenDoiService } from './chuyendoi.service';
import { ApiTags, ApiOperation } from '@nestjs/swagger';
import { ApiProperty } from '@nestjs/swagger';
import * as fs from 'fs';
import * as path from 'path';

class LichSuQueryDto {
  @ApiProperty({ required: false, description: 'Số trang (mặc định 1)' })
  trang?: number;

  @ApiProperty({ required: false, description: 'Kích thước trang (mặc định 100)' })
  kichThuoc?: number;
}

// Cấu hình đường dẫn lưu trữ
const ROOT_DIR = process.cwd();
const CONFIG_DIR = path.join(ROOT_DIR, 'config', 'targets');
const MODULE_ROOT = path.join(CONFIG_DIR, 'Module');

// Đảm bảo thư mục gốc tồn tại
if (!fs.existsSync(MODULE_ROOT)) fs.mkdirSync(MODULE_ROOT, { recursive: true });

/**
 * Controller cho các API chuyển đổi dữ liệu.
 * Tất cả mô tả bằng tiếng Việt để dễ đọc.
 */
@ApiTags('chuyendoi')
@Controller('chuyendoi')
export class ChuyenDoiController {
  constructor(private readonly cs: ChuyenDoiService) {}

  @ApiOperation({ summary: 'Trả về mẫu JSON mapping và trạng thái cấu hình' })
  @Get('khoi-tao-giao-dien')
  khoiTaoGiaoDien() {
    // Trả về file JSON mẫu và trạng thái hiện tại (cả khi chưa có env)
    return this.cs.khoiTaoGiaoDien();
  }

  @ApiOperation({ summary: 'Chạy chuyển đổi theo mapping (hoặc dùng mapping đã lưu)' })
  @Post('chay')
  chay(@Body() body: any) {
    return this.cs.chay(body);
  }

  @ApiOperation({ summary: 'Tạo mapping từ mô tả model (tên + thuộc tính)' })
  @Post('tao-mapping')
  taoMapping(@Body() body: any) {
    // body: { modelName, nguonTable?, dichTable?, attributes: [{name,type,rename?}], options? }
    return this.cs.taoMapping(body);
  }

  @ApiOperation({ summary: 'Xem lịch sử chuyển đổi (phân trang)' })
  @Get('lich-su')
  lichSu(@Query() q: LichSuQueryDto) {
    const t = q && q.trang ? Number(q.trang) : 1;
    const k = q && q.kichThuoc ? Number(q.kichThuoc) : 100;
    return this.cs.lichSu(t, k);
  }

  @ApiOperation({ summary: 'Kiểm tra trạng thái kết nối (nguồn/đích) và các target' })
  @Get('trang-thai-ket-noi')
  trangThaiKetNoi() {
    return this.cs.trangThaiKetNoi();
  }

  @ApiOperation({ summary: 'Liệt kê các targets (mỗi file một bảng) để xem/điều chỉnh' })
  @Get('targets')
  targets() {
    // Logic quét thư mục Module để trả về danh sách targets và config
    try {
      const response = { targets: [], moduleConfigs: {} };

      if (fs.existsSync(MODULE_ROOT)) {
        const modules = fs.readdirSync(MODULE_ROOT, { withFileTypes: true });

        for (const dirent of modules) {
          if (!dirent.isDirectory()) continue;
          const modName = dirent.name;
          const modPath = path.join(MODULE_ROOT, modName);

          // Mặc định khởi tạo config rỗng cho module để đảm bảo nó luôn hiện diện
          response.moduleConfigs[modName] = { module: modName, targets: [] };

          // 1. Đọc config.json của module
          const configPath = path.join(modPath, 'config.json');
          if (fs.existsSync(configPath)) {
            try {
              response.moduleConfigs[modName] = JSON.parse(fs.readFileSync(configPath, 'utf8'));
            } catch (e) {}
          }

          // 2. Quét các file .json target trong module
          const files = fs.readdirSync(modPath);
          for (const file of files) {
            if (!file.toLowerCase().endsWith('.json') || file === 'config.json') continue;

            try {
              const content = JSON.parse(fs.readFileSync(path.join(modPath, file), 'utf8'));
              
              // Tìm thứ tự chạy (runOrder) từ config module
              let order = 999;
              const tCfg = response.moduleConfigs[modName]?.targets?.find((t: any) => t.file.includes(file));
              if (tCfg) order = tCfg.order;

              response.targets.push({
                file: `Module/${modName}/${file}`, // Format chuẩn cho Frontend phân tích
                nguon: content.nguon,
                dich: content.dich,
                total: content.total || 0,
                notes: content.notes,
                runOrder: order
              });
            } catch (e) { console.error(`Lỗi đọc target ${file}`, e); }
          }
        }
      }
      return response;
    } catch (e) { throw new Error(e.message); }
  }

  @ApiOperation({ summary: 'Chạy chuyển đổi cho 1 target (gửi file name trong body: { file: "nguoidung.json" })' })
  @Post('chay-target')
  async chayTarget(@Body() body: { file: string }) {
    if (!body || !body.file) throw new Error('Thiếu tham số file');
    return this.cs.chayTarget(body.file);
  }

  @ApiOperation({ summary: 'Tạo Module mới (thư mục + config.json)' })
  @Post('tao-module')
  taoModule(@Body() body: { moduleName: string; description?: string }) {
    try {
      const { moduleName, description } = body;
      if (!moduleName) throw new Error('Thiếu tên module');

      const safeName = moduleName.replace(/[^a-zA-Z0-9_\-]/g, '');
      const dirPath = path.join(MODULE_ROOT, safeName);

      if (fs.existsSync(dirPath)) throw new Error('Module đã tồn tại');

      fs.mkdirSync(dirPath, { recursive: true });

      // Tạo file config.json mặc định để quản lý thứ tự chạy
      const cfg = {
        module: safeName,
        description: description || '',
        createdAt: new Date().toISOString(),
        runSequential: true,
        targets: []
      };
      fs.writeFileSync(path.join(dirPath, 'config.json'), JSON.stringify(cfg, null, 2));

      return { success: true, path: dirPath };
    } catch (e) { throw new Error(e.message); }
  }

  // API Tạo Target mà bạn cần đây
  @ApiOperation({ summary: 'Tạo Target mới trong Module' })
  @Post('tao-target')
  taoTarget(@Body() body: { moduleName: string; fileName: string; sourceTable: string; destTable: string }) {
    try {
      const { moduleName, fileName, sourceTable, destTable } = body;
      const safeMod = moduleName.replace(/[^a-zA-Z0-9_\-]/g, '');
      const safeFile = fileName.toLowerCase().endsWith('.json') ? fileName : `${fileName}.json`;
      
      const modDir = path.join(MODULE_ROOT, safeMod);
      if (!fs.existsSync(modDir)) throw new Error('Module không tồn tại');

      const filePath = path.join(modDir, safeFile);
      if (fs.existsSync(filePath)) throw new Error('File target đã tồn tại');

      // Nội dung file target
      const content = {
        nguon: { table: sourceTable },
        dich: { table: destTable },
        mapping: [],
        options: { batchSize: 500, logDir: "logs" },
        logFile: `logs/history_${safeMod}_${safeFile}`,
        notes: `Target ${safeFile}`,
        total: 0
      };

      fs.writeFileSync(filePath, JSON.stringify(content, null, 2));

      // Cập nhật config.json của module để thêm vào danh sách chạy
      const cfgPath = path.join(modDir, 'config.json');
      if (fs.existsSync(cfgPath)) {
        const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
        cfg.targets = cfg.targets || [];
        cfg.targets.push({
          order: cfg.targets.length + 1,
          file: `Module\\${safeMod}\\${safeFile}`,
          notes: content.notes
        });
        fs.writeFileSync(cfgPath, JSON.stringify(cfg, null, 2));
      }

      return { success: true, file: `Module/${safeMod}/${safeFile}` };
    } catch (e) { throw new Error(e.message); }
  }

  @ApiOperation({ summary: 'Bắt đầu chạy 1 target theo file (chạy bất đồng bộ, trả về runId)' })
  @Post('start-run')
  async startRun(@Body() body: { file: string }) {
    if (!body || !body.file) throw new Error('Thiếu tham số file');
    return this.cs.startTargetRun(body.file);
  }

  @ApiOperation({ summary: 'Lấy trạng thái run theo runId' })
  @Get('run-status')
  async runStatus(@Query('runId') runId: string) {
    if (!runId) throw new Error('Thiếu runId');
    return this.cs.getRunStatus(runId);
  }

  @ApiOperation({ summary: 'Lấy danh sách lỗi của 1 run' })
  @Get('run-errors')
  async runErrors(@Query('runId') runId: string) {
    if (!runId) throw new Error('Thiếu runId');
    return this.cs.getRunErrors(runId);
  }

  @ApiOperation({ summary: 'Thử lại 1 lỗi trong run (id_bak)' })
  @Post('run-retry')
  async runRetry(@Body() body: { runId: string; id_bak: string }) {
    if (!body || !body.runId || !body.id_bak) throw new Error('Thiếu runId hoặc id_bak');
    return this.cs.retryError(body.runId, body.id_bak);
  }

  @ApiOperation({ summary: 'Lưu cấu hình mapping chi tiết từ giao diện Mapping Tool' })
  @Post('save-mapping')
  async saveMapping(@Body() body: any) {
    return this.cs.saveMapping(body);
  }

  @ApiOperation({ summary: 'Lấy chi tiết cấu hình target để chỉnh sửa' })
  @Get('target-detail')
  async getTargetDetail(@Query('file') file: string) {
    if (!file) throw new Error('Thiếu tham số file');
    return this.cs.getTargetDetail(file);
  }

  @ApiOperation({ summary: 'Trang công cụ mapping chi tiết (HTML)' })
  @Get('mapping-tool')
  @Header('Content-Type', 'text/html')
  async mappingToolHtml() {
    try {
      const p = path.resolve(__dirname, '../../public/mapping-tool.html');
      if (fs.existsSync(p)) {
        return fs.readFileSync(p, 'utf8');
      }
    } catch (e) {}
    return `<!doctype html><html><body><h1>Không tìm thấy public/mapping-tool.html</h1></body></html>`;
  }

  @ApiOperation({ summary: 'Trang index hiển thị UI (HTML/CSS/JS) — trả về file public/index.html' })
  @Get('index')
  @Header('Content-Type', 'text/html')
  async indexHtml() {
    // Trả về file public/index.html để UI có CSS/JS đầy đủ khi truy cập qua API
    try {
      const fs = require('fs');
      const path = require('path');
      const p = path.resolve(__dirname, '../../public/index.html');
      if (fs.existsSync(p)) {
        return fs.readFileSync(p, 'utf8');
      }
    } catch (e) {
      // fallthrough to minimal HTML
    }
    // Fallback: trả về một trang nhỏ nếu file không tồn tại
    return `<!doctype html><html><head><meta charset="utf-8"><title>Index</title></head><body><h1>UI tạm không có</h1><p>Không tìm thấy public/index.html trên server.</p></body></html>`;
  }
}
