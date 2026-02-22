import { Injectable } from '@nestjs/common';
import * as mssql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import { ChuyenDoiBase } from '../core/chuyendoi.base';

const THU_MUC_CAU_HINH = path.resolve(__dirname, '../../config');

@Injectable()
export class ChuyenDoiService {
  private poolCache: { [k: string]: mssql.ConnectionPool } = {};
  private valueMaps: { [k: string]: any } = {};
  // In-memory run manager: runId -> status/progress/errors
  private runs: { [runId: string]: any } = {};

  khoiTaoGiaoDien() {
    // Trả về mẫu JSON mapping và trạng thái env
    const mau = {
      nguon: {
        server: '<ten-server-nguon>',
        user: '<user>',
        password: '<mat-khau>',
        database: '<ten-db-nguon>',
        table: '<ten-bang-nguon>'
      },
      dich: {
        server: '<ten-server-dich>',
        user: '<user>',
        password: '<mat-khau>',
        database: '<ten-db-dich>',
        table: '<ten-bang-dich>'
      },
      mapping: [
        { nguon: 'Id', dich: 'id_bak', type: 'int' },
        { nguon: 'Name', dich: 'ten', type: 'nvarchar' }
      ],
      options: {
        batchSize: 1000,
        testLimit: 100
      }
    };

    let envExists = false;
    try {
      if (!fs.existsSync(THU_MUC_CAU_HINH)) fs.mkdirSync(THU_MUC_CAU_HINH, { recursive: true });
      envExists = fs.existsSync(path.join(THU_MUC_CAU_HINH, 'mapping.json'));
    } catch (e) {}

    return { mau, envExists };
  }

  async chay(body: any) {
    // body có thể là mapping hoặc chỉ lệnh run
    // ưu tiên lấy thông tin từ env nếu có (ví dụ: SRC_SERVER,SRC_USER,SRC_PASSWORD,... hoặc SRC_JSON và DST_JSON)
    let config: any = null;
    if (process.env.SRC_JSON && process.env.DST_JSON) {
      try {
        config = {
          nguon: JSON.parse(process.env.SRC_JSON),
          dich: JSON.parse(process.env.DST_JSON),
          mapping: process.env.MAPPING_JSON ? JSON.parse(process.env.MAPPING_JSON) : undefined,
          options: process.env.OPTIONS_JSON ? JSON.parse(process.env.OPTIONS_JSON) : undefined,
          moduleName: process.env.MODULE_NAME
        };
      } catch (e) {
        // ignore parse error
      }
    }

    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    if (!config) {
      if (body && Object.keys(body).length) {
        fs.writeFileSync(mappingPath, JSON.stringify(body, null, 2), 'utf8');
      }
      const configRaw = fs.existsSync(mappingPath) ? fs.readFileSync(mappingPath, 'utf8') : null;
      if (!configRaw) throw new Error('Không tìm thấy cấu hình mapping (env hoặc config/mapping.json)');
      config = JSON.parse(configRaw);
    }

    // Hỗ trợ nhiều bảng: đọc các file trong folder config/targets/*.json nếu tồn tại;
    // mỗi file target chứa object override (ví dụ chỉ cần { "nguon": { "table": "..." }, "dich": { "table": "..." } })
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');

    // Kiểm tra kết nối chung (nguồn/đích) trước khi bắt đầu xử lý các bảng
    try {
      await this.getPool(config.nguon);
      await this.getPool(config.dich);
    } catch (err) {
      const e: any = err;
      // Trả về lỗi rõ ràng, không tiếp tục xử lý
      return { error: 'Không thể kết nối tới nguồn hoặc đích chung: ' + (e && e.message ? e.message : e) };
    }
    let targets: any[] = [];
    try {
      if (fs.existsSync(targetsDir)) {
  const files = fs.readdirSync(targetsDir).filter((f: string) => f.endsWith('.json'));
        for (const f of files) {
          try {
            const raw = fs.readFileSync(path.join(targetsDir, f), 'utf8');
            const t = JSON.parse(raw);
            targets.push(t);
          } catch (e) {
            // skip invalid file
          }
        }
      }
    } catch (e) {}

    if (!targets.length) {
      targets = config.targets && Array.isArray(config.targets) ? config.targets : [config];
    }

    // load value maps nếu có
    try {
      const vmPath = path.join(THU_MUC_CAU_HINH, 'value-maps.json');
      if (fs.existsSync(vmPath)) this.valueMaps = JSON.parse(fs.readFileSync(vmPath, 'utf8'));
    } catch (e) { this.valueMaps = {}; }
    const summary: any[] = [];

    for (const t of targets) {
      // hợp nhất cấu hình target với cấu hình chung
      const cfg = {
        nguon: { ...(config.nguon || {}), ...(t.nguon || {}) },
        dich: { ...(config.dich || {}), ...(t.dich || {}) },
        mapping: t.mapping || config.mapping || [],
        options: { ...(config.options || {}), ...(t.options || {}) },
        moduleName: t.moduleName || config.moduleName
      };

  // gán valueMaps vào config để runner có thể sử dụng trong applyTransform
  (cfg as any).valueMaps = this.valueMaps || {};

      // Kiểm tra credential tối thiểu
      if (!cfg.nguon || !cfg.dich) {
        summary.push({ target: 'unknown', error: 'Thiếu cấu hình nguon hoặc dich' });
        continue;
      }
      if (!cfg.nguon.user || !cfg.nguon.password) {
        summary.push({ target: JSON.stringify(cfg.nguon), error: 'Nguồn thiếu user/password' });
        continue;
      }
      if (!cfg.dich.user || !cfg.dich.password) {
        summary.push({ target: JSON.stringify(cfg.dich), error: 'Đích thiếu user/password' });
        continue;
      }

      // Thử kết nối trước khi chạy
      let srcPoolT: mssql.ConnectionPool;
      let dstPoolT: mssql.ConnectionPool;
      try {
        srcPoolT = await this.getPool(cfg.nguon);
      } catch (err) {
        const e: any = err;
        summary.push({ target: cfg.nguon.table || 'nguon', error: 'Không thể kết nối tới nguồn: ' + (e && e.message ? e.message : e) });
        continue;
      }
      try {
        dstPoolT = await this.getPool(cfg.dich);
      } catch (err) {
        const e: any = err;
        summary.push({ target: cfg.dich.table || 'dich', error: 'Không thể kết nối tới đích: ' + (e && e.message ? e.message : e) });
        continue;
      }

      // Nạp module tuỳ chỉnh hoặc dùng DefaultRunner
      let runner: ChuyenDoiBase | null = null;
      if (cfg.moduleName) {
        try {
          const modPath = path.resolve(__dirname, `../modules/${cfg.moduleName}.ts`);
          if (fs.existsSync(modPath)) {
            const loaded = require(modPath);
            const cls = loaded[Object.keys(loaded)[0]];
            runner = new cls(srcPoolT, dstPoolT, cfg);
          }
        } catch (e) {
          runner = null;
        }
      }

      if (!runner) {
        class DefaultRunner extends ChuyenDoiBase {
          mapRow(row: any) {
            const mapped: any = {};
            for (const m of this.config.mapping) {
              // m có thể là chuỗi mapping đơn giản hoặc object { nguon, dich, rule }
              const src = m.nguon;
              const dst = m.dich;
              const raw = row[src];
              if (m.rule) {
                mapped[dst] = this.applyTransform(m.rule, raw);
              } else {
                mapped[dst] = raw;
              }
            }
            return mapped;
          }
        }
        runner = new DefaultRunner(srcPoolT, dstPoolT, cfg);
      }

      const res = await runner.run({ batchSize: cfg.options?.batchSize, testLimit: cfg.options?.testLimit });
      summary.push({ target: cfg.nguon.table + ' -> ' + cfg.dich.table, result: res });
    }

    return { summary };
  }

  async taoMapping(spec: any) {
    // spec: { modelName, nguonTable?, dichTable?, attributes: [{ name, type, rename? }], options? }
    if (!spec || !spec.modelName || !Array.isArray(spec.attributes)) throw new Error('Thiếu modelName hoặc attributes');
    const modelName = spec.modelName;
    const mapping = spec.attributes.map((a: any) => ({ nguon: a.name, dich: a.rename || a.name, type: a.type || 'nvarchar' }));

    // cập nhật config/mapping.json chung: đảm bảo mapping chung tồn tại
    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    let common: any = {};
    if (fs.existsSync(mappingPath)) {
      try { common = JSON.parse(fs.readFileSync(mappingPath, 'utf8')); } catch (e) { common = {}; }
    }
    common.mapping = common.mapping || mapping;
    // lưu mapping chung (không ghi table)
    fs.writeFileSync(mappingPath, JSON.stringify(common, null, 2), 'utf8');

    // tạo file target cho model (chứa table names override)
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
    if (!fs.existsSync(targetsDir)) fs.mkdirSync(targetsDir, { recursive: true });
    const targetObj: any = { nguon: {}, dich: {} };
    if (spec.nguonTable) targetObj.nguon.table = spec.nguonTable;
    if (spec.dichTable) targetObj.dich.table = spec.dichTable;
    const targetPath = path.join(targetsDir, `${modelName}.json`);
    fs.writeFileSync(targetPath, JSON.stringify(targetObj, null, 2), 'utf8');

    return { mapping, targetFile: targetPath };
  }

  async saveMapping(data: any) {
    try {
      // data: { folder, filename, nguonTable, dichTable, attributes }
      const folder = data.folder || '';
      const filename = data.filename.endsWith('.json') ? data.filename : `${data.filename}.json`;
      
      const targetDir = path.join(THU_MUC_CAU_HINH, 'targets', folder);
      
      // Tạo thư mục nếu chưa tồn tại
      if (!fs.existsSync(targetDir)) {
        fs.mkdirSync(targetDir, { recursive: true });
      }

      const filePath = path.join(targetDir, filename);
      const relativePath = folder ? path.join(folder, filename) : filename;

      // Đọc config cũ nếu có để giữ lại các options khác
      let existingConfig: any = {};
      if (fs.existsSync(filePath)) {
        try { existingConfig = JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch (e) {}
      }

      const configContent = {
        ...existingConfig,
        nguon: { ...existingConfig.nguon, table: data.nguonTable },
        dich: { ...existingConfig.dich, table: data.dichTable },
        mapping: data.attributes || [],
        notes: `Updated via Mapping Tool at ${new Date().toISOString()}`
      };

      fs.writeFileSync(filePath, JSON.stringify(configContent, null, 2), 'utf8');

      return { success: true, message: `Đã lưu file: ${relativePath}`, path: filePath, relativePath: relativePath.replace(/\\/g, '/') };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  async getTargetDetail(relativePath: string) {
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
    const filePath = path.join(targetsDir, relativePath);
    if (!fs.existsSync(filePath)) throw new Error('File không tồn tại: ' + relativePath);
    const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return { ...content, relativePath };
  }

  /**
   * Trả về trạng thái kết nối của cấu hình chung và từng target (nếu có)
   */
  async trangThaiKetNoi() {
    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    if (!fs.existsSync(mappingPath)) return { error: 'Chưa có mapping.json' };
    const config = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));

    const result: any = { chung: { nguon: null, dich: null }, targets: [] };
    // kiểm tra chung
    try {
      await this.getPool(config.nguon);
      result.chung.nguon = { ok: true };
    } catch (e) {
      result.chung.nguon = { ok: false, error: (e as any).message || String(e) };
    }
    try {
      await this.getPool(config.dich);
      result.chung.dich = { ok: true };
    } catch (e) {
      result.chung.dich = { ok: false, error: (e as any).message || String(e) };
    }

    // load targets
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
    if (fs.existsSync(targetsDir)) {
  const files = fs.readdirSync(targetsDir).filter((f: string) => f.endsWith('.json'));
      for (const f of files) {
        try {
          const raw = fs.readFileSync(path.join(targetsDir, f), 'utf8');
          const t = JSON.parse(raw);
          const cfg = {
            nguon: { ...(config.nguon || {}), ...(t.nguon || {}) },
            dich: { ...(config.dich || {}), ...(t.dich || {}) }
          };
          const tn: any = { file: f, nguon: null, dich: null };
          try {
            await this.getPool(cfg.nguon);
            tn.nguon = { ok: true };
          } catch (e) {
            tn.nguon = { ok: false, error: (e as any).message || String(e) };
          }
          try {
            await this.getPool(cfg.dich);
            tn.dich = { ok: true };
          } catch (e) {
            tn.dich = { ok: false, error: (e as any).message || String(e) };
          }
          result.targets.push(tn);
        } catch (e) {
          // skip invalid
        }
      }
    }

    return result;
  }

  /**
   * Trả về danh sách targets hiện có kèm cấu hình hợp nhất (dễ edit trên UI)
   */
  async getTargets() {
    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    if (!fs.existsSync(mappingPath)) return { targets: [] };
    const config = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
    const out: any[] = [];
    if (fs.existsSync(targetsDir)) {
  const files = fs.readdirSync(targetsDir).filter((f: string) => f.endsWith('.json'));
      for (const f of files) {
        try {
          const raw = fs.readFileSync(path.join(targetsDir, f), 'utf8');
          const t = JSON.parse(raw);
          const merged = {
            file: f,
            nguon: { ...(config.nguon || {}), ...(t.nguon || {}) },
            dich: { ...(config.dich || {}), ...(t.dich || {}) },
            mapping: t.mapping || config.mapping || [],
            options: t.options || config.options || {}
          };
          out.push(merged);
        } catch (e) {
          // skip
        }
      }
    }
    return { targets: out };
  }

  /**
   * Chay 1 target theo file name trong folder config/targets
   */
  async chayTarget(fileName: string) {
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
    const filePath = path.join(targetsDir, fileName);
    if (!fs.existsSync(filePath)) throw new Error('File target khong ton tai: ' + fileName);
    const raw = fs.readFileSync(filePath, 'utf8');
    const t = JSON.parse(raw);

    // load common mapping if exists
    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    const common = fs.existsSync(mappingPath) ? JSON.parse(fs.readFileSync(mappingPath, 'utf8')) : {};
    const config = {
      nguon: { ...(common.nguon || {}), ...(t.nguon || {}) },
      dich: { ...(common.dich || {}), ...(t.dich || {}) },
      mapping: t.mapping || common.mapping || [],
      options: { ...(common.options || {}), ...(t.options || {}) },
      moduleName: t.moduleName || common.moduleName
    };

    // For backward-compatibility keep synchronous behavior by calling chay
    const runConfig: any = { ...config };
    return this.chay(runConfig);
  }

  // Start a background run for a specific target file. Returns runId immediately.
  async startTargetRun(fileName: string) {
    const crypto = require('crypto');
    const runId = crypto.randomUUID ? crypto.randomUUID() : (Date.now() + '-' + Math.random().toString(36).slice(2,9));
    const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
    const filePath = path.join(targetsDir, fileName);
    if (!fs.existsSync(filePath)) throw new Error('File target khong ton tai: ' + fileName);
    const raw = fs.readFileSync(filePath, 'utf8');
    const t = JSON.parse(raw);

    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    const common = fs.existsSync(mappingPath) ? JSON.parse(fs.readFileSync(mappingPath, 'utf8')) : {};
    const cfg = {
      nguon: { ...(common.nguon || {}), ...(t.nguon || {}) },
      dich: { ...(common.dich || {}), ...(t.dich || {}) },
      mapping: t.mapping || common.mapping || [],
      options: { ...(common.options || {}), ...(t.options || {}) },
      moduleName: t.moduleName || common.moduleName,
      valueMaps: this.valueMaps || {}
    };

    // initialize run state
    this.runs[runId] = { runId, file: fileName, status: 'queued', total: 0, processed: 0, errors: [], startedAt: null, finishedAt: null };

    // run asynchronously
    (async () => {
      const runState = this.runs[runId];
      runState.status = 'starting';
      runState.startedAt = new Date().toISOString();
      // prepare pools
      let srcPool: mssql.ConnectionPool;
      let dstPool: mssql.ConnectionPool;
      try {
        srcPool = await this.getPool(cfg.nguon);
        dstPool = await this.getPool(cfg.dich);
      } catch (e) {
        runState.status = 'failed'; runState.error = (e && e.message) || String(e); runState.finishedAt = new Date().toISOString(); return;
      }

      // count total
      try {
        const totalRes = await srcPool.request().query(`SELECT COUNT(1) as c FROM [${cfg.nguon.table}]`);
        runState.total = (totalRes.recordset && totalRes.recordset[0] && totalRes.recordset[0].c) || 0;
      } catch (e) {
        runState.total = 0;
      }

      runState.status = 'running';
      let offset = 0;
      const batchSize = cfg.options?.batchSize || 1000;
      const testLimit = cfg.options?.testLimit || 0;
      let processed = 0;

      // construct runner
      let runner: ChuyenDoiBase | null = null;
      if (cfg.moduleName) {
        try {
          const modPath = path.resolve(__dirname, `../modules/${cfg.moduleName}.ts`);
          if (fs.existsSync(modPath)) {
            const loaded = require(modPath);
            const cls = loaded[Object.keys(loaded)[0]];
            runner = new cls(srcPool, dstPool, cfg);
          }
        } catch (e) { runner = null; }
      }
      if (!runner) {
        class DefaultRunnerLocal extends ChuyenDoiBase {
          mapRow(row: any) { const mapped: any = {}; for (const m of this.config.mapping) { const src = m.nguon; const dst = m.dich; const raw = row[src]; if (m.rule) mapped[dst] = this.applyTransform(m.rule, raw); else mapped[dst] = raw; } return mapped; }
        }
        runner = new DefaultRunnerLocal(srcPool, dstPool, cfg);
      }

      try {
        await this.ensureLichSuTable(dstPoolT, cfg.dich.database || '');
      } catch (e) { /* ignore */ }

      try {
        while (true) {
          const q = `SELECT * FROM [${cfg.nguon.table}] ORDER BY 1 OFFSET ${offset} ROWS FETCH NEXT ${batchSize} ROWS ONLY`;
          const srcResult = await srcPool.request().query(q);
          const rows: any[] = srcResult.recordset || [];
          if (!rows.length) break;
          for (const r of rows) {
            try {
              const mapped = runner!.mapRow(r);
              const idBak = mapped['id_bak'] ?? null;
              if (idBak == null) continue;
              // attempt upsert
              try {
                const skip = await this.shouldSkipByHistoryPool(dstPoolT, idBak);
                if (skip) continue;
                await this.upsertDestination(dstPoolT, cfg.dich.table, mapped);
                await this.markHistoryPool(dstPoolT, idBak);
                processed++;
                runState.processed = processed;
                if (testLimit && processed >= testLimit) break;
              } catch (e) {
                runState.errors.push({ id_bak: idBak, row: r, error: ((e as any) && (e as any).message) || String(e) });
              }
            } catch (e) {
              runState.errors.push({ id_bak: null, row: r, error: ((e as any) && (e as any).message) || String(e) });
            }
          }
          if (testLimit && processed >= testLimit) break;
          offset += batchSize;
        }
        runState.status = 'done';
        runState.finishedAt = new Date().toISOString();
      } catch (e) {
  runState.status = 'failed'; runState.error = ((e as any) && (e as any).message) || String(e); runState.finishedAt = new Date().toISOString();
      }
    })();

    return { runId };
  }

  getRunStatus(runId: string) {
    const r = this.runs[runId];
    if (!r) return null;
    return { runId: r.runId, file: r.file, status: r.status, total: r.total, processed: r.processed, errors: r.errors.length, startedAt: r.startedAt, finishedAt: r.finishedAt };
  }

  getRunErrors(runId: string) {
    const r = this.runs[runId];
    if (!r) return null;
    return r.errors;
  }

  async retryError(runId: string, id_bak: string) {
    const r = this.runs[runId];
    if (!r) throw new Error('Run not found');
    const idx = (r.errors || []).findIndex((e: any) => e.id_bak == id_bak);
    if (idx === -1) throw new Error('Error record not found');
    const errEntry = r.errors[idx];
    // attempt to remap and upsert
    try {
      const cfg = JSON.parse(fs.readFileSync(path.join(THU_MUC_CAU_HINH, 'mapping.json'), 'utf8')) || {};
    } catch (e) {}
    // naive retry: use same logic as in startTargetRun but operating on errEntry.row
    try {
      const common = {};
      // we don't have full cfg here; reconstruct minimal one from run
      const targetsDir = path.join(THU_MUC_CAU_HINH, 'targets');
      const file = r.file;
      const filePath = path.join(targetsDir, file);
      const t = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
      const commonCfg = fs.existsSync(mappingPath) ? JSON.parse(fs.readFileSync(mappingPath, 'utf8')) : {};
      const cfg = {
        nguon: { ...(commonCfg.nguon || {}), ...(t.nguon || {}) },
        dich: { ...(commonCfg.dich || {}), ...(t.dich || {}) },
        mapping: t.mapping || commonCfg.mapping || [],
        options: { ...(commonCfg.options || {}), ...(t.options || {}) },
        moduleName: t.moduleName || commonCfg.moduleName,
        valueMaps: this.valueMaps || {}
      };

  const srcPool = await this.getPool(cfg.nguon);
  const dstPool = await this.getPool(cfg.dich);
  class DefaultRunnerLocal extends ChuyenDoiBase { mapRow(row: any) { const mapped: any = {}; for (const m of this.config.mapping) { const src = m.nguon; const dst = m.dich; const raw = row[src]; if (m.rule) mapped[dst] = this.applyTransform(m.rule, raw); else mapped[dst] = raw; } return mapped; } }
  const runner = new DefaultRunnerLocal(srcPool, dstPool, cfg);
  const mapped = runner.mapRow(errEntry.row);
  await this.upsertDestination(dstPool, cfg.dich.table, mapped);
  await this.markHistoryPool(dstPool, mapped['id_bak']);
      // on success remove error entry
      r.errors.splice(idx, 1);
      return { success: true };
    } catch (e) {
      throw e;
    }
  }

  async lichSu(trang: number, kichThuoc: number) {
    const mappingPath = path.join(THU_MUC_CAU_HINH, 'mapping.json');
    if (!fs.existsSync(mappingPath)) return { total: 0, items: [] };
    const config = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
    const dstPool = await this.getPool(config.dich);
    const skip = (trang - 1) * kichThuoc;
    const totalRes = await dstPool.request().query('SELECT COUNT(1) as c FROM lichsu_chuyendoi');
    const total = totalRes.recordset[0].c || 0;
    const res = await dstPool.request().query(`SELECT * FROM lichsu_chuyendoi ORDER BY last_run DESC OFFSET ${skip} ROWS FETCH NEXT ${kichThuoc} ROWS ONLY`);
    return { total, items: res.recordset };
  }

  private async getPool(conn: any): Promise<mssql.ConnectionPool> {
    const key = `${conn.server}|${conn.user}|${conn.database}`;
    if (this.poolCache[key]) return this.poolCache[key];
    const cfg: mssql.config = {
      user: conn.user,
      password: conn.password,
      server: conn.server,
      database: conn.database,
      options: { encrypt: false, enableArithAbort: true }
    } as any;
    const pool = await new mssql.ConnectionPool(cfg).connect();
    this.poolCache[key] = pool;
    return pool;
  }

  private async ensureLichSuTable(pool: mssql.ConnectionPool, dbName?: string) {
    const q = `IF OBJECT_ID('dbo.lichsu_chuyendoi','U') IS NULL BEGIN CREATE TABLE dbo.lichsu_chuyendoi (id INT IDENTITY(1,1) PRIMARY KEY, id_bak NVARCHAR(255) UNIQUE, last_run DATETIME) END`;
    await pool.request().query(q);
  }

  private async upsertDestination(pool: mssql.ConnectionPool, table: string, mapped: any) {
    const cols = Object.keys(mapped).filter(k => k !== 'id_bak');
    const setClause = cols.map(c => `[${c}] = @${c}`).join(', ');
    const insertCols = Object.keys(mapped).map(c => `[${c}]`).join(', ');
    const insertVals = Object.keys(mapped).map(c => `@${c}`).join(', ');

    const req = pool.request();
    for (const k of Object.keys(mapped)) {
      req.input(k, mapped[k]);
    }

    const q = `IF EXISTS (SELECT 1 FROM ${table} WHERE id_bak = @id_bak) BEGIN UPDATE ${table} SET ${setClause} WHERE id_bak = @id_bak END ELSE BEGIN INSERT INTO ${table} (${insertCols}) VALUES (${insertVals}) END`;
    await req.query(q);
  }

  private async shouldSkipByHistoryPool(pool: mssql.ConnectionPool, idBak: string) {
    try {
      const res = await pool.request().input('id', idBak).query('SELECT TOP 1 last_run FROM lichsu_chuyendoi WHERE id_bak = @id');
      const rows = res.recordset || [];
      if (!rows.length) return false;
      return false;
    } catch (e) {
      return false;
    }
  }

  private async markHistoryPool(pool: mssql.ConnectionPool, idBak: string) {
    await pool.request().input('id', idBak).query(`IF EXISTS (SELECT 1 FROM lichsu_chuyendoi WHERE id_bak = @id) BEGIN UPDATE lichsu_chuyendoi SET last_run = GETDATE() WHERE id_bak = @id END ELSE BEGIN INSERT INTO lichsu_chuyendoi (id_bak, last_run) VALUES (@id, GETDATE()) END`);
  }
}
