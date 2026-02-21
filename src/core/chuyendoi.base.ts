import * as mssql from 'mssql';
import * as path from 'path';

/**
 * Chứa lớp cơ sở cho các module chuyển đổi.
 * Mục đích: người dùng kế thừa `ChuyenDoiBase` và override `mapRow` để định nghĩa cách map 1 dòng nguồn sang object đích.
 *
 * Quy ước:
 * - object trả về phải có trường `id_bak` (khóa đối chiếu) để lưu lịch sử và upsert.
 */
export type RunOptions = { batchSize?: number; testLimit?: number };

export abstract class ChuyenDoiBase {
  protected srcPool: mssql.ConnectionPool;
  protected dstPool: mssql.ConnectionPool;
  protected config: any;

  constructor(srcPool: mssql.ConnectionPool, dstPool: mssql.ConnectionPool, config: any) {
    this.srcPool = srcPool;
    this.dstPool = dstPool;
    this.config = config;
  }

  // Người kế thừa phải ghi đè: chuyển 1 hàng từ nguồn thành object đích
  abstract mapRow(row: any): any;

  /**
   * Áp dụng rule chuyển đổi cho một giá trị theo định nghĩa trong mapping
   * Hỗ trợ các loại: map (map từ text->value), toInt, toFloat, toBool, trim, date, default
   */
  protected applyTransform(rule: any, value: any) {
    try {
      if (rule == null) return value;
      if (rule.type === 'map') {
        // Hỗ trợ inline map (rule.map) hoặc tham chiếu map chung theo rule.mapName
        const theMap = rule.map || (this.config && this.config.valueMaps && rule.mapName ? this.config.valueMaps[rule.mapName] : null);
        if (theMap) {
          if (value in theMap) return theMap[value];
          const sval = value == null ? '' : String(value);
          if (sval in theMap) return theMap[sval];
          return rule.default !== undefined ? rule.default : null;
        }
      }
      if (rule.type === 'toInt') {
        if (value == null || value === '') return rule.default !== undefined ? rule.default : null;
        const n = parseInt(String(value).replace(/[^-0-9]/g, ''), 10);
        return isNaN(n) ? (rule.default !== undefined ? rule.default : null) : n;
      }
      if (rule.type === 'toFloat') {
        if (value == null || value === '') return rule.default !== undefined ? rule.default : null;
        const n = parseFloat(String(value).replace(/,/g, '.').replace(/[^0-9.\-]/g, ''));
        return isNaN(n) ? (rule.default !== undefined ? rule.default : null) : n;
      }
      if (rule.type === 'toBool') {
        if (value === true || value === 1 || value === '1' || String(value).toLowerCase() === 'true' || String(value).toLowerCase() === 'yes' || String(value).toLowerCase() === 'y') return true;
        if (value === false || value === 0 || value === '0' || String(value).toLowerCase() === 'false' || String(value).toLowerCase() === 'no' || String(value).toLowerCase() === 'n') return false;
        return rule.default !== undefined ? !!rule.default : null;
      }
      if (rule.type === 'trim') {
        if (value == null) return rule.default !== undefined ? rule.default : null;
        return String(value).trim();
      }
      if (rule.type === 'date') {
        if (!value) return rule.default !== undefined ? rule.default : null;
        const d = new Date(value);
        if (isNaN(d.getTime())) return rule.default !== undefined ? rule.default : null;
        return d; // caller may format as needed or DB driver will accept Date
      }
      // fallback: nếu có default
      if (rule.default !== undefined) return rule.default;
      return value;
    } catch (e) {
      return rule && rule.default !== undefined ? rule.default : null;
    }
  }

  async run(opts: RunOptions = {}) {
    const batchSize = opts.batchSize || this.config.options?.batchSize || 1000;
    const testLimit = opts.testLimit || this.config.options?.testLimit || 0;

    await this.ensureLichSuTable();

    let offset = 0;
    let processed = 0;
    while (true) {
      const q = `SELECT * FROM [${this.config.nguon.table}] ORDER BY 1 OFFSET ${offset} ROWS FETCH NEXT ${batchSize} ROWS ONLY`;
      const srcResult = await this.srcPool.request().query(q);
      const rows: any[] = srcResult.recordset || [];
      if (!rows.length) break;

      for (const r of rows) {
        const mapped = this.mapRow(r);
        const idBak = mapped['id_bak'] ?? null;
        if (idBak == null) continue;

        const skip = await this.shouldSkipByHistory(idBak);
        if (skip) continue;

        await this.upsertDestination(this.config.dich.table, mapped);
        await this.markHistory(idBak);

        processed++;
        if (testLimit && processed >= testLimit) break;
      }

      if (testLimit && processed >= testLimit) break;
      offset += batchSize;
    }

    return { processed };
  }

  protected async ensureLichSuTable() {
    const q = `IF OBJECT_ID('dbo.lichsu_chuyendoi','U') IS NULL BEGIN CREATE TABLE dbo.lichsu_chuyendoi (id INT IDENTITY(1,1) PRIMARY KEY, id_bak NVARCHAR(255) UNIQUE, last_run DATETIME) END`;
    await this.dstPool.request().query(q);
  }

  protected async shouldSkipByHistory(idBak: string) {
    try {
      const res = await this.dstPool.request().input('id', idBak).query('SELECT TOP 1 last_run FROM lichsu_chuyendoi WHERE id_bak = @id');
      const rows = res.recordset || [];
      if (!rows.length) return false;
      // đơn giản: nếu đã có bản ghi thì không skip (business rule có thể thay đổi)
      return false;
    } catch (e) {
      return false;
    }
  }

  protected async markHistory(idBak: string) {
    // upsert history
    await this.dstPool.request().input('id', idBak).query(`IF EXISTS (SELECT 1 FROM lichsu_chuyendoi WHERE id_bak = @id) BEGIN UPDATE lichsu_chuyendoi SET last_run = GETDATE() WHERE id_bak = @id END ELSE BEGIN INSERT INTO lichsu_chuyendoi (id_bak, last_run) VALUES (@id, GETDATE()) END`);
  }

  protected async upsertDestination(table: string, mapped: any) {
    const cols = Object.keys(mapped).filter(k => k !== 'id_bak');
    const setClause = cols.map(c => `[${c}] = @${c}`).join(', ');
    const insertCols = Object.keys(mapped).map(c => `[${c}]`).join(', ');
    const insertVals = Object.keys(mapped).map(c => `@${c}`).join(', ');

    const req = this.dstPool.request();
    for (const k of Object.keys(mapped)) {
      req.input(k, mapped[k]);
    }

    const q = `IF EXISTS (SELECT 1 FROM ${table} WHERE id_bak = @id_bak) BEGIN UPDATE ${table} SET ${setClause} WHERE id_bak = @id_bak END ELSE BEGIN INSERT INTO ${table} (${insertCols}) VALUES (${insertVals}) END`;
    await req.query(q);
  }
}
