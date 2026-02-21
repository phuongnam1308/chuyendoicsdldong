import { ChuyenDoiBase } from '../core/chuyendoi.base';

export class ViDuChuyenDoi extends ChuyenDoiBase {
  mapRow(row: any) {
    // Ví dụ: map trực tiếp theo cấu hình mapping nếu có
    const mapped: any = {};
    for (const m of this.config.mapping) {
      mapped[m.dich] = row[m.nguon];
    }
    return mapped;
  }
}
