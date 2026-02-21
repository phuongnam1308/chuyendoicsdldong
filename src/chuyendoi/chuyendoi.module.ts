import { Module } from '@nestjs/common';
import { ChuyenDoiController } from './chuyendoi.controller';
import { ChuyenDoiService } from './chuyendoi.service';

@Module({
  controllers: [ChuyenDoiController],
  providers: [ChuyenDoiService],
})
export class ChuyenDoiModule {}
