import { Module } from '@nestjs/common';
import { ChuyenDoiModule } from './chuyendoi/chuyendoi.module';

@Module({
  imports: [ChuyenDoiModule],
})
export class AppModule {}
