import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import express from 'express';
import * as dotenv from 'dotenv';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { ExpressAdapter, NestExpressApplication } from '@nestjs/platform-express';

// load .env nếu có
dotenv.config();

/**
 * Khởi tạo ứng dụng NestJS và Swagger docs (tiếng Việt)
 * - API prefix: /api
 * - Swagger UI: /api/docs
 */
async function bootstrap() {
  // tạo instance Express và bọc bằng ExpressAdapter cho NestJS
  const server = express();
  const app = await NestFactory.create<NestExpressApplication>(AppModule, new ExpressAdapter(server));
  app.setGlobalPrefix('api');
  app.useStaticAssets(process.cwd() + '/public');

  // Cấu hình Swagger (OpenAPI) bằng tiếng Việt
  const config = new DocumentBuilder()
    .setTitle('DataSync - API')
    .setDescription('Tài liệu API bằng tiếng Việt cho khung chuyển đổi dữ liệu SQL Server')
    .setVersion('1.0')
    .addTag('chuyendoi')
    .build();
  try {
    const document = SwaggerModule.createDocument(app, config);
    SwaggerModule.setup('api/docs', app, document, {
      swaggerOptions: { docExpansion: 'none' }
    });
  } catch (e) {
    // Nếu Swagger bị lỗi do metadata không hợp lệ, không để app crash — log và tiếp tục
    // Lỗi này thường do các tham số controller chưa có metadata (emitDecoratorMetadata)
    // hoặc một số decorator bị đặt sai chỗ.
    const err: any = e;
    console.error('Không thể tạo Swagger document:', err && err.stack ? err.stack : err);
  }

  await app.listen(3000);
  console.log('ứng dụng DataSync chạy tại http://localhost:3000');
  console.log('Tài liệu Swagger: http://localhost:3000/api/docs');
}

bootstrap();
