import { Body, Controller, Get, Post } from "@nestjs/common";

@Controller()
export class AppController {
  @Get("ping")
  ping() {
    return { ok: true, framework: "nestjs" };
  }

  @Post("orders")
  createOrder(@Body() body: { item?: string; qty?: number }) {
    return {
      id: "ord_1",
      item: body?.item ?? "sample-item",
      qty: Number(body?.qty ?? 1),
    };
  }
}
