import "dotenv/config";
import { createClient } from "redis";
import { env } from "./utils/env.js";
import { BALANCES, ORDERBOOKS, ORDERS, type Balance, type OrderRecord, type OrderType, type RestingOrder, type Side } from "./store/exchange-store.js";
import { markdown, randomUUIDv7 } from "bun";
import { isPrivateIdentifier } from "typescript";

export type EngineCommandType =
  | "create_order"
  | "get_depth"
  | "get_user_balance"
  | "get_order"
  | "cancel_order";

export interface EngineRequest {
  correlationId: string;
  responseQueue: string;
  type: EngineCommandType;
  payload: Record<string, unknown>;
}

export interface EngineResponse {
  correlationId: string;
  ok: boolean;
  data?: unknown;
  error?: string;
}

const brokerClient = createClient({ url: env.redisUrl }).on("error", (error) => {
  console.error("Redis broker client error", error);
});

const responseClient = createClient({ url: env.redisUrl }).on("error", (error) => {
  console.error("Redis response client error", error);
});

await Promise.all([brokerClient.connect(), responseClient.connect()]);

// :-)) I added this just to check the flow, remove it when you start
const DUMMY_SELL_ORDER = {
  orderId: "dummy-sell-order-1",
  userId: "dummy-seller",
  type: "limit",
  side: "sell",
  symbol: "BTC",
  price: 100,
  qty: 1,
  filledQty: 0,
  status: "open",
};

async function sendResponse(responseQueue: string, response: EngineResponse): Promise<void> {
  await responseClient.lPush(responseQueue, JSON.stringify(response));
}

function handleEngineRequest(message: EngineRequest): unknown {
  /**
   * TODO(student):
   * 1. Check _message.type.
   * 2. Read _message.payload.
   * 3. Call your order book / balance / order logic.
   * 4. Return the data that should go back to the backend.
   *
   * Required message types:
   * - create_order
   * - get_depth
   * - get_user_balance
   * - get_order
   * - cancel_order
   */
  
  const newOrderId = randomUUIDv7();
  if (message.type === 'create_order') {
    ORDERS.set(newOrderId, {
      orderId: newOrderId,
      userId: message.payload.userId as string,
      side: message.payload.side as Side,
      type: message.payload.type as OrderType,
      symbol: message.payload.symbol as string,
      price: message.payload.price as number,
      qty: message.payload.qty as number,
      filledQty: 0,
      status: 'open',
      fills: [],
      createdAt: new Date().getTime()
    });
    const createdOrder = ORDERS.get(newOrderId);
    if (!createdOrder) throw new Error("creation of ordered failed in engine");
    if (!ORDERBOOKS.get(createdOrder.symbol)) {
      ORDERBOOKS.set(createdOrder.symbol, {
        bids: new Map,
        asks: new Map
      });
    }
    const curOrderBook = ORDERBOOKS.get(createdOrder.symbol)!;
    switch (createdOrder.side) {
      case 'buy': {
        const askSide = new Map([...curOrderBook.asks].sort((a, b) => a[0] - b[0]));
        for (let price of askSide) {
          if (createdOrder.type == 'limit' && price[0] > createdOrder.price!) continue;
          const sellers = price[1].filter(a => a.side == 'sell' && a.status !== 'cancelled' || a.status !== 'filled');
          for (let order of sellers) {
            if (!createdOrder.qty) break;
            const remain = Math.min((order.qty - order.filledQty), createdOrder.qty);
            createdOrder.qty -= remain;
            createdOrder.status = 'partially_filled';
            createdOrder.fills.push({
              fillId: randomUUIDv7(),
              symbol: order.symbol,
              price: order.price,
              qty: remain,
              buyOrderId: order.orderId,
              sellOrderId: createdOrder.orderId,
              createdAt: new Date().getTime(),
            });
          }
        }
        break;
      }
      case 'sell': {
        const bidSide = new Map([...curOrderBook.bids].sort((a, b) => b[0] - a[0]));
        for (let price of bidSide) {
          if (createdOrder.type == 'limit' && price[0] >= createdOrder.price!) continue;
          const buyers = price[1].filter(a => a.side == 'buy' && a.status !== 'cancelled' || a.status !== 'filled');
          for (let order of buyers) {
            if (!createdOrder.qty) break;
            const remain = Math.min(order.qty - order.filledQty, createdOrder.qty);
            createdOrder.qty -= remain;
            createdOrder.status = 'partially_filled';
            createdOrder.fills.push({
              fillId: randomUUIDv7(),
              symbol: order.symbol,
              price: order.price,
              qty: remain,
              buyOrderId: createdOrder.orderId,
              sellOrderId: order.orderId,
              createdAt: new Date().getTime(),
            });
          }
        }
      }
    }
    if (createdOrder.qty) {
      if (createdOrder.side == 'buy') {
        if (!curOrderBook.bids.get(createdOrder.price!)) curOrderBook.bids.set(createdOrder.price!, []);
        const prices = curOrderBook.bids.get(createdOrder.price!)!;
        prices.push({
          orderId: createdOrder.orderId,
          userId: createdOrder.userId,
          side: createdOrder.side,
          type: "limit",
          symbol: createdOrder.symbol,
          price: createdOrder.price!,
          qty: createdOrder.qty,
          filledQty: createdOrder.filledQty,
          status: createdOrder.status,
          createdAt: createdOrder.createdAt
        });
      }
      else {
        if (!curOrderBook.asks.get(createdOrder.price!)) curOrderBook.asks.set(createdOrder.price!, []);
        const prices = curOrderBook.asks.get(createdOrder.price!)!;
        prices.push({
          orderId: createdOrder.orderId,
          userId: createdOrder.userId,
          side: createdOrder.side,
          type: "limit",
          symbol: createdOrder.symbol,
          price: createdOrder.price!,
          qty: createdOrder.qty,
          filledQty: createdOrder.filledQty,
          status: createdOrder.status,
          createdAt: createdOrder.createdAt
        });
      }
    }
    else {
      createdOrder.status = 'filled';
    }
    return createdOrder;
  }
  
  throw new Error("else nothing is done");
}

console.log(`Engine listening on Redis queue: ${env.incomingQueue}`);

for (;;) {
  const item = await brokerClient.brPop(env.incomingQueue, 0);
  if (!item) continue;

  let message: EngineRequest;

  try {
    message = JSON.parse(item.element) as EngineRequest;
  } catch {
    console.error("Skipping invalid broker message");
    continue;
  }

  try {
    const data = handleEngineRequest(message);
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: true,
      data,
    });
  } catch (error) {
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: false,
      error: error instanceof Error ? error.message : "engine_error",
    });
  }
}