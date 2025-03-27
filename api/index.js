import Fastify from "fastify";
import crypto from "crypto";
import fs from "fs";
import csv from "csv-parser";
import path from "path";

const app = Fastify({
  logger: true,
});

const csvPath = path.resolve(__dirname, "order_books.csv");
const pad = (num) => String(num).padStart(2, "0");

// 缓存 candleData
let candleDataCache = null;
let lastLoadTime = null;
const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

async function loadCSV(candleData) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(csvPath)
      .pipe(csv(["time", "code", "price"]))
      .on("data", (row) => {
        try {
          const formattedTime = row.time
            .replace(" JST", "")
            .replace(" +0900", "+09:00");
          const date = new Date(formattedTime);

          const hourKey = `${row.code}_${date.getFullYear()}-${pad(
            date.getMonth() + 1
          )}-${pad(date.getDate())}_${pad(date.getHours())}`;
          const price = parseInt(row.price, 10);
          
          if (!candleData.has(hourKey)) {
            candleData.set(hourKey, {
              open: price,
              high: price,
              low: price,
              close: price,
            });
          } else {
            const candle = candleData.get(hourKey);
            candle.high = Math.max(candle.high, price);
            candle.low = Math.min(candle.low, price);
            candle.close = price;
          }
        } catch (error) {
          reject(new Error(`Error processing row: ${error.message}`));
        }
      })
      .on("end", resolve)
      .on("error", reject);
  });
}

app.put("/login", async (req, reply) => {
  try {
    const { username, password } = req.body;
    
    if (!username || !password) {
      return reply.code(400).send({ error: "Username and password are required" });
    }

    // 使用更安全的密码哈希方法
    const salt = crypto.randomBytes(16).toString('hex');
    const token = crypto
      .createHash("sha256")
      .update(`${username}${password}${salt}`)
      .digest("hex");

    return { token };
  } catch (error) {
    app.log.error(error);
    return reply.code(500).send({ error: "Internal server error" });
  }
});

app.put("/flag", async function (req, reply) {
  try {
    app.log.info("Flag request received:", req.body);
    return {};
  } catch (error) {
    app.log.error(error);
    return reply.code(500).send({ error: "Internal server error" });
  }
});

app.get("/candle", async function (req, reply) {
  try {
    const { code, year, month, day, hour } = req.query;

    // 输入验证
    if (!code || !year || !month || !day || !hour) {
      return reply.code(400).send({ error: "Missing required parameters" });
    }

    // 验证日期参数
    const date = new Date(`${year}-${month}-${day}T${hour}:00:00`);
    if (isNaN(date.getTime())) {
      return reply.code(400).send({ error: "Invalid date parameters" });
    }

    // 检查缓存是否需要更新
    if (!candleDataCache || !lastLoadTime || Date.now() - lastLoadTime > CACHE_DURATION) {
      candleDataCache = new Map();
      await loadCSV(candleDataCache);
      lastLoadTime = Date.now();
      app.log.info("CSV data reloaded, total entries:", candleDataCache.size);
    }

    const key = `${code}_${year}-${pad(month)}-${pad(day)}_${pad(hour)}`;
    const data = candleDataCache.get(key);

    if (!data) {
      return reply.code(404).send({ error: "No data found for the specified parameters" });
    }

    return data;
  } catch (error) {
    app.log.error(error);
    return reply.code(500).send({ error: "Internal server error" });
  }
});

export default async function handler(req, reply) {
  await app.ready();
  app.server.emit("request", req, reply);
}
