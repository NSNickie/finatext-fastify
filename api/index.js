import Fastify from "fastify";
import crypto from "crypto";
import fs from "fs";
import csv from "csv-parser";
import path from "path";

const app = Fastify({
  logger: true,
});

// 使用 process.cwd() 替代 __dirname
const csvPath = path.join(__dirname, "order_books.csv");
const pad = (num) => String(num).padStart(2, "0");

// 添加时区转换函数
function convertToJST(date) {
  // 将时间转换为 JST (UTC+9)
  return new Date(date.getTime() + 9 * 60 * 60 * 1000);
}

// 由于 Vercel 是无状态的，我们每次都需要重新加载数据
async function loadCSV(candleData) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(csvPath)) {
      reject(new Error(`CSV file not found at ${csvPath}`));
      return;
    }

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
      return reply
        .code(400)
        .send({ error: "Username and password are required" });
    }

    const salt = crypto.randomBytes(16).toString("hex");
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
    console.log(req.body);
    app.log.info("Flag request received:" + req.body);
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

    const candleData = new Map();
    await new Promise((resolve, reject) => {
      fs.createReadStream(csvPath)
        .pipe(csv(["time", "code", "price"]))
        .on("data", (row) => {
          try {
            const formattedTime = row.time
              .replace(" JST", "")
              .replace(" +0900", "+09:00");
            const date = new Date(formattedTime);

            // 确保日期在 JST 时区
            const jstDate = convertToJST(date);

            const hourKey = `${row.code}_${jstDate.getFullYear()}-${pad(
              jstDate.getMonth() + 1
            )}-${pad(jstDate.getDate())}_${pad(jstDate.getHours())}`;
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
            app.log.error("Error processing row:", error);
            reject(error);
          }
        })
        .on("end", resolve)
        .on("error", reject);
    });

    // 构建查询 key 时也使用 JST 时区
    const queryDate = new Date(`${year}-${month}-${day}T${hour}:00:00`);
    // const jstQueryDate = convertToJST(queryDate);
    const key = `${code}_${queryDate.getFullYear()}-${pad(
      queryDate.getMonth() + 1
    )}-${pad(queryDate.getDate())}_${pad(queryDate.getHours())}`;
    console.log(key);
    // console.log(candleData);
    app.log.info(`Searching for key: ${key}`);
    const data = candleData.get(key);

    if (!data) {
      app.log.info(`No data found for key: ${key}`);
      return reply
        .code(404)
        .send({ error: "No data found for the specified parameters" });
    }

    app.log.info(`Found data for key: ${key}`);
    return data;
  } catch (error) {
    app.log.error("Error in /candle endpoint:", error);
    return reply.code(500).send({ error: "Internal server error" });
  }
});

export default async function handler(req, reply) {
  await app.ready();
  app.server.emit("request", req, reply);
}
