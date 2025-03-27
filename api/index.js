import Fastify from "fastify";
import crypto from "crypto";
import fs from "fs";
import csv from "csv-parser";
import path from "path";

const app = Fastify({
  logger: true,
});

const candleData = new Map();
const csvPath = path.resolve(__dirname, "order_books.csv");
console.log(csvPath);
function loadCSV() {
  return new Promise((resolve, reject) => {
    fs.createReadStream(csvPath)
      .pipe(csv(["time", "code", "price"]))
      .on("data", (row) => {
        const date = new Date(row.time);
        const hourKey = `${row.code}_${date.getFullYear()}-${
          date.getMonth() + 1
        }-${date.getDate()}_${date.getHours()}`;
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
      })
      .on("end", resolve)
      .on("error", reject);
  });
}

app.put("/login", async (req, reply) => {
  const { username, password } = req.body;
  console.log(`username:${username}`);
  console.log(`password:${password}`);

  const token = crypto
    .createHash("sha1")
    .update(`${username}${password}`)
    .digest("hex");
  console.log(token);

  return { token };
});

app.put("/flag", function (req, reply) {
  console.log(req.body);
  return {};
});

app.get("/candle", function (req, reply) {
  const { code, year, month, day, hour } = req.query;
  const key = `${code}_${year}-${month}-${day}_${hour}`;
  console.log(key);
  if (candleData.has(key)) {
    console.log("return:\n" + candleData.get(key));
    return reply.send(candleData.get(key));
  } else {
    console.log("return:\n" + candleData.get(key));
    return reply.code(404).send({ error: "No data found" });
  }
});

export default async function handler(req, reply) {
  await app.ready();
  await loadCSV();
  console.log(candleData);
  app.server.emit("request", req, reply);
}
