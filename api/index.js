import Fastify from "fastify";
import crypto from "crypto";
import fs from "fs";
import csv from "csv-parser";

const app = Fastify({
  logger: true,
});

const candleData = new Map();

function loadCSV() {
  return new Promise((resolve, reject) => {
    fs.createReadStream("./order_books.csv")
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

  if (candleData.has(key)) {
    return reply.send(candleData.get(key));
  } else {
    return reply.code(404).send({ error: "No data found" });
  }
});

export default async function handler(req, reply) {
  await app.ready();
  await loadCSV();
  app.server.emit("request", req, reply);
}
