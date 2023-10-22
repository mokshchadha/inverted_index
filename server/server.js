const cassandra = require("cassandra-driver");
const path = require("path");
const fastify = require("fastify")({
  logger: true,
});
const fastifyStatic = require("@fastify/static");
const PORT = 3000;

let dbClient = getClient();

fastify.register(fastifyStatic, {
  root: path.join(__dirname),
  prefix: "/",
});

fastify.get("/", (req, reply) => {
  reply.sendFile("/ui/index.html");
});

fastify.get("/search", async (request, reply) => {
  const data = request.query;
  const results = await textSearchQuery(data.queryText);
  reply.send(results);
});

fastify.listen({ port: PORT }, (err, address) => {
  if (err) throw err;
  dbClient.shutdown();
});

async function textSearchQuery(queryText) {
  const words = queryText.split(" ").filter((e) => e.length >= 3);
  const idxPrefix = "reviews_inverted_index";
  //HIT the first index
  const query1 =
    `SELECT words from ${idxPrefix}_2 where char_sequence IN ` +
    ` (${words.map((w) => `'${w}'`).join(",")}) limit 5;`;

  const result = await dbClient.execute(query1);
  const relevantWords = result.rows.map((w) => w.words.slice(0, 5)).flat();

  //HIT the second index
  const query2 =
    `SELECT ids from ${idxPrefix}_1 where word IN ` +
    ` (${relevantWords.map((w) => `'${w}'`).join(",")}) limit 5;`;
  const result2 = await dbClient.execute(query2);

  const ids = result2.rows.map((e) => e.ids.slice(0, 10)).flat();

  //HIT the table
  const finalQuery = await dbClient.execute(
    `SELECT id, review_text from amazon.reviews where id in (${ids.join(",")})`
  );
  const data = finalQuery.rows;
  return data;
}

function getClient() {
  const client = new cassandra.Client({
    contactPoints: ["127.0.0.1:9042", "h2"],
    localDataCenter: "datacenter1",
    keyspace: "amazon",
  });
  client.connect();
  return client;
}
