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
  reply.send({ hello: "world" });
});

fastify.listen({ port: PORT }, (err, address) => {
  if (err) throw err;
  dbClient.shutdown();
});

async function textSearchQuery(queryText) {
  const words = queryText.split(" ").filter((e) => e.length >= 3);
  const tableName = "reviews";
  const idxPrefix = "reviews_inverted_index";
  const query1 =
    `SELECT words from ${idxPrefix}_2 where char_sequence IN ` +
    ` (${words.map((w) => `'${w}'`).join(",")}) limit 10;`;

  const result = await dbClient.execute(query1);
  console.log({ result });
  //search the first idx
  //hit the second idx
  //return results
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
