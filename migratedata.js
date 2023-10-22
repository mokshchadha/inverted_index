const fs = require("fs");
const _ = require("lodash");
const crypto = require("crypto");
const readline = require("node:readline/promises");
const cassandra = require("cassandra-driver");

function insertIntoTable(doc, client) {
  const docInsertionQuery = getInsertionQuery(doc);
  const indexName = "reviews_inverted_index";
  const queries1 = getInsertionQueriesForReviewTextIdx(doc, indexName);
  const queries2 = getInsertionQueriesForWordSequenceIdxTable(doc, indexName);
  return client.batch([docInsertionQuery, ...queries1, ...queries2]);
}

async function main() {
  const fileStream = fs.createReadStream("data/arts.txt");
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  const client = new cassandra.Client({
    contactPoints: ["127.0.0.1:9042", "h2"],
    localDataCenter: "datacenter1",
    keyspace: "amazon",
  });
  client.connect();
  await truncateTables(client);

  let doc = {};
  let queries = [];

  rl.on("line", (line) => {
    if (line.trim().length === 0) {
      doc.id = crypto.randomUUID();
      insertIntoTable(doc, client);
      doc = {};
      return;
    }
    const [key, value] = line.trim().split(":");
    const keyName = key.split("/")[1];
    doc[keyName] = value.trim();
  });

  rl.on("close", async () => {
    console.log("Finished reading the file.");
    client.shutdown();
  });
}

main();

///

function getInsertionQuery(doc) {
  return {
    query:
      "INSERT INTO reviews (id , product_id, title ,price, user_id, profile_name, helpfulness, score, time, summary, review_text ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    params: [
      doc.id,
      doc.productId,
      doc.title,
      doc.price,
      doc.userId,
      doc.profileName,
      doc.helpfulness,
      doc.score,
      doc.time,
      doc.summary,
      doc.text,
    ],
  };
}

function getInsertionQueriesForReviewTextIdx(doc, indexName) {
  const words = sanitisedWordsArr(doc.text);
  const id = doc.id;
  const queries = words.map((word) => ({
    query: `UPDATE ${indexName}_1 SET ids = ids + {'${id}'} WHERE word = '${word}';`,
    params: [],
  }));
  return queries;
}

function getInsertionQueriesForWordSequenceIdxTable(doc, indexName) {
  const idxName = `${indexName}_2`;
  const words = sanitisedWordsArr(doc.text);
  const wordsLenGreaterThan3 = words.filter((e) => e.length >= 3);
  let set = new Set();
  wordsLenGreaterThan3.map((e) => {
    const substrings = findSubstrings(e);
    substrings.map((s) => set.add(s));
  });
  const uniquePrefixes = [...set];
  console.log({ uniquePrefixes });

  const prefixWithData = uniquePrefixes.map((e) => {
    const associatedWords = wordsLenGreaterThan3.filter((w) => w.startsWith(e));
    return {
      prefix: e,
      words: associatedWords,
    };
  });

  return prefixWithData.map(({ prefix, words }) => {
    return {
      query:
        `UPDATE ${idxName} SET words = words + ` +
        `{${words.map((e) => `'${e}'`).join(",")} }` +
        ` WHERE char_sequence = '${prefix}';`,
      params: [],
    };
  });
}

function sanitisedWordsArr(text) {
  return text
    .split(" ")
    .map((e) => e.trim().replace(".", "").replace(",", "").toLowerCase());
}

async function truncateTables(client) {
  client.batch([
    "truncate amazon.reviews;",
    "truncate amazon.reviews_inverted_index_1;",
    "truncate amazon.reviews_inverted_index_2;",
  ]);
}

function findSubstrings(word) {
  const substrings = [];
  const wordLen = word.length;

  for (let i = 3; i < wordLen; i++) {
    substrings.push(word.slice(0, i + 1));
  }

  return substrings;
}
