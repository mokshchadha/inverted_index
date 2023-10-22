const fs = require("fs");
const _ = require("lodash");
const crypto = require("crypto");
const readline = require("node:readline/promises");
const cassandra = require("cassandra-driver");

async function insertIntoTable(doc, client) {
  const docInsertionQuery = getInsertionQuery(doc);
  const indexName = "reviews_inverted_index";
  const queries1 = getInsertionQueriesForReviewTextIdx(doc, indexName);
  const queries2 = getInsertionQueriesForWordSequenceIdxTable(doc, indexName);
  const queries = [docInsertionQuery, ...queries1, ...queries2];

  try {
    const chunks = _.chunk(queries, 400);
    for (c of chunks) {
      await client.batch(c);
    }
  } catch (error) {
    console.log({ queries });
    console.error(error);
  }
}

async function main() {
  const client = new cassandra.Client({
    contactPoints: ["127.0.0.1:9042", "h2"],
    localDataCenter: "datacenter1",
    keyspace: "amazon",
  });
  client.connect();

  await truncateTables(client);

  const fileStream = fs.createReadStream("data/arts.txt");
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let doc = {};
  let documents = [];

  rl.on("line", (line) => {
    if (line.trim().length === 0) {
      doc.id = crypto.randomUUID();
      documents.push(doc);
      doc = {};
      return;
    }
    const [key, value] = line.trim().split(":");
    const keyName = key.split("/")[1];
    doc[keyName] = value.trim();
  });

  rl.on("close", async () => {
    console.log("Finished reading the file.");
    for (let doc of documents) {
      await insertIntoTable(doc, client);
    }
    client.shutdown();
    console.log("client closed");
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
  const queries = words
    .map((word) => {
      const final = removePunctuation(word);
      if (final)
        return {
          query:
            `UPDATE ${indexName}_1 SET ids = ids + {'${id}'} WHERE word = ` +
            `'${removePunctuation(word)}';`,
          params: [],
        };
    })
    .filter((e) => e);
  return queries;
}

function getInsertionQueriesForWordSequenceIdxTable(doc, indexName) {
  const idxName = `${indexName}_2`;
  const words = sanitisedWordsArr(doc.text);

  const wordsLenGreaterThan3 = words
    .map(removePunctuation)
    .filter((e) => e.length >= 3);

  let set = new Set();
  wordsLenGreaterThan3.map((e) => {
    const substrings = findSubstrings(e);
    substrings.map((s) => set.add(s));
  });
  const uniquePrefixes = [...set];

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
  await client.execute("truncate amazon.reviews;");
  await client.execute("truncate amazon.reviews_inverted_index_1;");
  await client.execute("truncate amazon.reviews_inverted_index_2;");
  console.log("truncate data successfull");
}

function findSubstrings(word) {
  const substrings = [];
  const wordLen = word.length;

  for (let i = 3; i < wordLen; i++) {
    substrings.push(word.slice(0, i + 1));
  }

  return substrings;
}

function removePunctuation(word) {
  return word.replace(/[.,\/#!$%\^&\*;:{}=\-_`~()'"?<>|[\]\\]/g, "").trim();
}
