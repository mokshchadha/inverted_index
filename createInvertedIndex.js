const KEYSPACE = "amazon";
const cassandra = require("cassandra-driver");

async function main() {
  const client = await getClient();
  try {
    await createIndexTable(client, "reviews");
  } catch (error) {
    console.error(error);
  }
  //   client.close();
}

main();

/////////==========================

async function createIndexTable(client, tableName) {
  const indexTableName = KEYSPACE + `.${tableName}_inverted_index`;
  const query = `CREATE TABLE IF NOT EXISTS ${indexTableName}_1 (word text PRIMARY KEY, ids SET<text>);`;
  await client
    .execute(query)
    .then(() => console.log("first Index table created"));

  const query2 = `CREATE TABLE IF NOT EXISTS ${indexTableName}_2 (char_sequence text PRIMARY KEY, words SET<text>);`;
  await client
    .execute(query2)
    .then(() => console.log("second Index table created"));
}

async function getClient() {
  const client = new cassandra.Client({
    contactPoints: ["127.0.0.1:9042", "h2"],
    localDataCenter: "datacenter1",
    keyspace: KEYSPACE,
  });
  client.connect();
  return client;
}

async function insertDataIntoIndex(tableName) {
  // check if the unique ids which are there in the table not added to the index table
  // for the ids not added insert the words break up
}
