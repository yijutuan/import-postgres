const {Pool} = require('pg');

const {MongoClient} = require('mongodb');

process.env.DATABASE_URL = 'host=localhost port=30678 dbname=postgres user=postgres password=postgres';

async function runImportPostgre() {
  // try {
  //   const client = await MongoClient.connect('mongodb://localhost:32701/liferay-cloud-api', {
  //     ignoreUndefined: true,
  //     useNewUrlParser: true,
  //     useUnifiedTopology: true,
  //   });

  //   // having issues connecting...

  //   const mongodb = client.db();
  //   const activitiesCollection = await mongodb.collection('activities');
  //   const cursor = await activitiesCollection.find({}).batchSize(10000);
  //   let record;
  //   console.log(await mongodb.collection('activities').count());

  //   // while((record = await cursor.next())) {
  //   //   console.log(record);
  //   //   break;
  //   // }

  // } catch (error) {
  //   console.error('Error connecting to mongodb', error);
  // }

  const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'postgres',
    port: '30678',
  });

  const record = getValuesFromFile('test.json');

  await insert(pool, record);
}

function getValuesFromFile(file) {
  const record = require(`../mongodb-docs/${file}`);
  return record;
}

/**
 * Insert Values from record into Postgresql
 * @param {Pool} pool 
 * @param {Object} record 
 */
async function insert(pool, record) {
  const databaseName = 'testimport';
  const columnsName = '(id, projectId, organizationId)';
  const valuesList = createValuesList(record);

  const query = 
    `INSERT INTO ${databaseName} ${columnsName} `
    + `VALUES (${valuesList})`;
  try {
    const result = await pool.query(query);
    console.log(result);
  } catch (error) {
    console.log(error);
  }
  pool.end();
}

/**
 * Create list of values and add organizationId value
 * @param {object} record 
 */
function createValuesList(record) {
  const {id, projectId} = record;
  const organizationId = getOrganizationId(projectId);
  // don't forget to add quotes for string values
  return `${id}, '${projectId}', '${organizationId}'`;
}

/**
 * For now, organizationId will just be the root project, but
 * we can change this to Salesforce value or another value later.
 * @param {string} projectId 
 */
function getOrganizationId(projectId) {
  return projectId.split('-')[0];
}

module.exports = {
  runImportPostgre,
}
