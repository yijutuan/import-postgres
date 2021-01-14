const {Pool} = require('pg');
const {MongoClient} = require('mongodb');
const get = require('get-value');

const config = require('../config/config');

const DATABASE_NAME = 'testimport';
const ORIGINAL_COLUMNS = ['id', 'projectId'];
const ADDED_COLUMNS = ['organizationId'];

async function runImportPostgre() {
  console.log('Connecting to Postgresql');
  const pool = new Pool({...config.postgres});
  console.log('Connected to Postgresql');

  const mongodbURL = createMongoURL(
    config.mongodbURL,
    config.mongodbUser,
    config.mongodbPassword
  );
  console.log('Connecting to Mongodb');

  try {
    const client = await MongoClient.connect(mongodbURL, {
      ignoreUndefined: true,
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log('Connected to Mongodb');

    const mongodb = client.db();
    const activitiesCollection = await mongodb.collection('activities');
    const cursor = await activitiesCollection.find({}).batchSize(10000);
    let record;

    while((record = await cursor.next())) {
      console.log(record);
      await insert(pool, DATABASE_NAME, record);
      break; // just to test one record at a time
    }

  } catch (error) {
    console.error('Error connecting to mongodb', error);
    return;
  }

  return;

  // const record = getValuesFromFile('test.json');
  // await insert(pool, DATABASE_NAME, record);
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
async function insert(pool, databaseName, record) {
  const columnsList = ORIGINAL_COLUMNS;

  addOnAnalysisColumns(columnsList);
  const valuesString = createValuesString(record, columnsList);
  const columnsString = createColumnsString(columnsList);

  const query = 
    `INSERT INTO ${databaseName} ${columnsString} `
    + `VALUES (${valuesString})`;
  console.log('query: ', query);
  try {
    const result = await pool.query(query);
    console.log(result);
  } catch (error) {
    console.log('Could not insert into postgres', error);
  }
  pool.end();
}

function addOnAnalysisColumns(columnsList) {
  const newColumns = ADDED_COLUMNS;
  columnsList.push(...newColumns);
}

function createColumnsString(columnsList) {
  return '(' + columnsList.join(', ') + ')';
}

/**
 * Create list of values and add organizationId value
 * @param {object} record 
 */
function createValuesString(record, columnsList) {
  const valuesList = columnsList.map(colName => {
    if (colName === 'organizationId') {
      return getOrganizationId(record);
    }

    return get(record, colName);
  });


  const valuesListHandleStrings = valuesList.map(value => {
    if (typeof value === 'string') {
      return `'${value}'`;
    }
    return value;
  });

  return valuesListHandleStrings.join(', ');
}

/**
 * For now, organizationId will just be the root project, but
 * we can change this to Salesforce value or another value later.
 * @param {object} record
 */
function getOrganizationId(record) {
  const projectId = get(record, 'projectId');
  return projectId.split('-')[0];
}

/**
 * Creates an URL to connect to a MongoDB.
 * @param {!string} url MongoDB URL to which the user and the password should be
   added, if any
 * @param {string?} user The username to be added to the connection URL
 * @param {string?} password The password to be added to the connection URL
 * @returns {string} Returns MongoDB URL with schema, username and password,
   if any
 */
function createMongoURL(url, user, password) {
  if (user || password) {
    url = url.replace(/^mongodb(?:\+srv)?:\/\//, (match) => {
      return `${match}${user}:${password}@`;
    });
  }

  return url;
}

module.exports = {
  runImportPostgre,
}
