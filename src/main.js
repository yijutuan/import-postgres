const {Pool} = require('pg');
const {MongoClient} = require('mongodb');
const get = require('get-value');

const config = require('../config/config');

const DATABASE_NAME = 'testimport';

const ORIGINAL_COLUMNS = {
  'id': {
    type: 'text',
    constraints: ['PRIMARY KEY'],
  },
  'projectId': {
    type: 'text',
    constraints: ['NOT NULL'],
  },
};

const ADDED_COLUMNS = {
  'organizationId': {
    type: 'text',
    constraints: ['NOT NULL'],
  },
};

async function runImportPostgre() {
  const allColumns = {
    ...ORIGINAL_COLUMNS,
    ...ADDED_COLUMNS,
  };

  console.log('Connecting to Postgresql');
  const pool = new Pool({...config.postgres});
  console.log('Connected to Postgresql');

  await createTable(pool, allColumns);

  const mongodbURL = createMongoURL(
    config.mongodbURL,
    config.mongodbUser,
    config.mongodbPassword
  );

  console.log('Connecting to Mongodb');
  const client = await MongoClient.connect(mongodbURL, {
    ignoreUndefined: true,
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  console.log('Connected to Mongodb');

  try {
    const mongodb = client.db();
    const activitiesCollection = await mongodb.collection('activities');
    const cursor = await activitiesCollection.find({}).batchSize(10000);
    let record;

    console.time('insertPostgres');
    let number = 0;
    while((record = await cursor.next())) {
      console.log(record);
      await insert(pool, DATABASE_NAME, allColumns, record);
      number++;
      if (number >= 10) {
        break;
      }
    }
    console.timeEnd('insertPostgres');

  } catch (error) {
    console.error('Error when getting value from mongodb and updating to postgres', error);
    pool.end();
    return;
  }

  return;

  // const record = getValuesFromFile('test.json');
  // await insert(pool, DATABASE_NAME, record);
  pool.end();
}

/**
 * Create Table in Postgres
 * @param {Pool} pool
 * @param {object} allColumns
 */
async function createTable(pool, allColumns) {
  const columnStringArray = Object.keys(allColumns).map(colName => {
    const type = allColumns[colName].type;
    const constraints = allColumns[colName].constraints ?
      allColumns[colName].constraints.join(' ')
      : '';
    return `${colName} ${type} ${constraints}`;
  });

  const columnsString = createColumnsString(columnStringArray);
  const query = `CREATE TABLE ${DATABASE_NAME} ${columnsString};`;

  console.log(query);
  await runSqlQuery(pool, query);
}

/**
 * Gets value from a JSON file. This is used for testing purposes
 * @param {string} file - name of file in mongodb-docs
 */
function getValuesFromFile(file) {
  const record = require(`../mongodb-docs/${file}`);
  return record;
}

/**
 * Insert Values from record into Postgresql
 * @param {Pool} pool 
 * @param {string} databaseName
 * @param {allColumns} object
 * @param {Object} record 
 */
async function insert(pool, databaseName, allColumns, record) {
  const columnsList = Object.keys(allColumns);
  const columnsString = createColumnsString(columnsList);
  const valuesString = createValuesString(record, columnsList);

  const query = 
    `INSERT INTO ${databaseName} ${columnsString} `
    + `VALUES (${valuesString})`;
  console.log('query: ', query);
  await runSqlQuery(pool, query);
}

/**
 * Runs the Sql Query
 * @param {Pool} pool
 * @param {string} query
 */
async function runSqlQuery(pool, query) {
  try {
    const result = await pool.query(query);
    console.log(result);
  } catch (error) {
    console.log('Could not insert into postgres', error);
    throw error;
  }
}

/**
 * Create string representing columns.
 */
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
