const config = {
  mongodbUser: process.env.LCP_MONGO_USER || '',
  mongodbPassword: process.env.LCP_MONGO_PASSWORD || '',
  mongodbURL: process.env.LCP_MONGO_URL || 'mongodb+srv://localhost:32701/liferay-cloud-api',

  postgres: {
    user: process.env.LCP_POSTGRES_USER || '',
    host: process.env.LCP_POSTGRES_HOST || 'localhost',
    database: process.env.LCP_POSTGRES_DB || 'postgres',
    password: process.env.LCP_POSTGRES_PASSWORD || '',
    port: process.env.LCP_POSTGRES_PORT || '27017',
  }
}

module.exports = config;
