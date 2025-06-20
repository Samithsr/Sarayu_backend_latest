const express = require('express');
const router = express.Router();
const { MongoClient } = require('mongodb');
const winston = require('winston');
const { v4: uuidv4 } = require('uuid');
const stream = require('stream');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss'
    }),
    winston.format.errors({ stack: true }),
    winston.format.splat(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ 
      filename: 'logs/backup-error.log', 
      level: 'error',
      maxsize: 5242880,
      maxFiles: 5
    }),
    new winston.transports.File({ 
      filename: 'logs/backup.log',
      maxsize: 5242880,
      maxFiles: 5
    }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

const DB_URI = process.env.MONGODB_URI || 'mongodb://13.127.36.85:27017';
const DB_NAME = process.env.DB_NAME || 'sarayu-test-project-ec2';

router.get('/size', async (req, res) => {
  let client;
  try {
    client = new MongoClient(DB_URI, {
      connectTimeoutMS: 10000,
      socketTimeoutMS: 30000,
      serverSelectionTimeoutMS: 5000
    });
    
    await client.connect();
    const db = client.db(DB_NAME);
    const stats = await db.stats();
    const collections = await db.listCollections().toArray();
    
    const JSON_OVERHEAD_MULTIPLIER = 1.7; 
    const estimatedJsonSize = stats.dataSize * JSON_OVERHEAD_MULTIPLIER;

    res.json({ 
      dbName: DB_NAME,
      size: estimatedJsonSize, 
      storageSize: stats.storageSize,
      collections: collections.length,
      status: 'healthy'
    });
  } catch (error) {
    logger.error('Database size calculation error:', error);
    res.status(500).json({ 
      error: 'Failed to calculate database size',
      details: error.message
    });
  } finally {
    if (client) {
      await client.close().catch(err => {
        logger.error('Error closing MongoDB connection:', err);
      });
    }
  }
});

router.get('/', async (req, res) => {
  const backupId = uuidv4();
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  let client;
  
  logger.info(`Starting backup process (ID: ${backupId})`);

  try {
    client = new MongoClient(DB_URI, {
      connectTimeoutMS: 10000,
      socketTimeoutMS: 30000,
      serverSelectionTimeoutMS: 5000
    });
    
    await client.connect();
    const db = client.db(DB_NAME);
    const stats = await db.stats();
    const collections = await db.listCollections().toArray();

    const fileName = `backup_${DB_NAME}_${timestamp}.json`;

    const JSON_OVERHEAD_MULTIPLIER = 1.7;
    const estimatedJsonSize = stats.dataSize * JSON_OVERHEAD_MULTIPLIER;

    res.set({
      'Content-Type': 'application/json',
      'Content-Disposition': `attachment; filename="${fileName}"`,
      'X-Backup-ID': backupId,
      'X-Estimated-Size': estimatedJsonSize,
      'X-Collections-Count': collections.length
    });

    const passThrough = new stream.PassThrough();
    passThrough.pipe(res);

    passThrough.write('[');

    let isFirstEntry = true;
    for (const collectionInfo of collections) {
      try {
        const collection = db.collection(collectionInfo.name);
        const count = await collection.estimatedDocumentCount();
        logger.info(`Backing up collection ${collectionInfo.name} (${count} documents)`);
        
        const cursor = collection.find({}).batchSize(1000);
        
        for await (const doc of cursor) {
          if (!isFirstEntry) {
            passThrough.write(',');
          }
          isFirstEntry = false;

          const entry = {
            collection: collectionInfo.name,
            document: doc
          };
          passThrough.write(JSON.stringify(entry));
        }
      } catch (err) {
        logger.error(`Error backing up collection ${collectionInfo.name}:`, err);
        if (!isFirstEntry) {
          passThrough.write(',');
        }
        isFirstEntry = false;
        const errorEntry = {
          collection: collectionInfo.name,
          error: `Failed to backup collection ${collectionInfo.name}`,
          details: err.message
        };
        passThrough.write(JSON.stringify(errorEntry));
      }
    }

    passThrough.write(']');
    passThrough.end();

    logger.info(`Backup ${backupId} completed successfully`);

  } catch (error) {
    logger.error(`Backup ${backupId} failed:`, error);
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Failed to create backup',
        details: error.message,
        backupId: backupId
      });
    }
  } finally {
    if (client) {
      await client.close().catch(err => {
        logger.error('Error closing MongoDB connection:', err);
      });
    }
  }
});

module.exports = router;