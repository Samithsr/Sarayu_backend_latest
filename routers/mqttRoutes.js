const express = require("express");
const {
  getLatestLiveMessage,
  subscribeToTopic,
  isTopicSubscribed,
  unsubscribeFromTopic,
  updateThresholds,
} = require("../middlewares/mqttHandler");
const MessagesModel = require("../models/messages-model");
const AllTopicsModel = require("../models/all-mqtt-messages");
const TopicsModel = require("../models/topics-model");
const moment = require("moment-timezone");
const SubscribedTopic = require("../models/subscribed-topic-model");
const { stringify } = require("csv-stringify");
const redis = require("redis");
const Gateway = require("../models/gateway-model");

const redisClient = redis.createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379',
  socket: {
    reconnectStrategy: retries => Math.min(retries * 50, 1000), 
  }
});

let redisConnected = false;

// Connect to Redis with status tracking
redisClient.connect()
  .then(() => {
    redisConnected = true;
    console.log('Connected to Redis');
  })
  .catch(err => console.error('Redis connection failed:', err));

redisClient.on('error', err => {
  redisConnected = false;
  console.error('Redis Client Error:', err);
});

redisClient.on('reconnecting', () => console.log('Reconnecting to Redis...'));
redisClient.on('ready', () => {
  redisConnected = true;
  console.log('Redis Client Ready');
});

// Redis key prefix and TTLs
const CACHE_PREFIX = "mqtt:";
const TTL_SHORT = 300;    // 5 minutes
const TTL_MEDIUM = 1800;  // 30 minutes
const TTL_LONG = 3600;    // 1 hour

const router = express.Router();

// Helper function to safely interact with Redis
const safeRedisGet = async (key) => {
  if (!redisConnected) return null;
  try {
    return await redisClient.get(key);
  } catch (err) {
    console.error(`Redis get error for key ${key}:`, err);
    return null;
  }
};

const safeRedisSet = async (key, value, ttl) => {
  if (!redisConnected) return;
  try {
    await redisClient.setEx(key, ttl, JSON.stringify(value));
  } catch (err) {
    console.error(`Redis set error for key ${key}:`, err);
  }
};

const safeRedisDel = async (key) => {
  if (!redisConnected) return;
  try {
    await redisClient.del(key);
  } catch (err) {
    console.error(`Redis del error for key ${key}:`, err);
  }
};

const fetchMessages = async (topic, fileIds = [], filter = {}) => {
  const cacheKey = `${CACHE_PREFIX}messages:${topic}:${JSON.stringify(filter)}`;
  const cachedData = await safeRedisGet(cacheKey);

  if (cachedData) {
    return JSON.parse(cachedData);
  }

  const query = { topic };
  if (filter.from) query.timestamp = { $gte: filter.from };
  if (filter.to) query.timestamp = { ...query.timestamp, $lte: filter.to };
  
  const messages = await MessagesModel.find(query)
    .sort({ timestamp: -1 })
    .lean();
  
  const result = messages.map(msg => ({
    timestamp: msg.timestamp.toISOString(),
    message: msg.message
  }));

  await safeRedisSet(cacheKey, result, TTL_SHORT);
  return result;
};

router.get('/all-topics-labels', async (req, res) => {
  try {
    const topics = await TopicsModel.find({}).lean();
    const response = { success: true, data: topics };
    
    res.status(200).json(response);
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post('/get-single-topic-label', async (req, res) => {
  try {
    const { topic } = req.body;
    const cacheKey = `${CACHE_PREFIX}topic-label:${topic}`;
    const cachedData = await safeRedisGet(cacheKey);

    if (cachedData) {
      return res.status(200).json(JSON.parse(cachedData));
    }

    const label = await TopicsModel.find({ topic }, { label: 1, _id: 0 }).lean();
    const response = { success: true, data: label };
    
    await safeRedisSet(cacheKey, response, TTL_LONG);
    res.status(200).json(response);
  } catch (error) {
    res.status(400).json({ success: false, message: error.message });
  }
});

router.put('/topic-label-update/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { updatedLabel } = req.body;
    const topic = await TopicsModel.findById(id);
    topic.label = updatedLabel;
    await topic.save();

    await Promise.all([
      safeRedisDel(`${CACHE_PREFIX}all-topics-labels`),
      safeRedisDel(`${CACHE_PREFIX}topic-label:${topic.topic}`)
    ]);

    res.status(200).json({ success: true, data: [] });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post("/subscribe", (req, res) => {
  const { topic } = req.body;
  if (!topic) {
    return res.status(400).json({ success: false, message: "Topic is required" });
  }

  subscribeToTopic(topic);
  res.json({ success: true, message: `Subscribed to topic: ${topic}` });
});

router.post("/create-tagname", async (req, res) => {
  try {
    const { topic, label, device } = req.body;

    const existingTopic = await TopicsModel.findOne({ topic }).lean();
    if (existingTopic) {
      return res.status(400).json({
        success: false,
        message: "TagName already exists!",
      });
    }

    await TopicsModel.create({ topic, label, device });
    
    res.status(201).json({ success: true, data: [] });
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

router.get("/get-all-subscribedtopics", async (req, res) => {
  try {
    const cacheKey = `${CACHE_PREFIX}subscribed-topics`;
    const cachedData = await safeRedisGet(cacheKey);

    if (cachedData) {
      return res.status(200).json(JSON.parse(cachedData));
    }

    const subscribedTopicList = await SubscribedTopic.find({}, { _id: 0, topic: 1 }).lean();
    const topics = subscribedTopicList.map(item => item.topic);
    const response = { success: true, data: topics };
    
    await safeRedisSet(cacheKey, response, TTL_MEDIUM);
    res.status(200).json(response);
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

router.get("/get-all-tagname", async (req, res) => {
  try {
    const topics = await TopicsModel.find({}, { topic: 1 }).select("topic -_id").lean();
    const response = { success: true, data: topics };
    res.status(200).json(response);
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

// router.get("/get-all-tagname", async (req, res) => {
//   try {
//     const topics = await TopicsModel.find({},{topic:1}).select("topic -_id").lean();
//     const topicsWithStatus = await Promise.all(
//       topics.map(async (t) => {
//         const messageCount = await MessagesModel.countDocuments({ topic: t.topic });
//         return { topic: t.topic, isEmpty: messageCount === 0 };
//       })
//     );

//     const response = { success: true, data: topicsWithStatus };
//     res.status(200).json(response);
//   } catch (error) {
//     res.status(500).json({ success: false, message: "Internal server error" });
//   }
// });

router.get("/get-recent-5-tagname", async (req, res) => {
  try {
    const cacheKey = `${CACHE_PREFIX}recent-5-tagname`;
    const cachedData = await safeRedisGet(cacheKey);

    if (cachedData) {
      return res.status(200).json(JSON.parse(cachedData));
    }

    const topicsWithMessages = await MessagesModel.distinct("topic").lean();
    const topics = await TopicsModel.find({ topic: { $nin: topicsWithMessages } })
      .select("topic -_id")
      .sort({ createdAt: -1 })
      .limit(5)
      .lean();

    const response = { success: true, data: topics };
    await safeRedisSet(cacheKey, response, TTL_LONG);
    res.status(200).json(response);
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

router.delete("/delete-topic/:topic", async (req, res) => {
  try {
    const { topic } = req.params;
    const topicDoc = await TopicsModel.findOne({ topic }).lean();
    if (!topicDoc) {
      return res.status(404).json({ success: false, message: "No topic found" });
    }
    await TopicsModel.deleteOne({ topic });
    await MessagesModel.deleteMany({ topic });

    await Promise.all([
      safeRedisDel(`${CACHE_PREFIX}all-topics-labels`),
      safeRedisDel(`${CACHE_PREFIX}get-all-tagname`),
      safeRedisDel(`${CACHE_PREFIX}recent-5-tagname`),
      safeRedisDel(`${CACHE_PREFIX}topic-label:${topic}`),
      safeRedisDel(`${CACHE_PREFIX}topic-exists:${topic}`),
      safeRedisDel(`${CACHE_PREFIX}msg-count:${topic}`)
    ]);

    res.status(200).json({ success: true, message: "Topic deleted" });
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

router.post("/subscribe-to-all", async (req, res) => {
  try {
    const cacheKey = `${CACHE_PREFIX}all-topics`;
    let topics = await safeRedisGet(cacheKey);

    if (!topics) {
      topics = await TopicsModel.find().select("topic -_id").lean();
      await safeRedisSet(cacheKey, topics, TTL_LONG);
    } else {
      topics = JSON.parse(topics);
    }

    if (!topics.length) {
      return res.status(404).json({
        success: false,
        message: "No topics found to subscribe to.",
      });
    }

    topics.forEach((t) => subscribeToTopic(t.topic));
    await safeRedisDel(`${CACHE_PREFIX}subscribed-topics`);

    res.status(200).json({
      success: true,
      message: "Subscribed to all topics successfully.",
      data: topics.map((t) => t.topic),
    });
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

router.post("/unsubscribe-from-all", async (req, res) => {
  try {
    const cacheKey = `${CACHE_PREFIX}all-topics`;
    let topics = await safeRedisGet(cacheKey);

    if (!topics) {
      topics = await TopicsModel.find().select("topic -_id").lean();
      await safeRedisSet(cacheKey, topics, TTL_LONG);
    } else {
      topics = JSON.parse(topics);
    }

    if (!topics.length) {
      return res.status(404).json({
        success: false,
        message: "No topics found to unsubscribe from.",
      });
    }

    const unsubscribedTopics = [];
    topics.forEach((t) => {
      if (isTopicSubscribed(t.topic)) {
        unsubscribeFromTopic(t.topic);
        unsubscribedTopics.push(t.topic);
      }
    });

    if (!unsubscribedTopics.length) {
      return res.status(400).json({
        success: false,
        message: "No topics were subscribed.",
      });
    }

    await safeRedisDel(`${CACHE_PREFIX}subscribed-topics`);

    res.status(200).json({
      success: true,
      message: "Unsubscribed from all subscribed topics successfully.",
      data: unsubscribedTopics,
    });
  } catch (error) {
    res.status(500).json({ success: false, message: "Internal server error" });
  }
});

router.post("/messages", (req, res) => {
  const { topic } = req.body;
  if (!topic) {
    return res.status(400).json({ success: false, message: "Topic is required" });
  }
  const latestMessage = getLatestLiveMessage(topic);
  if (!latestMessage) {
    return res.status(404).json({ success: false, message: "No live message available" });
  }
  res.json({ success: true, message: latestMessage });
});

router.post("/realtime-data/last-2-hours", async (req, res) => {
  const { topic } = req.body;
  if (!topic) {
    return res.status(400).json({ error: "Topic is required" });
  }

  const cacheKey = `${CACHE_PREFIX}realtime-2h:${topic}`;
  const cachedData = await safeRedisGet(cacheKey);

  if (cachedData) {
    return res.json(JSON.parse(cachedData));
  }

  try {
    const twoHoursAgo = moment().tz("Asia/Kolkata").subtract(2, "hours").toDate();
    const messages = await MessagesModel.find({
      topic,
      timestamp: { $gte: twoHoursAgo },
    }).sort({ timestamp: -1 }).lean();

    const response = { topic, messages };
    await safeRedisSet(cacheKey, response, TTL_SHORT);
    res.json(response);
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).send("Internal Server Error");
  }
});

router.post("/report-filter", async (req, res) => {
  const { topics, from, to, filterType, minValue, maxValue, page = 1, limit = 1000, aggregationMethod, startTimeOfDay, endTimeOfDay } = req.body;

  const cacheKey = `${CACHE_PREFIX}report:${JSON.stringify({ topics, from, to, filterType, minValue, maxValue, page, limit, aggregationMethod, startTimeOfDay, endTimeOfDay })}`;
  const cachedData = await safeRedisGet(cacheKey);

  if (cachedData) {
    return res.status(200).json(JSON.parse(cachedData));
  }

  if (!Array.isArray(topics) || topics.length === 0 || !from || !to) {
    return res.status(400).json({
      error: "Topics array, from date, and to date are required.",
    });
  }

  const MAX_TOPICS = 5;
  if (topics.length > MAX_TOPICS) {
    return res.status(400).json({
      error: `Too many topics. Maximum allowed is ${MAX_TOPICS}.`,
    });
  }

  try {
    const fromDate = moment(from).tz("Asia/Kolkata").toDate();
    const toDate = moment(to).tz("Asia/Kolkata").toDate();

    const dateRangeDays = moment(toDate).diff(moment(fromDate), "days");
    const MAX_DAYS = 365;
    if (dateRangeDays > MAX_DAYS) {
      return res.status(400).json({
        error: `Date range too large. Maximum allowed is ${MAX_DAYS} days.`,
      });
    }

    const pageNum = parseInt(page, 10);
    const limitNum = parseInt(limit, 10) || 1000; // Default to 1000 if not provided
    const skip = (pageNum - 1) * limitNum;

    const allMessages = [];
    let totalMessages = 0;

    for (const topic of topics) {
      const messages = await fetchMessages(topic, [], { from: fromDate, to: toDate });
      allMessages.push({ topic, messages });
      totalMessages += messages.length;
    }

    let report = [];
    let totalRecords = 0;

    // Apply time-of-day filtering if provided
    let filteredMessages = allMessages;
    if (startTimeOfDay || endTimeOfDay) {
      filteredMessages = allMessages.map(({ topic, messages }) => {
        return {
          topic,
          messages: messages.filter((msg) => {
            const timestamp = moment.tz(msg.timestamp, "Asia/Kolkata");
            const hour = timestamp.hour();
            const minute = timestamp.minute();
            const currentMinutes = hour * 60 + minute;

            let startMinutes, endMinutes;
            if (startTimeOfDay) {
              const start = moment(startTimeOfDay);
              startMinutes = start.hour() * 60 + start.minute();
            }
            if (endTimeOfDay) {
              const end = moment(endTimeOfDay);
              endMinutes = end.hour() * 60 + end.minute();
            }

            if (startMinutes !== undefined && endMinutes !== undefined) {
              return startMinutes <= endMinutes
                ? currentMinutes >= startMinutes && currentMinutes <= endMinutes
                : currentMinutes >= startMinutes || currentMinutes <= endMinutes;
            } else if (startMinutes !== undefined) {
              return currentMinutes >= startMinutes;
            } else if (endMinutes !== undefined) {
              return currentMinutes <= endMinutes;
            }
            return true;
          }),
        };
      });
    }

    if (filterType === "minPerDay" || filterType === "maxPerDay") {
      const dailyData = {};
      filteredMessages.forEach(({ topic, messages }) => {
        messages.forEach((msg) => {
          const day = moment(msg.timestamp).tz("Asia/Kolkata").format("YYYY-MM-DD");
          if (!dailyData[day]) dailyData[day] = {};
          if (!dailyData[day][topic]) dailyData[day][topic] = [];
          dailyData[day][topic].push(Number(msg.message));
        });
      });

      report = Object.entries(dailyData).map(([day, topicsData]) => {
        const row = { timestamp: moment(day).tz("Asia/Kolkata").toISOString() };
        topics.forEach((topic) => {
          const values = topicsData[topic] || [];
          if (values.length > 0) {
            switch (aggregationMethod) {
              case "average":
                row[topic] = (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2);
                break;
              case "sum":
                row[topic] = values.reduce((a, b) => a + b, 0);
                break;
              case "min":
                row[topic] = Math.min(...values);
                break;
              case "max":
                row[topic] = Math.max(...values);
                break;
              default:
                row[topic] = filterType === "minPerDay" ? Math.min(...values) : Math.max(...values);
            }
          } else {
            row[topic] = "N/A";
          }
        });
        return row;
      });

      totalRecords = report.length;
      report.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)); // Default descending
      report = report.slice(skip, skip + limitNum);
    } else {
      const timestampMap = new Map();
      filteredMessages.forEach(({ topic, messages }) => {
        messages.forEach((msg) => {
          const timestamp = moment(msg.timestamp).tz("Asia/Kolkata").startOf("second").toISOString();
          const value = Number(msg.message);

          if (
            filterType === "custom" &&
            ((minValue !== undefined && value < minValue) ||
             (maxValue !== undefined && value > maxValue))
          ) {
            return;
          }

          if (!timestampMap.has(timestamp)) {
            const row = { timestamp };
            topics.forEach((t) => (row[t] = "N/A"));
            timestampMap.set(timestamp, row);
          }

          timestampMap.get(timestamp)[topic] = value;
        });
      });

      report = Array.from(timestampMap.values());
      totalRecords = report.length;
      report.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)); // Default descending
      report = report.slice(skip, skip + limitNum);
    }

    if (totalRecords === 0) {
      return res.status(404).json({ error: "No data found for the given criteria." });
    }

    const response = {
      report,
      totalRecords,
      page: pageNum,
      limit: limitNum,
      totalPages: Math.ceil(totalRecords / limitNum),
    };

    await safeRedisSet(cacheKey, response, TTL_MEDIUM);
    res.status(200).json(response);
  } catch (error) {
    console.error("Error fetching report data:", error.message);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
});

router.post("/report-filter-csv", async (req, res) => {
  const { topics, from, to, filterType, minValue, maxValue, aggregationMethod, startTimeOfDay, endTimeOfDay } = req.body;

  if (!Array.isArray(topics) || topics.length === 0 || !from || !to) {
    return res.status(400).json({
      error: "Topics array, from date, and to date are required.",
    });
  }

  const MAX_TOPICS = 5;
  if (topics.length > MAX_TOPICS) {
    return res.status(400).json({
      error: `Too many topics. Maximum allowed is ${MAX_TOPICS}.`,
    });
  }

  try {
    const fromDate = moment(from).tz("Asia/Kolkata").toDate();
    const toDate = moment(to).tz("Asia/Kolkata").toDate();

    const dateRangeDays = moment(toDate).diff(moment(fromDate), "days");
    const MAX_DAYS = 365;
    if (dateRangeDays > MAX_DAYS) {
      return res.status(400).json({
        error: `Date range too large. Maximum allowed is ${MAX_DAYS} days.`,
      });
    }

    const currentTime = moment().format("YYYY-MM-DD_HH-mm-ss");
    res.setHeader("Content-Type", "text/csv");
    res.setHeader("Content-Disposition", `attachment; filename=report_${currentTime}.csv`);

    const allMessages = [];
    for (const topic of topics) {
      const messages = await fetchMessages(topic, [], { from: fromDate, to: toDate });
      allMessages.push({ topic, messages });
    }

    let reportData = [];
    // Apply time-of-day filtering if provided
    let filteredMessages = allMessages;
    if (startTimeOfDay || endTimeOfDay) {
      filteredMessages = allMessages.map(({ topic, messages }) => {
        return {
          topic,
          messages: messages.filter((msg) => {
            const timestamp = moment.tz(msg.timestamp, "Asia/Kolkata");
            const hour = timestamp.hour();
            const minute = timestamp.minute();
            const currentMinutes = hour * 60 + minute;

            let startMinutes, endMinutes;
            if (startTimeOfDay) {
              const start = moment(startTimeOfDay);
              startMinutes = start.hour() * 60 + start.minute();
            }
            if (endTimeOfDay) {
              const end = moment(endTimeOfDay);
              endMinutes = end.hour() * 60 + end.minute();
            }

            if (startMinutes !== undefined && endMinutes !== undefined) {
              return startMinutes <= endMinutes
                ? currentMinutes >= startMinutes && currentMinutes <= endMinutes
                : currentMinutes >= startMinutes || currentMinutes <= endMinutes;
            } else if (startMinutes !== undefined) {
              return currentMinutes >= startMinutes;
            } else if (endMinutes !== undefined) {
              return currentMinutes <= endMinutes;
            }
            return true;
          }),
        };
      });
    }

    if (filterType === "minPerDay" || filterType === "maxPerDay") {
      const dailyData = {};
      filteredMessages.forEach(({ topic, messages }) => {
        messages.forEach((msg) => {
          const day = moment(msg.timestamp).tz("Asia/Kolkata").format("YYYY-MM-DD");
          if (!dailyData[day]) dailyData[day] = {};
          if (!dailyData[day][topic]) dailyData[day][topic] = [];
          dailyData[day][topic].push(Number(msg.message));
        });
      });

      reportData = Object.entries(dailyData).map(([day, topicsData]) => {
        const row = { timestamp: moment(day).tz("Asia/Kolkata").toISOString() };
        topics.forEach((topic) => {
          const values = topicsData[topic] || [];
          if (values.length > 0) {
            switch (aggregationMethod) {
              case "average":
                row[topic] = (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2);
                break;
              case "sum":
                row[topic] = values.reduce((a, b) => a + b, 0);
                break;
              case "min":
                row[topic] = Math.min(...values);
                break;
              case "max":
                row[topic] = Math.max(...values);
                break;
              default:
                row[topic] = filterType === "minPerDay" ? Math.min(...values) : Math.max(...values);
            }
          } else {
            row[topic] = "N/A";
          }
        });
        return row;
      });
    } else {
      const timestampMap = new Map();
      filteredMessages.forEach(({ topic, messages }) => {
        messages.forEach((msg) => {
          const timestamp = moment(msg.timestamp).tz("Asia/Kolkata").toISOString();
          const value = Number(msg.message);

          if (
            filterType === "custom" &&
            ((minValue !== undefined && value < minValue) ||
             (maxValue !== undefined && value > maxValue))
          ) {
            return;
          }

          const normalizedTimestamp = moment(msg.timestamp).tz("Asia/Kolkata").startOf('second').toISOString();

          if (!timestampMap.has(normalizedTimestamp)) {
            const row = { timestamp: normalizedTimestamp };
            topics.forEach((t) => (row[t] = "N/A"));
            timestampMap.set(normalizedTimestamp, row);
          }

          timestampMap.get(normalizedTimestamp)[topic] = value;
        });
      });

      reportData = Array.from(timestampMap.values());
    }

    reportData.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)); // Default descending

    if (reportData.length === 0) {
      return res.status(404).json({ error: "No data found for the given criteria." });
    }

    const topicLabels = await TopicsModel.find({ topic: { $in: topics } }, { topic: 1, label: 1, _id: 0 }).lean();
    const headers = topics.map((topic) => {
      const matchedTopic = topicLabels.find((t) => t.topic === topic);
      const label = matchedTopic ? matchedTopic.label : topic.split("|")[0].split("/")[2] || topic;
      const unit = topic.split("|")[1] || "";
      return `${label}(${unit})`;
    });

    const csvStream = stringify({ header: true, columns: ["Row #", "Timestamp", ...headers] });
    csvStream.pipe(res);

    reportData.forEach((row, index) => {
      const csvRow = [
        index + 1,
        new Date(row.timestamp).toLocaleString(),
        ...topics.map((topic) => row[topic] || "N/A"),
      ];
      csvStream.write(csvRow);
    });

    csvStream.end();
  } catch (error) {
    console.error("Error generating CSV:", error.message);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
});

router.post("/graph-min-max-avg", async (req, res) => {
  const { topics, from, to } = req.body;
  const cacheKey = `${CACHE_PREFIX}graph:${JSON.stringify({ topics, from, to })}`;
  const cachedData = await safeRedisGet(cacheKey);

  if (cachedData) {
    return res.status(200).json(JSON.parse(cachedData));
  }

  if (!Array.isArray(topics) || topics.length === 0 || !from || !to) {
    return res.status(400).json({
      error: "Topics array, from date, and to date are required.",
    });
  }

  try {
    const fromDate = moment(from).tz("Asia/Kolkata").toDate();
    const toDate = moment(to).tz("Asia/Kolkata").toDate();
    const results = await Promise.all(
      topics.map(async (topic) => {
        const messages = await fetchMessages(topic, [], { from: fromDate, to: toDate });
        return { topic, messages };
      })
    );

    const stats = {};
    results.forEach(({ topic, messages }) => {
      if (!messages.length) {
        stats[topic] = { max: null, min: null, avg: "N/A" };
        return;
      }
      const values = messages.map(msg => Number(msg.message));
      const maxValue = Math.max(...values);
      const minValue = Math.min(...values);
      const avgValue = (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2);
      stats[topic] = {
        max: {
          value: maxValue,
          time: messages.find(msg => Number(msg.message) === maxValue)?.timestamp
        },
        min: {
          value: minValue,
          time: messages.find(msg => Number(msg.message) === minValue)?.timestamp
        },
        avg: avgValue
      };
    });

    if (Object.keys(stats).length === 0) {
      return res.status(404).json({ error: "No data found for the given criteria." });
    }

    await safeRedisSet(cacheKey, stats, TTL_MEDIUM);
    res.status(200).json(stats);
  } catch (error) {
    console.error("Error fetching graph stats:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.post("/realtime-data/custom-range", async (req, res) => {
  const { topic, from, to, granularity, minValue, maxValue, aggregationMethod, sortOrder, limit, startTimeOfDay, endTimeOfDay } = req.body;
  
  const cacheKey = `${CACHE_PREFIX}custom-range:${JSON.stringify(req.body)}`;
  const cachedData = await safeRedisGet(cacheKey);

  if (cachedData) {
    return res.json(JSON.parse(cachedData));
  }

  if (!topic || !from || !to || !granularity) {
    return res.status(400).json({ error: "Topic, from, to, and granularity are required" });
  }

  if (minValue !== undefined && (isNaN(minValue) || minValue < 0)) {
    return res.status(400).json({ error: "minValue must be a valid non-negative number" });
  }
  if (maxValue !== undefined && (isNaN(maxValue) || maxValue < 0)) {
    return res.status(400).json({ error: "maxValue must be a valid non-negative number" });
  }
  if (minValue !== undefined && maxValue !== undefined && parseFloat(minValue) > parseFloat(maxValue)) {
    return res.status(400).json({ error: "minValue cannot be greater than maxValue" });
  }
  if (aggregationMethod && !["average", "sum", "min", "max"].includes(aggregationMethod)) {
    return res.status(400).json({ error: "aggregationMethod must be one of: average, sum, min, max" });
  }
  if (sortOrder && !["asc", "desc"].includes(sortOrder)) {
    return res.status(400).json({ error: "sortOrder must be either 'asc' or 'desc'" });
  }
  if (limit !== undefined && (isNaN(limit) || limit <= 0)) {
    return res.status(400).json({ error: "limit must be a positive number" });
  }
  if (startTimeOfDay && !moment(startTimeOfDay, moment.ISO_8601, true).isValid()) {
    return res.status(400).json({ error: "startTimeOfDay must be a valid ISO 8601 date string" });
  }
  if (endTimeOfDay && !moment(endTimeOfDay, moment.ISO_8601, true).isValid()) {
    return res.status(400).json({ error: "endTimeOfDay must be a valid ISO 8601 date string" });
  }

  try {
    const fromDate = moment.tz(from, "Asia/Kolkata").toDate();
    const toDate = moment.tz(to, "Asia/Kolkata").toDate();

    if (fromDate > toDate) {
      return res.status(400).json({ error: "'From' date cannot be later than 'To' date" });
    }

    let startHour, startMinute, endHour, endMinute;
    if (startTimeOfDay) {
      const start = moment(startTimeOfDay);
      startHour = start.hour();
      startMinute = start.minute();
    }
    if (endTimeOfDay) {
      const end = moment(endTimeOfDay);
      endHour = end.hour();
      endMinute = end.minute();
    }

    const query = {
      topic,
      timestamp: { $gte: fromDate, $lte: toDate },
    };

    if (minValue !== undefined || maxValue !== undefined) {
      query.message = {};
      if (minValue !== undefined) query.message.$gte = parseFloat(minValue);
      if (maxValue !== undefined) query.message.$lte = parseFloat(maxValue);
    }

    let messages = await MessagesModel.find(query).lean();

    if (startTimeOfDay && endTimeOfDay) {
      messages = messages.filter((msg) => {
        const timestamp = moment.tz(msg.timestamp, "Asia/Kolkata");
        const hour = timestamp.hour();
        const minute = timestamp.minute();

        const startMinutes = startHour * 60 + startMinute;
        const endMinutes = endHour * 60 + endMinute;
        const currentMinutes = hour * 60 + minute;

        return startMinutes <= endMinutes
          ? currentMinutes >= startMinutes && currentMinutes <= endMinutes
          : currentMinutes >= startMinutes || currentMinutes <= endMinutes;
      });
    } else if (startTimeOfDay) {
      messages = messages.filter((msg) => {
        const timestamp = moment.tz(msg.timestamp, "Asia/Kolkata");
        const currentMinutes = timestamp.hour() * 60 + timestamp.minute();
        return currentMinutes >= (startHour * 60 + startMinute);
      });
    } else if (endTimeOfDay) {
      messages = messages.filter((msg) => {
        const timestamp = moment.tz(msg.timestamp, "Asia/Kolkata");
        const currentMinutes = timestamp.hour() * 60 + minute;
        return currentMinutes <= (endHour * 60 + endMinute);
      });
    }

    if (sortOrder) {
      messages.sort((a, b) => sortOrder === "asc"
        ? new Date(a.timestamp) - new Date(b.timestamp)
        : new Date(b.timestamp) - new Date(a.timestamp));
    }

    if (limit) messages = messages.slice(0, parseInt(limit));

    if (!messages.length) {
      return res.json({ topic, messages: [] });
    }

    const groupedMessages = processMessages(messages, granularity, aggregationMethod || "average");
    const response = { success: true, topic, messages: groupedMessages };

    await safeRedisSet(cacheKey, response, TTL_SHORT);
    res.json(response);
  } catch (error) {
    console.error("Error fetching custom range data:", error);
    res.status(500).send("Internal Server Error");
  }
});

const processMessages = (messages, granularity, aggregationMethod) => {
  const grouped = messages.reduce((acc, msg) => {
    const timestamp = moment(msg.timestamp);
    let key;
    switch (granularity) {
      case "seconds": key = timestamp.startOf("second").toISOString(); break;
      case "minutes": key = timestamp.startOf("minute").toISOString(); break;
      case "hours": key = timestamp.startOf("hour").toISOString(); break;
      case "days": key = timestamp.startOf("day").toISOString(); break;
      default: key = timestamp.startOf("minute").toISOString();
    }

    if (!acc[key]) acc[key] = { timestamp: msg.timestamp, values: [] };
    acc[key].values.push(parseFloat(msg.message));
    return acc;
  }, {});

  return Object.values(grouped).map((group) => {
    const values = group.values;
    let aggregatedValue;
    switch (aggregationMethod) {
      case "sum": aggregatedValue = values.reduce((sum, val) => sum + val, 0); break;
      case "min": aggregatedValue = Math.min(...values); break;
      case "max": aggregatedValue = Math.max(...values); break;
      case "average": default: aggregatedValue = values.reduce((sum, val) => sum + val, 0) / values.length;
    }
    return { timestamp: group.timestamp, message: aggregatedValue };
  });
};

router.post("/add", async (req, res) => {
  try {
    const { topic } = req.query;
    const { thresholds } = req.body;
    if (!topic) return res.status(400).json({ error: "Topic name is required" });
    if (!Array.isArray(thresholds) || thresholds.length === 0) {
      return res.status(400).json({ error: "Thresholds are required and must be an array" });
    }

    const existingTopic = await AllTopicsModel.findOne({ topic }).lean();
    if (existingTopic) {
      updateThresholds(topic, thresholds);
      await AllTopicsModel.updateOne({ topic }, { thresholds });
      const updatedTopic = { ...existingTopic, thresholds };
      
      return res.status(200).json({ message: "Thresholds updated successfully", topic: updatedTopic });
    }

    const newTopic = await AllTopicsModel.create({ topic, thresholds });
    res.status(201).json({ topic: newTopic });
  } catch (error) {
    res.status(500).json({ error: "Internal server error", details: error.message });
  }
});

router.get("/get", async (req, res) => {
  try {
    const { topic } = req.query;
    if (!topic) return res.status(400).json({ error: "Topic name is required" });

    const topicData = await AllTopicsModel.findOne({ topic }).lean();
    if (!topicData) return res.status(404).json({ error: "Topic not found" });

    res.status(200).json({ data: topicData });
  } catch (error) {
    res.status(500).json({ error: "Internal server error", details: error.message });
  }
});

router.get("/is-subscribed", (req, res) => {
  const { topic } = req.query;
  if (!topic) return res.status(400).json({ success: false, message: "Topic is required" });

  const isSubscribed = isTopicSubscribed(topic);
  res.json({ success: true, isSubscribed });
});

router.post("/unsubscribe", (req, res) => {
  const { topic } = req.body;
  if (!topic) return res.status(400).json({ success: false, message: "Topic is required" });

  unsubscribeFromTopic(topic);
  res.json({ success: true, message: `Unsubscribed from topic: ${topic}` });
});

const getDayRange = (date) => {
  const start = new Date(date.setHours(0, 0, 0, 0));
  const end = new Date(date.setHours(23, 59, 59, 999));
  return { start, end };
};

router.get("/todays-highest", async (req, res) => {
  const { topic } = req.query;
  const { start, end } = getDayRange(new Date());
  const cacheKey = `${CACHE_PREFIX}today-highest:${topic}`;

  const cachedData = await safeRedisGet(cacheKey);
  if (cachedData) return res.status(200).json(JSON.parse(cachedData));

  try {
    const result = await MessagesModel.findOne({
      topic,
      timestamp: { $gte: start, $lte: end },
    }).sort({ message: -1 }).lean();

    const response = result || { message: "No data available" };
    await safeRedisSet(cacheKey, response, TTL_SHORT);
    res.status(200).json(response);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get("/yesterdays-highest", async (req, res) => {
  const { topic } = req.query;
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const { start, end } = getDayRange(yesterday);
  const cacheKey = `${CACHE_PREFIX}yesterday-highest:${topic}`;

  const cachedData = await safeRedisGet(cacheKey);
  if (cachedData) return res.status(200).json(JSON.parse(cachedData));

  try {
    const result = await MessagesModel.findOne({
      topic,
      timestamp: { $gte: start, $lte: end },
    }).sort({ message: -1 }).lean();

    const response = result || { message: "No data available" };
    await safeRedisSet(cacheKey, response, TTL_MEDIUM);
    res.status(200).json(response);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get("/last-7-days-highest", async (req, res) => {
  const { topic } = req.query;
  const last7Days = new Date();
  last7Days.setDate(last7Days.getDate() - 7);
  const cacheKey = `${CACHE_PREFIX}last7days-highest:${topic}`;

  const cachedData = await safeRedisGet(cacheKey);
  if (cachedData) return res.status(200).json(JSON.parse(cachedData));

  try {
    const result = await MessagesModel.findOne({
      topic,
      timestamp: { $gte: last7Days },
    }).sort({ message: -1 }).lean();

    const response = result || { message: "No data available" };
    await safeRedisSet(cacheKey, response, TTL_MEDIUM);
    res.status(200).json(response);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post("/topic-based-latest-message", async (req, res) => {
  const { topic } = req.body;

  if (!topic) {
    return res.status(400).json({
      success: false,
      message: "Topic is required in the request body.",
    });
  }

  try {
    const latestMessage = await MessagesModel.findOne({ topic })
      .sort({ timestamp: -1 })
      .lean();

    if (!latestMessage) {
      return res.status(404).json({
        success: false,
        message: "No messages found for the given topic.",
      });
    }

    res.status(200).json({
      success: true,
      data: {
        topic,
        timestamp: latestMessage.timestamp.toISOString(),
        message: latestMessage.message,
      },
    });
  } catch (error) {
    console.error("Error fetching latest message:", error.message);
    res.status(500).json({
      success: false,
      message: "Internal Server Error",
      details: error.message,
    });
  }
});

// Existing POST route
router.post("/registergateway-location", async (req, res) => {
  try {
    const { gatewayName, location } = req.body;
    const findGateway = await Gateway.findOne({ topic: gatewayName });
    if (!findGateway) {
      await Gateway.create({ topic: gatewayName, location });
      res.status(200).json({ success: true, message: "Gateway location registered successfully" });
    } else {
      await Gateway.updateOne({ topic: gatewayName }, { $set: { location } });
      res.status(200).json({ success: true, message: "Gateway location updated successfully" });
    }
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Updated GET route to fetch gateway location
router.get("/get-gateway-location/:gatewayName", async (req, res) => {
  try {
    const { gatewayName } = req.params;
    console.log("Gateway Name:", gatewayName); // Log the gatewayName for debugging
    
    if (!gatewayName) {
      return res.status(400).json({ 
        success: false, 
        location: "Location not set", 
        message: "gatewayName is required" 
      });
    }

    const decodedGatewayName = decodeURIComponent(gatewayName);
    const gateway = await Gateway.findOne({ topic: decodedGatewayName });
    
    if (gateway && gateway.location) {
      res.status(200).json({ 
        success: true, 
        location: gateway.location 
      });
    } else {
      res.status(200).json({ 
        success: false, 
        location: "Location not set" 
      });
    }
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      location: "Location not set", 
      message: error.message 
    });
  }
});

process.on('SIGTERM', async () => {
  if (redisConnected) await redisClient.quit();
});

module.exports = router;