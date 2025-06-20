const winston = require("winston");
const dotenv = require("dotenv");
const http = require("http");
const { Server } = require("socket.io");
const { subscribeToTopic, getLatestLiveMessage } = require("./middlewares/mqttHandler");
const SubscribedTopic = require("./models/subscribed-topic-model");
const express = require("express");
const connectDB = require("./env/db");

dotenv.config({ path: "./.env" });

connectDB();

const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const activeTopics = new Map();

io.on("connection", (socket) => {
  const subscriptions = new Map();

  socket.on("subscribeToTopic", async (topic) => {
    if (!topic || subscriptions.has(topic)) return;

    try {
      socket.join(topic);
      subscriptions.set(topic, true);

      if (!activeTopics.has(topic)) {
        activeTopics.set(topic, { 
          clients: new Set(), 
          lastMessage: null, 
          lastSentTime: null, 
          interval: null 
        });
        startTopicStream(topic);
      }

      activeTopics.get(topic).clients.add(socket.id);

      const latestMessage = await getLatestLiveMessage(topic);
      if (latestMessage) {
        socket.emit("liveMessage", { success: true, message: latestMessage, topic });
      }
    } catch (error) {
      logger.error(`Subscription error for ${topic}: ${error.message}`);
    }
  });

  socket.on("unsubscribeFromTopic", (topic) => {
    if (subscriptions.has(topic)) {
      socket.leave(topic);
      subscriptions.delete(topic);

      if (activeTopics.has(topic)) {
        const topicData = activeTopics.get(topic);
        topicData.clients.delete(socket.id);

        if (topicData.clients.size === 0) {
          clearInterval(topicData.interval);
          activeTopics.delete(topic);
        }
      }
    }
  });

  // Handle disconnection
  socket.on("disconnect", () => {
    subscriptions.forEach((_, topic) => {
      socket.leave(topic);

      if (activeTopics.has(topic)) {
        const topicData = activeTopics.get(topic);
        topicData.clients.delete(socket.id);

        if (topicData.clients.size === 0) {
          clearInterval(topicData.interval);
          activeTopics.delete(topic);
        }
      }
    });
    subscriptions.clear();
  });
});

const startTopicStream = (topic) => {
  const topicData = activeTopics.get(topic);

  topicData.interval = setInterval(async () => {
    try {
      const currentTime = Date.now();
      const latestMessage = await getLatestLiveMessage(topic);
      // console.log(`[Backend] Fetched latest message for ${topic}:`, latestMessage); 

      if (latestMessage) {
        const hasChanged = !topicData.lastMessage || 
                          topicData.lastMessage.message.message !== latestMessage.message.message;
        const timeSinceLastSent = topicData.lastSentTime ? 
                                  (currentTime - topicData.lastSentTime) : 
                                  Infinity;

        // console.log(`[Backend] Topic: ${topic}, hasChanged: ${hasChanged}, timeSinceLastSent: ${timeSinceLastSent}`);

        if (hasChanged || timeSinceLastSent >= 1000) {
          io.to(topic).emit("liveMessage", { success: true, message: latestMessage, topic });
          // console.log(`[Backend] Emitted liveMessage for ${topic}:`, { success: true, message: latestMessage, topic });
          topicData.lastMessage = latestMessage;
          topicData.lastSentTime = currentTime;
        } else {
          // console.log(`[Backend] Skipped emission for ${topic} (no change or too soon)`);
        }
      } else {
        // console.log(`[Backend] No message available for ${topic}`);
      }
    } catch (error) {
      logger.error(`Stream error for ${topic}: ${error.message}`);
    }
  }, 200);
};

const socketPort = process.env.SOCKET_PORT || 4000;
server.listen(socketPort, "0.0.0.0", () => {
  logger.info(`Socket.IO Server running on port ${socketPort}`);

  setTimeout(async () => {
    try {
      const SubscribedTopicList = await SubscribedTopic.find({}, { _id: 0, topic: 1 });
      if (SubscribedTopicList?.length > 0) {
        const topicsToSubscribe = [];

        SubscribedTopicList.forEach(({ topic }) => {
          // Add the original topic
          topicsToSubscribe.push(topic);              
          // Add the backup topic
          topicsToSubscribe.push(`${topic}|backup`);  
          // Extract and add the base topic (e.g., company/gateway from company/gateway/topic|unit)
          const baseTopicMatch = topic.match(/^(.*?)\/[^/]+(?:\|[^/]+)?$/);
          if (baseTopicMatch) {
            const baseTopic = baseTopicMatch[1]; 
            topicsToSubscribe.push(baseTopic);
          }
        });

        await Promise.all(topicsToSubscribe.map(topic => subscribeToTopic(topic)));
        logger.info("MQTT topics (including backup topics and base topics) subscribed successfully");
      }
    } catch (err) {
      logger.error(`Error subscribing to topics: ${err.message}`);
    }
  }, 5000);
});