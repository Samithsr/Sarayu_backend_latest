const mqtt = require("mqtt");
const MessageModel = require("../models/mqtt-message-model");
const AllTopicsModel = require("../models/all-mqtt-messages");
const Supervisor = require("../models/supervisor-model");
const Employee = require("../models/employee-model");
const sendMail = require("../utils/mail");
const NodeCache = require("node-cache");
const { EventEmitter } = require("events");

// Constants for configuration
const BATCH_SIZE = 10;
const BATCH_INTERVAL = 1000; // Batch processing interval in milliseconds
const MAX_QUEUE_SIZE = 100; // Maximum size of the message queue per topic
const MAX_MAIL_RETRIES = 3; // Maximum retries for email sending
const MAIL_RETRY_DELAY = 1000; // Delay between email retries in milliseconds
const THRESHOLD_COOLDOWN_PERIOD = 30000; // Cooldown period for threshold alerts (30 seconds)

// Email Queue Class for managing email sending
class EmailQueue extends EventEmitter {
  constructor() {
    super();
    this.queue = [];
    this.processing = false;
    this.processQueue();
  }

  async addToQueue(emailData) {
    this.queue.push({
      emailData,
      retries: 0,
      timestamp: Date.now(),
    });
    this.emit("mailAdded");
  }

  async processQueue() {
    if (this.processing || this.queue.length === 0) {
      setTimeout(() => this.processQueue(), 100);
      return;
    }
    this.processing = true;

    try {
      const currentTime = Date.now();
      const mailPromises = [];

      while (this.queue.length > 0) {
        const email = this.queue[0];

        // Skip emails that have exceeded max retries
        if (email.retries >= MAX_MAIL_RETRIES) {
          console.error(`Failed to send email after ${MAX_MAIL_RETRIES} retries:`, email);
          this.queue.shift();
          continue;
        }

        // Wait for retry delay if applicable
        if (email.timestamp + MAIL_RETRY_DELAY > currentTime && email.retries > 0) {
          break;
        }

        const currentEmail = this.queue.shift();
        mailPromises.push(
          this.sendMailWithRetry(currentEmail).catch((error) => {
            console.error("Email sending error:", error);
            currentEmail.retries++;
            currentEmail.timestamp = Date.now();
            this.queue.push(currentEmail);
          })
        );
      }

      await Promise.all(mailPromises);
    } finally {
      this.processing = false;
      setTimeout(() => this.processQueue(), 100);
    }
  }

  async sendMailWithRetry({ emailData, retries }) {
    try {
      const { recipients, subject, message } = emailData;

      if (!Array.isArray(recipients) || recipients.length === 0) {
        console.error("Invalid or empty recipients list:", recipients);
        return;
      }

      const sendPromises = recipients.map((recipient) =>
        sendMail(recipient, subject, message).catch((error) => {
          console.error(`Failed to send email to ${recipient}:`, error);
          throw error;
        })
      );

      await Promise.all(sendPromises);
      console.log(`Successfully sent emails to ${recipients.length} recipients`);
    } catch (error) {
      if (retries < MAX_MAIL_RETRIES) {
        throw error;
      }
      console.error("Max retries reached for email:", emailData);
    }
  }
}

// MQTT Handler Class for managing MQTT operations
class MQTTHandler {
  constructor() {
    this.messageQueue = new Map(); // Queue for batching messages
    this.latestMessages = new Map(); // Store latest messages per topic
    this.subscribedTopics = new Set(); // Track subscribed topics
    this.thresholdStates = new Map(); // Track threshold states
    this.processingBatch = false; // Prevent concurrent batch processing
    this.emailQueue = new EmailQueue(); // Email queue for threshold alerts
    this.recipientsCache = new NodeCache({
      stdTTL: 3600, // Cache TTL for recipients (1 hour)
      checkperiod: 600,
      useClones: false,
    });
    this.thresholdCache = new NodeCache({
      stdTTL: 1800, // Cache TTL for thresholds (30 minutes)
      checkperiod: 300,
      useClones: false,
    });

    // Initialize MQTT client
    this.client = this.initializeClient();
    this.initializeMessageBatchProcessing();

    // Set up interval to flush threshold cache every 2 minutes
    setInterval(() => {
      this.thresholdCache.flushAll();
      console.log('Threshold cache flushed');
    }, 120000); // 120000 ms = 2 minutes
  }

  initializeClient() {
    const options = {
      host: '192.168.1.172', // Replace with your EC2 instance's IP or domain
      port: 1883, // Use 8883 if TLS/SSL is enabled
      protocol: 'mqtt', // Use 'mqtts' if TLS/SSL is enabled
      keepalive: 30,
      reconnectPeriod: 1000,
      clean: true,
      connectTimeout: 10000,
      // Uncomment and set the following if authentication is required
      // username: 'Sarayu',
      // password: 'IOTteam@123',
      // Uncomment and set the following if using TLS/SSL
      // ca: fs.readFileSync('path/to/ca.crt'),
      // cert: fs.readFileSync('path/to/client.crt'),
      // key: fs.readFileSync('path/to/client.key'),
      // rejectUnauthorized: true, // Set to false if using self-signed certificates
    };

    const client = mqtt.connect(options);

    client.on("connect", () => {
      console.log("Connected to HiveMQ broker");
      this.resubscribeToTopics();
    });

    client.on("message", this.handleMessage.bind(this));
    client.on("error", (error) => console.error("MQTT Error:", error));
    client.on("offline", () => console.log("MQTT Client Offline"));
    client.on("reconnect", () => console.log("MQTT Client Reconnecting"));

    return client;
  }

  resubscribeToTopics() {
    for (const topic of this.subscribedTopics) {
      this.client.subscribe(topic, (err) => {
        if (err) {
          console.error(`Error resubscribing to topic ${topic}:`, err);
        } else {
          console.log(`Resubscribed to topic: ${topic}`);
        }
      });
    }
  }

  initializeMessageBatchProcessing() {
    setInterval(async () => {
      if (this.processingBatch) return;
      try {
        this.processingBatch = true;
        await this.processBatch();
      } finally {
        this.processingBatch = false;
      }
    }, BATCH_INTERVAL);
  }

  parsePayload(payload) {
    try {
      const payloadStr = typeof payload === "string" ? payload : payload.toString();

      try {
        const jsonParsed = JSON.parse(payloadStr);
        if (jsonParsed && typeof jsonParsed === "object") {
          if (jsonParsed.message && jsonParsed.message.message !== undefined) {
            return jsonParsed.message.message;
          }
          if (jsonParsed.message !== undefined) {
            const numValue = parseFloat(jsonParsed.message);
            return !isNaN(numValue) ? numValue : jsonParsed.message;
          }
        }
        const numValue = parseFloat(jsonParsed);
        return !isNaN(numValue) ? numValue : null;
      } catch (jsonError) {
        const numValue = parseFloat(payloadStr);
        return !isNaN(numValue) ? numValue : null;
      }
    } catch (error) {
      console.error("Payload parsing error:", error);
      return null;
    }
  }

  async handleMessage(topic, payload) {
    try {
      const value = this.parsePayload(payload);
      console.log(value);
      if (value === null) {
        console.warn(`Unable to parse payload for topic ${topic}:`, payload.toString());
        return;
      }

      this.updateLatestMessage(topic, payload.toString());
      if (payload.length < 100) {
        this.queueMessage(topic, value);
        await this.checkThresholds(topic, value);
      }
    } catch (error) {
      console.error("Message handling error:", error);
    }
  }

  updateLatestMessage(topic, value) {
    const messageData = {
      message: { message: value },
      timestamp: new Date(),
    };
    this.latestMessages.set(topic, messageData);
  }

  queueMessage(topic, value) {
    if (!this.messageQueue.has(topic)) {
      this.messageQueue.set(topic, []);
    }
    const messages = this.messageQueue.get(topic);
    messages.push({ value, timestamp: Date.now() });

    if (messages.length > MAX_QUEUE_SIZE) {
      messages.splice(0, messages.length - MAX_QUEUE_SIZE);
    }
  }

  async processBatch() {
    const batchOperations = [];

    for (const [topic, messages] of this.messageQueue.entries()) {
      if (messages.length === 0) continue;

      const batch = messages.splice(0, BATCH_SIZE);
      if (batch.length > 0) {
        batchOperations.push(
          MessageModel.bulkWrite(
            batch.map(({ value, timestamp }) => ({
              updateOne: {
                filter: { topic },
                update: {
                  $push: {
                    messages: {
                      message: value,
                      timestamp: new Date(timestamp),
                    },
                  },
                },
                upsert: true,
              },
            }))
          )
        );
      }
    }

    if (batchOperations.length > 0) {
      await Promise.allSettled(batchOperations).then((results) => {
        results.forEach((result, index) => {
          if (result.status === "rejected") {
            console.error(`Batch operation ${index} failed:`, result.reason);
          }
        });
      });
    }
  }

  async getRecipients(topic) {
    const cached = this.recipientsCache.get(topic);
    if (cached) return cached;

    try {
      const [employees, supervisors] = await Promise.all([
        Employee.find({ topics: topic }).select("email").lean(),
        Supervisor.find({ topics: topic }).select("email").lean(),
      ]);

      const recipients = [
        ...new Set([...employees.map((emp) => emp.email), ...supervisors.map((sup) => sup.email)]),
      ];

      if (recipients.length > 0) {
        this.recipientsCache.set(topic, recipients);
      }
      return recipients;
    } catch (error) {
      console.error("Error fetching recipients:", error);
      return [];
    }
  }

  async getThresholds(topic) {
    const cached = this.thresholdCache.get(topic);
    if (cached) {
      console.log(`Fetched thresholds for topic ${topic} from cache`);
      return cached.thresholds;
    }

    try {
      const topicData = await AllTopicsModel.findOne({ topic }).select("thresholds").lean();
      if (topicData) {
        console.log(`Fetched thresholds for topic ${topic} from database`);
        this.thresholdCache.set(topic, topicData);
        return topicData.thresholds;
      }
      return null;
    } catch (error) {
      console.error("Error fetching thresholds:", error);
      return null;
    }
  }

  async updateThresholds(topic, newThresholds) {
    try {
      await AllTopicsModel.updateOne({ topic }, { thresholds: newThresholds }, { upsert: true });
      this.thresholdCache.del(topic);
      console.log(`Updated thresholds for topic ${topic} and cleared cache`);
    } catch (error) {
      console.error("Error updating thresholds:", error);
    }
  }

  async checkThresholds(topic, liveValue) {
    const thresholds = await this.getThresholds(topic);
    if (!thresholds?.length) return;

    const sortedThresholds = [...thresholds].sort((a, b) => b.value - a.value);
    const topicState = this.thresholdStates.get(topic) || new Map();
    const currentTime = Date.now();
    let dangerTriggered = false;

    for (const { color, value, resetValue } of sortedThresholds) {
      const stateKey = `${color}-${value}`;
      const currentState = topicState.get(stateKey) || {
        triggered: false,
        lastAlertTime: 0,
      };

      if (liveValue >= value) {
        if (color === "red") {
          dangerTriggered = true;
        } else if (dangerTriggered) {
          continue;
        }

        const cooldownPassed = currentTime - currentState.lastAlertTime >= THRESHOLD_COOLDOWN_PERIOD;
        if (!currentState.triggered || cooldownPassed) {
          topicState.set(stateKey, {
            triggered: true,
            lastAlertTime: currentTime,
          });

          const recipients = await this.getRecipients(topic);
          if (recipients.length > 0) {
            const alert = {
              recipients,
              ...this.prepareThresholdAlert(topic, { color, value }, liveValue),
            };
            await this.emailQueue.addToQueue(alert);
          }

          if (color === "red") {
            break;
          }
        }
      } else if (liveValue < resetValue) {
        topicState.set(stateKey, {
          triggered: false,
          lastAlertTime: 0,
        });
      }
    }

    this.thresholdStates.set(topic, topicState);
  }

  prepareThresholdAlert(topic, threshold, liveValue) {
    const alertType = threshold.color === "red" ? "Danger" : "Warning";
    const severity = threshold.color === "red" ? "critical" : "warning";

    return {
      subject: `${alertType}: ${topic} Threshold Exceeded`,
      message: `
${alertType} Alert for ${topic}
Current Value: ${liveValue}
Threshold: ${threshold.value}
Severity: ${severity}
Timestamp: ${new Date().toISOString()}
${
  threshold.color === "red"
    ? "IMMEDIATE ACTION REQUIRED: Critical threshold exceeded!"
    : "WARNING: Monitor situation closely."
}`,
    };
  }

  subscribeToTopic(topic) {
    if (!this.subscribedTopics.has(topic)) {
      this.client.subscribe(topic, (err) => {
        if (err) {
          console.error(`Error subscribing to topic ${topic}:`, err);
        } else {
          console.log(`Subscribed to topic: ${topic}`);
          this.subscribedTopics.add(topic);
          this.messageQueue.set(topic, []);
        }
      });
    }
  }

  unsubscribeFromTopic(topic) {
    if (this.subscribedTopics.has(topic)) {
      this.client.unsubscribe(topic, (err) => {
        if (err) {
          console.error(`Error unsubscribing from topic ${topic}:`, err);
        } else {
          console.log(`Unsubscribed from topic: ${topic}`);
          this.subscribedTopics.delete(topic);
          this.messageQueue.delete(topic);
          this.latestMessages.delete(topic);
          this.thresholdStates.delete(topic);
        }
      });
    }
  }

  getLatestLiveMessage(topic) {
    const message = this.latestMessages.get(topic);
    return message || null;
  }

  isTopicSubscribed(topic) {
    return this.subscribedTopics.has(topic);
  }
}

const mqttHandler = new MQTTHandler();

module.exports = {
  subscribeToTopic: mqttHandler.subscribeToTopic.bind(mqttHandler),
  getLatestLiveMessage: mqttHandler.getLatestLiveMessage.bind(mqttHandler),
  isTopicSubscribed: mqttHandler.isTopicSubscribed.bind(mqttHandler),
  unsubscribeFromTopic: mqttHandler.unsubscribeFromTopic.bind(mqttHandler),
  updateThresholds: mqttHandler.updateThresholds.bind(mqttHandler),
};