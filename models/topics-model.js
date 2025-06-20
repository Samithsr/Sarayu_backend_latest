const mongoose = require("mongoose");

const topicSchema = new mongoose.Schema({
  topic: { type: String, required: true, unique: true, index: true },
  company: { type: String, index: true },
  label: { type: String, index: true },
  device: { type: String, index: true },
}, { timestamps: true });

topicSchema.index({ topic: 1, createdAt: -1 });

const TopicsModel = mongoose.model("Topic", topicSchema);
module.exports = TopicsModel;