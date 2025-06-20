const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema({
  topic: { type: String, required: true, index: true }, 
  message: { type: String, required: true, index: true },
  timestamp: { type: Date, default: Date.now, index: true },
}, { timestamps: false });

messageSchema.index({ topic: 1, timestamp: -1 });

const MessagesModel = mongoose.model("Message", messageSchema);
module.exports = MessagesModel;