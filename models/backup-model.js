const mongoose = require("mongoose");

const messageSchema = mongoose.Schema({
    message: {
        type: Number
    },
    timestamp: {
        type: Date,
        default: Date.now
    }
});

const backupSchema = mongoose.Schema({
    topic: {
        type: String,
        index: true,
    },
    messages: {
        type: [messageSchema]
    }
}, {
    timestamps: true
});

const Backup = mongoose.model("Backup", backupSchema);

module.exports = Backup;
