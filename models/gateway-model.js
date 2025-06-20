const mongoose = require("mongoose")

const gatewaySchema = new mongoose.Schema({
    topic : {
        type : String,
        required : true,
        unqiue : true,
        index : true,
    },
    location : {
        type : String,
        required : true,
    },
},{
    timestamps : true
})

const Gateway = mongoose.model("Gateway", gatewaySchema)

module.exports = Gateway