const mongoose = require("mongoose");

const patientSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    gender: {
      type: String,
      required: false,
      enum: ["male", "female", "other"],
    },
    yob: { type: String, required: false },
  },
  { timestamps: true },
);

module.exports = mongoose.model("Patient", patientSchema);
