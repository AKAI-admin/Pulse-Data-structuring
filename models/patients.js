const mongoose = require("mongoose");

const patientSchema = new mongoose.Schema(
  {
    patientId: { type: Number, required: true, unique: true },
    name: { type: String, required: true },
    gender: {
      type: String,
      required: true,
      enum: ["male", "female", "other"],
    },
    dob: { type: Date, required: true },
  },
  { timestamps: true },
);

module.exports = mongoose.model("Patient", patientSchema);
