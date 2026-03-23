const mongoose = require("mongoose");

const scanSchema = new mongoose.Schema(
  {
    patient: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Patient",
      required: true,
      index: true,
    },
    studyDate: { type: Date, required: false },
    report_url: { type: String, required: true },
    scan_url: { type: String, required: false },
    tags: [{ type: String }],
  },
  { timestamps: true },
);

module.exports = mongoose.model("Scan", scanSchema);
