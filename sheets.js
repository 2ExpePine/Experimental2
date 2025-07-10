// sheets.js
import { google } from "googleapis";
import dotenv from "dotenv";
dotenv.config();

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n"),
  },
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

export async function getChartLinks() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.SHEET_ID,
    range: `${process.env.SOURCE_SHEET}!D2:D`,
  });
  return res.data.values.map(([url]) => url.replace(/"/g, "").trim());
}

export async function writeBulkWithRetry(startRow, rows, maxRetry = 3) {
  const ranges = rows.map(
    (_, i) =>
      `${process.env.OUTPUT_SHEET}!B${startRow + i + 2}:AZ${startRow + i + 2}`
  );
  const data = rows.map((vals, i) => ({
    range: `${process.env.OUTPUT_SHEET}!B${startRow + i + 2}`,
    values: [vals],
  }));

  for (let tryNum = 1; tryNum <= maxRetry; tryNum++) {
    try {
      await sheets.spreadsheets.values.batchClear({
        spreadsheetId: process.env.OUTPUT_SHEET_ID,
        requestBody: { ranges },
      });
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: process.env.OUTPUT_SHEET_ID,
        requestBody: { valueInputOption: "RAW", data },
      });
      return;
    } catch (e) {
      if (tryNum === maxRetry) throw e;
      await new Promise((r) => setTimeout(r, 2 ** tryNum * 1000));
    }
  }
}
