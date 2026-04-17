const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const port = process.env.PORT || 3000;

const pool = new Pool({
  host: process.env.PGHOST || "postgres",
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE || "dbt",
  user: process.env.PGUSER || "airflow",
  password: process.env.PGPASSWORD || "airflow",
});

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, "public")));

async function fetchMartData() {
  const query = `
    SELECT
      trade_date,
      livart_close,
      livart_pct_change,
      usdkrw_close,
      usdkrw_pct_change
    FROM mart_livart_analysis
    ORDER BY trade_date ASC
  `;
  const { rows } = await pool.query(query);
  return rows;
}

async function fetchLatestAiReport() {
  const query = `
    SELECT
      report_date,
      title,
      summary,
      recommendation,
      created_at
    FROM ai_ax_reports
    ORDER BY report_date DESC, created_at DESC
    LIMIT 1
  `;

  try {
    const { rows } = await pool.query(query);
    if (!rows.length) {
      return {
        report_date: null,
        title: "AI 전략 리포트가 아직 없습니다",
        summary: "ai_ax_reports 테이블에 데이터가 들어오면 최신 인사이트를 표시합니다.",
        recommendation: "리포트 적재 파이프라인을 실행해 주세요.",
      };
    }
    return rows[0];
  } catch (error) {
    if (error.code === "42P01") {
      return {
        report_date: null,
        title: "AI 전략 리포트 테이블이 아직 생성되지 않았습니다",
        summary: "MVP 단계에서는 테이블 없이도 대시보드가 동작하도록 처리했습니다.",
        recommendation: "추후 ai_ax_reports 테이블을 생성하면 최신 리포트를 자동 표시합니다.",
      };
    }
    throw error;
  }
}

app.get("/api/dashboard", async (req, res) => {
  try {
    const [marketData, latestReport] = await Promise.all([
      fetchMartData(),
      fetchLatestAiReport(),
    ]);
    res.json({ marketData, latestReport });
  } catch (error) {
    res.status(500).json({
      message: "대시보드 데이터를 불러오지 못했습니다.",
      detail: error.message,
    });
  }
});

app.get("/", async (req, res) => {
  try {
    const [marketData, latestReport] = await Promise.all([
      fetchMartData(),
      fetchLatestAiReport(),
    ]);
    res.render("index", { marketData, latestReport });
  } catch (error) {
    res.status(500).send(`대시보드 렌더링 실패: ${error.message}`);
  }
});

app.listen(port, () => {
  console.log(`Livart web dashboard listening on port ${port}`);
});
