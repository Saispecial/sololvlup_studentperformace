/**
 * neon_runner.js
 * Thin Node.js shim that reads a SQL payload from stdin and executes it
 * against NeonDB using @neondatabase/serverless (HTTP driver — no TCP needed).
 *
 * DATABASE_URL must be set in the environment or a .env file at project root.
 * Run:  node neon_runner.js  (stdin = JSON { sql, projectId, database })
 */
require("dotenv").config({ path: require("path").resolve(__dirname, "../.env") });

const { neon } = require("@neondatabase/serverless");

async function main() {
  let input = "";
  process.stdin.on("data", (chunk) => (input += chunk));
  process.stdin.on("end", async () => {
    try {
      const { sql } = JSON.parse(input);
      const db = neon(process.env.DATABASE_URL);
      const rows = await db.query(sql);
      process.stdout.write(JSON.stringify({ rows: rows.rows ?? rows }));
    } catch (err) {
      process.stderr.write(err.message);
      process.exit(1);
    }
  });
}

main();
