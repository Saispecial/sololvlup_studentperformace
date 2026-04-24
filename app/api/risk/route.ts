/**
 * GET /api/risk
 * Fetches latest activity per user from NeonDB, then calls the ML service
 * to get dropout risk + engagement score for each user.
 */
import { neon } from "@neondatabase/serverless";
import { NextResponse } from "next/server";

const db = neon(process.env.DATABASE_URL!);
const ML_SERVICE_URL = process.env.ML_SERVICE_URL ?? "http://localhost:8000";

export async function GET() {
  try {
    // Latest activity snapshot per user
    const rows = await db`
      SELECT DISTINCT ON (user_id)
        user_id, hour_of_day, time_spent_minutes, quiz_score,
        streak_days, quests_completed_today, xp_earned_today, cumulative_xp
      FROM fact_user_activity
      ORDER BY user_id, timestamp DESC
    `;

    // Call ML service for each user
    const predictions = await Promise.all(
      rows.map(async (row: any) => {
        const res = await fetch(`${ML_SERVICE_URL}/predict`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(row),
        });
        const pred = await res.json();
        return { user_id: row.user_id, ...pred };
      })
    );

    return NextResponse.json({ data: predictions });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
