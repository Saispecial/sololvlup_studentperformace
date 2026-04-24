/**
 * GET /api/xp
 * Returns total XP earned per user.
 */
import { neon } from "@neondatabase/serverless";
import { NextResponse } from "next/server";

const db = neon(process.env.DATABASE_URL!);

export async function GET() {
  try {
    const rows = await db`
      SELECT user_id, SUM(xp_earned_today) AS total_xp
      FROM fact_user_activity
      GROUP BY user_id
      ORDER BY total_xp DESC
    `;
    return NextResponse.json({ data: rows });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
