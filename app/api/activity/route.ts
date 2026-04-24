/**
 * GET /api/activity
 * Returns all rows from fact_user_activity via NeonDB.
 */
import { neon } from "@neondatabase/serverless";
import { NextResponse } from "next/server";

const db = neon(process.env.DATABASE_URL!);

export async function GET() {
  try {
    const rows = await db`
      SELECT * FROM fact_user_activity
      ORDER BY timestamp DESC
      LIMIT 500
    `;
    return NextResponse.json({ data: rows });
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
