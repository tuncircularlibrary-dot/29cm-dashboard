"""
FastAPI 백엔드 v2 — Supabase (PostgreSQL) 연동
"""
import os
from datetime import date, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app = FastAPI(title="29CM 랭킹 API v2", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise HTTPException(503, "DATABASE_URL 환경변수가 없습니다.")
    return psycopg2.connect(url, sslmode="require")

def qdf(conn, sql, params=()):
    return pd.read_sql_query(sql, conn, params=params)

BASE_Q = """
    SELECT dr.collected_date::text, dr.rank,
           p.brand, p.product_name, p.product_url,
           dr.original_price, dr.final_price, dr.discount_rate,
           dr.tag_exclusive, dr.tag_new, dr.tag_coupon, dr.tag_free_shipping
    FROM daily_rankings dr
    JOIN products p ON dr.product_id = p.id
"""

def latest_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(collected_date)::text FROM daily_rankings")
        r = cur.fetchone()
        return r[0] if r and r[0] else str(date.today())


# ── 정적 파일 (React 빌드 결과) ──────────────────────────
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ── API 엔드포인트 ────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/dates")
def get_dates():
    conn = get_conn()
    try:
        df = qdf(conn, "SELECT DISTINCT collected_date::text FROM daily_rankings ORDER BY collected_date DESC")
        return {"dates": df["collected_date"].tolist()}
    finally:
        conn.close()

@app.get("/api/summary")
def get_summary(target_date: Optional[str] = Query(None)):
    conn = get_conn()
    try:
        if not target_date:
            target_date = latest_date(conn)
        df = qdf(conn, BASE_Q + " WHERE dr.collected_date = %s", (target_date,))
        if df.empty:
            raise HTTPException(404, f"{target_date} 데이터 없음")
        disc = df[df["discount_rate"] > 0]
        return {
            "date": target_date,
            "total_products": len(df),
            "avg_final_price": round(df["final_price"].mean()),
            "avg_discount_rate": round(df["discount_rate"].mean(), 1),
            "discounted_count": len(disc),
            "discounted_ratio": round(len(disc) / len(df) * 100, 1),
            "tags": {
                "exclusive": int(df["tag_exclusive"].sum()),
                "new": int(df["tag_new"].sum()),
                "coupon": int(df["tag_coupon"].sum()),
                "free_shipping": int(df["tag_free_shipping"].sum()),
            },
        }
    finally:
        conn.close()

@app.get("/api/top-brands")
def get_top_brands(target_date: Optional[str] = Query(None), top_n: int = Query(10)):
    conn = get_conn()
    try:
        if not target_date:
            target_date = latest_date(conn)
        df = qdf(conn, BASE_Q + " WHERE dr.collected_date = %s", (target_date,))
        if df.empty:
            return {"date": target_date, "brands": []}
        bc = (df.groupby("brand")
              .agg(count=("brand", "count"), avg_price=("final_price", "mean"), avg_rank=("rank", "mean"))
              .reset_index().sort_values("count", ascending=False).head(top_n))
        total = len(df)
        return {"date": target_date, "brands": [
            {"brand": r["brand"], "count": int(r["count"]),
             "share": round(r["count"] / total * 100, 1),
             "avg_price": round(r["avg_price"]), "avg_rank": round(r["avg_rank"], 1)}
            for _, r in bc.iterrows()
        ]}
    finally:
        conn.close()

@app.get("/api/price-distribution")
def get_price_dist(target_date: Optional[str] = Query(None)):
    conn = get_conn()
    try:
        if not target_date:
            target_date = latest_date(conn)
        df = qdf(conn, BASE_Q + " WHERE dr.collected_date = %s", (target_date,))
        if df.empty:
            return {"date": target_date, "distribution": []}
        bins = [0, 100_000, 200_000, 300_000, 500_000, 1_000_000, float("inf")]
        labels = ["~10만", "10~20만", "20~30만", "30~50만", "50~100만", "100만~"]
        df["range"] = pd.cut(df["final_price"], bins=bins, labels=labels, right=False)
        dist = df["range"].value_counts().reindex(labels, fill_value=0)
        return {"date": target_date, "distribution": [
            {"range": l, "count": int(c)} for l, c in dist.items()
        ]}
    finally:
        conn.close()

@app.get("/api/tag-stats")
def get_tag_stats():
    conn = get_conn()
    try:
        df = qdf(conn, BASE_Q)
        if df.empty:
            return {"trend": []}
        daily = (df.groupby("collected_date")
                 .agg(exclusive=("tag_exclusive", "sum"), new=("tag_new", "sum"),
                      coupon=("tag_coupon", "sum"), free_shipping=("tag_free_shipping", "sum"),
                      total=("rank", "count"))
                 .reset_index().sort_values("collected_date"))
        return {"trend": daily.to_dict(orient="records")}
    finally:
        conn.close()

@app.get("/api/daily-rankings")
def get_daily_rankings(target_date: Optional[str] = Query(None), limit: int = Query(100)):
    conn = get_conn()
    try:
        if not target_date:
            target_date = latest_date(conn)
        df = qdf(conn, BASE_Q + " WHERE dr.collected_date = %s ORDER BY dr.rank ASC LIMIT %s",
                 (target_date, limit))
        if df.empty:
            return {"date": target_date, "rankings": []}
        for col in ["tag_exclusive", "tag_new", "tag_coupon", "tag_free_shipping"]:
            df[col] = df[col].astype(bool)
        return {"date": target_date, "rankings": df.to_dict(orient="records")}
    finally:
        conn.close()

@app.get("/api/brand-list")
def brand_list():
    conn = get_conn()
    try:
        df = qdf(conn, "SELECT DISTINCT brand FROM products ORDER BY brand")
        return {"brands": df["brand"].tolist()}
    finally:
        conn.close()

@app.get("/api/trend/weekly")
def get_weekly_trend(weeks: int = Query(8, ge=1, le=52)):
    conn = get_conn()
    try:
        df = qdf(conn, BASE_Q)
        if df.empty:
            return {"weekly": []}
        df["collected_date"] = pd.to_datetime(df["collected_date"])
        df["week"] = df["collected_date"].dt.to_period("W").astype(str)
        df["week_start"] = df["collected_date"].dt.to_period("W").apply(
            lambda p: p.start_time.strftime("%Y-%m-%d"))
        recent = sorted(df["week"].unique())[-weeks:]
        df = df[df["week"].isin(recent)]
        weekly = (df.groupby(["week", "week_start"])
                  .agg(avg_price=("final_price", "mean"),
                       avg_discount=("discount_rate", "mean"),
                       tag_coupon=("tag_coupon", "sum"),
                       tag_free_shipping=("tag_free_shipping", "sum"),
                       tag_new=("tag_new", "sum"),
                       total=("rank", "count"),
                       data_days=("collected_date", "nunique"))
                  .reset_index().sort_values("week_start"))
        result = []
        for _, r in weekly.iterrows():
            grp = df[df["week"] == r["week"]]
            top3 = grp.groupby("brand").size().sort_values(ascending=False).head(3)
            result.append({
                "week": r["week"], "week_start": r["week_start"],
                "avg_price": round(r["avg_price"]),
                "avg_discount": round(r["avg_discount"], 1),
                "tag_coupon": int(r["tag_coupon"]),
                "tag_free_shipping": int(r["tag_free_shipping"]),
                "tag_new": int(r["tag_new"]),
                "total": int(r["total"]), "data_days": int(r["data_days"]),
                "top_brands": [{"brand": b, "count": int(c)} for b, c in top3.items()],
            })
        return {"weeks": len(result), "weekly": result}
    finally:
        conn.close()

@app.get("/api/trend/monthly")
def get_monthly_trend(months: int = Query(6, ge=1, le=24)):
    conn = get_conn()
    try:
        df = qdf(conn, BASE_Q)
        if df.empty:
            return {"monthly": []}
        df["collected_date"] = pd.to_datetime(df["collected_date"])
        df["month"] = df["collected_date"].dt.to_period("M").astype(str)
        recent = sorted(df["month"].unique())[-months:]
        df = df[df["month"].isin(recent)]
        monthly = (df.groupby("month")
                   .agg(avg_price=("final_price", "mean"),
                        min_price=("final_price", "min"),
                        max_price=("final_price", "max"),
                        avg_discount=("discount_rate", "mean"),
                        tag_coupon=("tag_coupon", "sum"),
                        tag_free_shipping=("tag_free_shipping", "sum"),
                        tag_new=("tag_new", "sum"),
                        total=("rank", "count"),
                        data_days=("collected_date", "nunique"))
                   .reset_index().sort_values("month"))
        bins = [0, 100_000, 200_000, 300_000, 500_000, 1_000_000, float("inf")]
        lbl = ["~10만", "10~20만", "20~30만", "30~50만", "50~100만", "100만~"]
        result = []
        for _, r in monthly.iterrows():
            m_df = df[df["month"] == r["month"]].copy()
            top5 = m_df.groupby("brand").size().sort_values(ascending=False).head(5)
            total_m = len(m_df)
            m_df["range"] = pd.cut(m_df["final_price"], bins=bins, labels=lbl, right=False)
            pdist = m_df["range"].value_counts().reindex(lbl, fill_value=0)
            result.append({
                "month": r["month"],
                "avg_price": round(r["avg_price"]),
                "min_price": round(r["min_price"]),
                "max_price": round(r["max_price"]),
                "avg_discount": round(r["avg_discount"], 1),
                "tag_coupon_ratio": round(r["tag_coupon"] / r["total"] * 100, 1),
                "tag_free_shipping_ratio": round(r["tag_free_shipping"] / r["total"] * 100, 1),
                "tag_new_ratio": round(r["tag_new"] / r["total"] * 100, 1),
                "total": int(r["total"]), "data_days": int(r["data_days"]),
                "top_brands": [{"brand": b, "count": int(c),
                                "share": round(c / total_m * 100, 1)} for b, c in top5.items()],
                "price_distribution": [{"range": l, "count": int(c)} for l, c in pdist.items()],
            })
        return {"months": len(result), "monthly": result}
    finally:
        conn.close()

@app.get("/api/trend/brand-rank")
def get_brand_rank_trend(brand: str = Query(...), days: int = Query(30, ge=7, le=180)):
    conn = get_conn()
    try:
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        df = qdf(conn, BASE_Q + " WHERE p.brand=%s AND dr.collected_date>=%s ORDER BY dr.collected_date",
                 (brand, cutoff))
        if df.empty:
            return {"brand": brand, "trend": []}
        daily = (df.groupby("collected_date")
                 .agg(best_rank=("rank", "min"), avg_rank=("rank", "mean"),
                      count=("rank", "count"), avg_price=("final_price", "mean"))
                 .reset_index())
        return {
            "brand": brand,
            "overall_best_rank": int(daily["best_rank"].min()),
            "overall_avg_rank": round(daily["avg_rank"].mean(), 1),
            "trend": [{"date": r["collected_date"], "best_rank": int(r["best_rank"]),
                       "avg_rank": round(r["avg_rank"], 1), "count": int(r["count"]),
                       "avg_price": round(r["avg_price"])}
                      for _, r in daily.iterrows()],
        }
    finally:
        conn.close()

@app.get("/api/trend/price-change")
def get_price_change(days: int = Query(7, ge=2, le=30)):
    conn = get_conn()
    try:
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        df = qdf(conn, BASE_Q + " WHERE dr.collected_date>=%s ORDER BY dr.collected_date", (cutoff,))
        if df.empty or df["collected_date"].nunique() < 2:
            return {"message": "최소 2일치 데이터가 필요합니다.", "items": []}
        dates = sorted(df["collected_date"].unique())
        first_d, last_d = dates[0], dates[-1]
        first = df[df["collected_date"] == first_d][["brand", "product_name", "final_price"]].copy()
        last = df[df["collected_date"] == last_d][["brand", "product_name", "final_price"]].copy()
        first.columns = ["brand", "product_name", "price_before"]
        last.columns = ["brand", "product_name", "price_after"]
        merged = pd.merge(first, last, on=["brand", "product_name"])
        merged["change"] = merged["price_after"] - merged["price_before"]
        merged["change_pct"] = round(merged["change"] / merged["price_before"] * 100, 1)
        merged = merged[merged["change"] != 0].sort_values("change")
        def to_list(d):
            return [{"brand": r["brand"], "product_name": r["product_name"],
                     "price_before": int(r["price_before"]), "price_after": int(r["price_after"]),
                     "change": int(r["change"]), "change_pct": float(r["change_pct"])}
                    for _, r in d.iterrows()]
        return {
            "compare_period": f"{first_d} → {last_d}",
            "price_down": to_list(merged[merged["change"] < 0].head(10)),
            "price_up": to_list(merged[merged["change"] > 0].head(10)),
        }
    finally:
        conn.close()
