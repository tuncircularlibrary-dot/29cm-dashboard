"""
DB 설정 및 저장 — Supabase (PostgreSQL)
환경변수 DATABASE_URL 에서 연결 정보를 읽습니다.
"""
import os
import logging
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger("db")

def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("환경변수 DATABASE_URL이 설정되지 않았습니다.")
    return psycopg2.connect(url, sslmode="require")

def create_tables():
    """테이블이 없으면 생성 (멱등)"""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS products (
                        id SERIAL PRIMARY KEY,
                        brand TEXT NOT NULL,
                        product_name TEXT NOT NULL,
                        product_url TEXT,
                        UNIQUE (brand, product_name)
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_rankings (
                        id SERIAL PRIMARY KEY,
                        collected_date DATE NOT NULL,
                        rank INTEGER NOT NULL,
                        product_id INTEGER REFERENCES products(id),
                        original_price INTEGER DEFAULT 0,
                        final_price INTEGER DEFAULT 0,
                        discount_rate INTEGER DEFAULT 0,
                        tag_exclusive BOOLEAN DEFAULT FALSE,
                        tag_new BOOLEAN DEFAULT FALSE,
                        tag_coupon BOOLEAN DEFAULT FALSE,
                        tag_free_shipping BOOLEAN DEFAULT FALSE,
                        UNIQUE (collected_date, product_id)
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_date_rank ON daily_rankings (collected_date, rank);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_product_date ON daily_rankings (product_id, collected_date);")
        log.info("테이블 준비 완료")
    finally:
        conn.close()

def save_to_db(products: list) -> int:
    """
    크롤러에서 받은 상품 목록을 DB에 저장.
    이미 오늘 데이터가 있으면 업데이트(upsert).
    """
    if not products:
        return 0

    create_tables()
    conn = get_conn()
    saved = 0

    try:
        with conn:
            with conn.cursor() as cur:
                for p in products:
                    # 1. products 테이블 upsert
                    cur.execute("""
                        INSERT INTO products (brand, product_name, product_url)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand, product_name)
                        DO UPDATE SET product_url = EXCLUDED.product_url
                        RETURNING id
                    """, (p["brand"], p["product_name"], p.get("product_url", "")))
                    row = cur.fetchone()
                    if not row:
                        cur.execute(
                            "SELECT id FROM products WHERE brand=%s AND product_name=%s",
                            (p["brand"], p["product_name"])
                        )
                        row = cur.fetchone()
                    product_id = row[0]

                    # 2. daily_rankings upsert
                    cur.execute("""
                        INSERT INTO daily_rankings (
                            collected_date, rank, product_id,
                            original_price, final_price, discount_rate,
                            tag_exclusive, tag_new, tag_coupon, tag_free_shipping
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (collected_date, product_id)
                        DO UPDATE SET
                            rank = EXCLUDED.rank,
                            original_price = EXCLUDED.original_price,
                            final_price = EXCLUDED.final_price,
                            discount_rate = EXCLUDED.discount_rate,
                            tag_exclusive = EXCLUDED.tag_exclusive,
                            tag_new = EXCLUDED.tag_new,
                            tag_coupon = EXCLUDED.tag_coupon,
                            tag_free_shipping = EXCLUDED.tag_free_shipping
                    """, (
                        p.get("collected_date", str(date.today())),
                        p["rank"],
                        product_id,
                        p.get("original_price", 0),
                        p.get("final_price", 0),
                        p.get("discount_rate", 0),
                        p.get("tag_exclusive", False),
                        p.get("tag_new", False),
                        p.get("tag_coupon", False),
                        p.get("tag_free_shipping", False),
                    ))
                    saved += 1

        log.info(f"저장 완료: {saved}개")
        return saved

    except Exception as e:
        log.error(f"DB 저장 오류: {e}", exc_info=True)
        return 0
    finally:
        conn.close()
