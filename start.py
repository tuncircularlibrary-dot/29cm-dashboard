"""
Railway 시작 파일
스케줄러(크롤러) + FastAPI 서버를 동시에 실행
"""
import threading
import subprocess
import schedule
import time
import logging
import os
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("start")

def run_crawl():
    log.info("=" * 50)
    log.info("일일 크롤링 시작")
    log.info("=" * 50)
    try:
        from crawler.crawler import Crawler29CM
        from db_setup import save_to_db

        crawler = Crawler29CM(headless=True)
        products = crawler.run()

        if not products:
            log.error("수집된 상품 없음")
            return

        log.info(f"{len(products)}개 상품 수집 완료")
        count = save_to_db(products)
        log.info(f"DB 저장 완료: {count}개")

    except Exception as e:
        log.error(f"크롤링 오류: {e}", exc_info=True)

def run_scheduler():
    run_time = os.getenv("CRAWL_TIME", "09:00")
    schedule.every().day.at(run_time).do(run_crawl)
    log.info(f"스케줄러 시작: 매일 {run_time} 자동 크롤링")

    # 서버 시작 시 즉시 1회 실행 (최초 데이터 수집)
    if os.getenv("RUN_ON_START", "true").lower() == "true":
        log.info("최초 실행: 지금 바로 크롤링 시작")
        threading.Thread(target=run_crawl, daemon=True).start()

    while True:
        schedule.run_pending()
        time.sleep(60)

def run_api():
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    log.info(f"API 서버 시작: port {port}")
    uvicorn.run("api.main:app", host="0.0.0.0", port=port)

if __name__ == "__main__":
    # 스케줄러를 백그라운드 스레드로 실행
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()

    # API 서버를 메인 스레드에서 실행
    run_api()
