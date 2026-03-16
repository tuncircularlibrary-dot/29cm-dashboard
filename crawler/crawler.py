"""
29CM 여성 가방 인기순위 크롤러 (실제 사이트 반영 버전)
=====================================
실제 확인된 29CM URL 구조:
  https://www.29cm.co.kr/store/category/list
    ?categoryLargeCode=268100100   ← 여성 카테고리
    &categoryMediumCode=268200100  ← 가방/잡화
    &sort=POPULAR                  ← 인기순

[동작 방식]
1. Selenium으로 Chrome 자동 실행
2. 팝업 닫기 → 무한 스크롤로 100개 로드
3. 상품 카드에서 데이터 파싱
4. JSON + SQLite DB 동시 저장
"""

import time
import json
import re
import logging
import sqlite3
import os
from datetime import date
from dataclasses import dataclass, asdict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─── 크롤링 대상 URL ───────────────────────────────────────
# 실제 29CM 여성 가방 카테고리 인기순
TARGET_URL = (
    "https://www.29cm.co.kr/store/category/list"
    "?categoryLargeCode=268100100"
    "&categoryMediumCode=268200100"
    "&sort=POPULAR"
)

# DB 경로 (이 파일 기준 상위 폴더)
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "rankings.db")


@dataclass
class ProductData:
    rank: int
    brand: str
    product_name: str
    original_price: int
    final_price: int
    discount_rate: int
    tag_exclusive: bool
    tag_new: bool
    tag_coupon: bool
    tag_free_shipping: bool
    collected_date: str
    product_url: str
    image_url: str = ""


class Crawler29CM:
    TARGET_COUNT = 100

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self.today = str(date.today())

    def _setup_driver(self):
        logger.info("🚀 Chrome 드라이버 초기화 중...")
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
        # Railway(Docker) 환경: 시스템 chromium 사용
        # 로컬 환경: ChromeDriverManager 자동 설치
        import shutil
        chromium_path = shutil.which("chromium") or shutil.which("chromium-browser")
        chromedriver_path = shutil.which("chromedriver")

        if chromium_path:
            options.binary_location = chromium_path
            service = Service(chromedriver_path or "/usr/bin/chromedriver")
        else:
            service = Service(ChromeDriverManager().install())

        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("✅ Chrome 드라이버 준비 완료")

    def _close_popups(self):
        """팝업/쿠키 배너 닫기"""
        time.sleep(2)
        for sel in [
            "button[class*='close']",
            "button[aria-label*='닫기']",
            "button[aria-label*='Close']",
            "[class*='modal'] button[class*='close']",
            "[class*='cookie'] button",
        ]:
            try:
                for btn in self.driver.find_elements(By.CSS_SELECTOR, sel):
                    if btn.is_displayed():
                        btn.click()
                        time.sleep(0.4)
            except Exception:
                pass

    def _get_cards(self):
        """29CM 상품 카드 요소 수집 (여러 선택자 시도)"""
        for sel in [
            "ul[class*='ProductList'] > li",
            "ul[class*='product_list'] > li",
            "div[class*='ProductList'] li",
            "[data-testid='product-card']",
            "[data-testid='product-item']",
            "li[class*='Product']",
            "li[class*='product']",
        ]:
            cards = self.driver.find_elements(By.CSS_SELECTOR, sel)
            if len(cards) >= 5:
                logger.info(f"  카드 선택자 사용: '{sel}' ({len(cards)}개)")
                return cards
        # fallback: img + 가격 텍스트 있는 li
        result = []
        for li in self.driver.find_elements(By.CSS_SELECTOR, "li"):
            try:
                if li.find_elements(By.TAG_NAME, "img") and "원" in li.text:
                    result.append(li)
            except Exception:
                pass
        return result

    def _scroll_to_load(self):
        logger.info("📜 스크롤로 상품 로드 중...")
        last_height = 0
        no_change = 0
        for i in range(30):
            count = len(self._get_cards())
            logger.info(f"  스크롤 {i+1}: {count}개 로드됨")
            if count >= self.TARGET_COUNT:
                break
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.0)
            h = self.driver.execute_script("return document.body.scrollHeight")
            if h == last_height:
                no_change += 1
                if no_change >= 3:
                    logger.warning(f"⚠️ 더 이상 로드 안 됨 ({count}개)")
                    break
            else:
                no_change = 0
            last_height = h

    @staticmethod
    def _to_int(text: str) -> int:
        nums = re.sub(r"[^\d]", "", text or "")
        return int(nums) if nums else 0

    def _parse_card(self, card, rank: int):
        try:
            text = card.text or ""

            # 브랜드명
            brand = ""
            for sel in ["[class*='brand']", "[class*='Brand']", "strong", "b"]:
                try:
                    el = card.find_element(By.CSS_SELECTOR, sel)
                    v = el.text.strip()
                    if v and len(v) < 60:
                        brand = v; break
                except NoSuchElementException:
                    pass

            # 상품명
            product_name = ""
            for sel in ["[class*='name']", "[class*='Name']", "[class*='title']", "p"]:
                try:
                    for el in card.find_elements(By.CSS_SELECTOR, sel):
                        v = el.text.strip()
                        if v and v != brand and 3 < len(v) < 200:
                            product_name = v; break
                    if product_name:
                        break
                except Exception:
                    pass

            # 가격
            prices = []
            for sel in ["[class*='price']", "[class*='Price']"]:
                for el in card.find_elements(By.CSS_SELECTOR, sel):
                    p = self._to_int(el.text)
                    if p > 0:
                        prices.append(p)
            if not prices:
                for m in re.findall(r"[\d,]{4,}", text):
                    p = self._to_int(m)
                    if p > 1000:
                        prices.append(p)

            prices = sorted(set(prices), reverse=True)
            original_price = prices[0] if prices else 0
            final_price = prices[-1] if len(prices) > 1 else original_price

            # 할인율
            discount_rate = 0
            try:
                disc_el = card.find_element(
                    By.CSS_SELECTOR,
                    "[class*='discount'], [class*='Discount'], [class*='rate'], [class*='percent']"
                )
                m = re.search(r"(\d+)%", disc_el.text)
                if m:
                    discount_rate = int(m.group(1))
            except NoSuchElementException:
                pass
            if discount_rate == 0 and original_price > final_price > 0:
                discount_rate = round((1 - final_price / original_price) * 100)

            # 태그
            tag_exclusive = any(k in text for k in ["단독", "EXCLUSIVE"])
            tag_new = any(k in text for k in ["신상품", "NEW", "New Arrival"])
            tag_coupon = any(k in text for k in ["쿠폰", "COUPON"])
            tag_free_shipping = any(k in text for k in ["무료배송", "무료 배송"])

            # URL / 이미지
            product_url = ""
            image_url = ""
            try:
                a = card.find_element(By.TAG_NAME, "a")
                href = a.get_attribute("href") or ""
                if "29cm.co.kr" in href:
                    product_url = href
            except NoSuchElementException:
                pass
            try:
                img = card.find_element(By.TAG_NAME, "img")
                image_url = img.get_attribute("src") or ""
            except NoSuchElementException:
                pass

            if final_price == 0 and not brand:
                return None

            return ProductData(
                rank=rank, brand=brand or "Unknown",
                product_name=product_name or "Unknown",
                original_price=original_price, final_price=final_price,
                discount_rate=discount_rate,
                tag_exclusive=tag_exclusive, tag_new=tag_new,
                tag_coupon=tag_coupon, tag_free_shipping=tag_free_shipping,
                collected_date=self.today, product_url=product_url,
                image_url=image_url,
            )
        except StaleElementReferenceException:
            return None
        except Exception as e:
            logger.error(f"  {rank}위 파싱 오류: {e}")
            return None

    def run(self) -> list[dict]:
        products = []
        try:
            self._setup_driver()
            logger.info(f"🌐 접속: {TARGET_URL}")
            self.driver.get(TARGET_URL)

            try:
                WebDriverWait(self.driver, 25).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "li, article, [class*='product'], [class*='Product']")
                    )
                )
            except TimeoutException:
                logger.error("❌ 페이지 로딩 타임아웃")
                logger.info(f"  URL: {self.driver.current_url}")
                return []

            time.sleep(3)
            self._close_popups()
            self._scroll_to_load()
            time.sleep(2)

            cards = self._get_cards()
            if not cards:
                logger.error("❌ 상품 카드 없음 — --show-browser 옵션으로 확인하세요")
                logger.debug(self.driver.page_source[:3000])
                return []

            logger.info(f"🔍 파싱 시작 ({len(cards)}개 카드 → 최대 {self.TARGET_COUNT}개)")
            rank = 1
            for card in cards[:self.TARGET_COUNT]:
                p = self._parse_card(card, rank)
                if p:
                    products.append(asdict(p))
                    rank += 1
                time.sleep(0.05)

            logger.info(f"✅ {len(products)}개 수집 완료!")

        except Exception as e:
            logger.error(f"❌ 크롤링 오류: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
        return products

    def save_to_json(self, products: list[dict], out_dir: str = ".") -> str:
        os.makedirs(out_dir, exist_ok=True)
        filename = os.path.join(out_dir, f"data_{self.today}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({
                "collected_date": self.today,
                "total_count": len(products),
                "source": "29CM 여성 가방 인기순",
                "url": TARGET_URL,
                "products": products,
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 JSON 저장: {filename}")
        return filename

    def save_to_db(self, products: list[dict]):
        import sys, tempfile
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from db_setup import create_tables, load_json_to_db
        conn = sqlite3.connect(DB_PATH)
        create_tables(conn)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json",
                                         delete=False, encoding="utf-8") as f:
            json.dump({"products": products, "collected_date": self.today}, f,
                      ensure_ascii=False)
            tmp = f.name
        count = load_json_to_db(conn, tmp)
        conn.close()
        os.unlink(tmp)
        logger.info(f"🗄️  DB 저장: {count}개")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--show-browser", action="store_true",
                        help="브라우저 창 보이기 (디버깅용)")
    parser.add_argument("--no-db", action="store_true", help="DB 저장 건너뜀")
    args = parser.parse_args()

    print("=" * 60)
    print("  29CM 여성 가방 인기순위 크롤러")
    print(f"  URL: {TARGET_URL}")
    print("=" * 60)

    crawler = Crawler29CM(headless=not args.show_browser)
    products = crawler.run()

    if products:
        saved = crawler.save_to_json(products)
        if not args.no_db:
            try:
                crawler.save_to_db(products)
            except Exception as e:
                logger.warning(f"DB 저장 실패 (JSON은 OK): {e}")

        print(f"\n📊 수집 결과 (상위 5개):")
        print("-" * 60)
        for p in products[:5]:
            tags = [k for k, v in {
                "단독": p["tag_exclusive"], "신상품": p["tag_new"],
                "쿠폰": p["tag_coupon"], "무료배송": p["tag_free_shipping"]
            }.items() if v]
            print(f"  {p['rank']:3}위 | {p['brand']:<15} | {p['final_price']:>10,}원 | {', '.join(tags) or '–'}")
        print(f"\n  → 총 {len(products)}개 수집 · {saved} 저장 완료!")
    else:
        print("\n❌ 수집 실패 — --show-browser 로 재실행해 확인하세요")
        print(f"   python crawler.py --show-browser")
