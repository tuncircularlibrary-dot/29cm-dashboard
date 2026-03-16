"""
29CM 크롤러 v2 — requests 방식 (Selenium 없음)
Chrome 없이 HTTP 요청으로 직접 데이터 수집
클라우드 서버에서도 안정적으로 동작
"""
import requests
import logging
import time
from datetime import date
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

TARGET_URL = (
    "https://www.29cm.co.kr/store/category/list"
    "?categoryLargeCode=268100100"
    "&categoryMediumCode=268200100"
    "&sort=POPULAR"
)

# 실제 브라우저처럼 보이는 헤더
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.29cm.co.kr/",
    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}

# 29CM Next.js 내부 API (앱이 실제로 사용하는 엔드포인트)
API_URLS = [
    # Next.js 데이터 API
    "https://www.29cm.co.kr/_next/data/",
    # 카테고리 상품 목록 API
    "https://api.29cm.co.kr/api/v3/store/category/list",
    # 대안 API
    "https://www.29cm.co.kr/api/category/items",
]


class Crawler29CM:

    def __init__(self, headless=True):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.today = str(date.today())

    def run(self):
        """메인 실행 — 여러 방법 순서대로 시도"""
        log.info("29CM 크롤링 시작 (requests 방식)")

        # 방법 1: Next.js 내부 JSON API 시도
        products = self._try_nextjs_api()
        if products:
            log.info(f"✅ Next.js API 방식 성공: {len(products)}개")
            return products

        # 방법 2: HTML 파싱 방식
        products = self._try_html_parse()
        if products:
            log.info(f"✅ HTML 파싱 방식 성공: {len(products)}개")
            return products

        log.error("❌ 모든 방법 실패")
        return []

    def _try_nextjs_api(self):
        """Next.js 빌드ID를 먼저 가져와서 내부 JSON API 호출"""
        try:
            log.info("Next.js API 방식 시도...")

            # 먼저 메인 페이지에서 buildId 추출
            resp = self.session.get("https://www.29cm.co.kr/", timeout=15)
            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")

            # __NEXT_DATA__ 에서 buildId 추출
            next_data = soup.find("script", id="__NEXT_DATA__")
            if not next_data:
                log.warning("__NEXT_DATA__ 없음")
                return []

            import json
            data = json.loads(next_data.string)
            build_id = data.get("buildId", "")

            if not build_id:
                return []

            log.info(f"buildId: {build_id}")

            # Next.js 내부 API 호출
            api_url = (
                f"https://www.29cm.co.kr/_next/data/{build_id}"
                f"/store/category/list.json"
                f"?categoryLargeCode=268100100"
                f"&categoryMediumCode=268200100"
                f"&sort=POPULAR"
            )

            resp2 = self.session.get(api_url, timeout=15)
            if resp2.status_code != 200:
                log.warning(f"Next.js API 응답: {resp2.status_code}")
                return []

            result = resp2.json()
            return self._parse_nextjs_data(result)

        except Exception as e:
            log.warning(f"Next.js API 실패: {e}")
            return []

    def _parse_nextjs_data(self, data):
        """Next.js JSON 데이터에서 상품 파싱"""
        try:
            products = []
            # 데이터 구조 탐색
            page_props = data.get("pageProps", {})
            items = (
                page_props.get("items") or
                page_props.get("products") or
                page_props.get("itemList") or
                page_props.get("data", {}).get("items") or
                []
            )

            for i, item in enumerate(items[:100]):
                p = self._extract_product(item, i + 1)
                if p:
                    products.append(p)

            return products
        except Exception as e:
            log.warning(f"Next.js 데이터 파싱 실패: {e}")
            return []

    def _try_html_parse(self):
        """HTML 직접 파싱 방식"""
        try:
            log.info("HTML 파싱 방식 시도...")

            # 세션 쿠키 설정을 위해 먼저 메인 페이지 방문
            self.session.get("https://www.29cm.co.kr/", timeout=10)
            time.sleep(1)

            resp = self.session.get(TARGET_URL, timeout=15)
            if resp.status_code != 200:
                log.warning(f"HTML 응답: {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")

            # __NEXT_DATA__ 에서 상품 데이터 추출
            import json
            next_data = soup.find("script", id="__NEXT_DATA__")
            if next_data:
                try:
                    data = json.loads(next_data.string)
                    products = self._deep_find_items(data)
                    if products:
                        return products
                except Exception:
                    pass

            # CSS 셀렉터로 상품 카드 직접 파싱
            return self._parse_product_cards(soup)

        except Exception as e:
            log.warning(f"HTML 파싱 실패: {e}")
            return []

    def _deep_find_items(self, data, depth=0):
        """JSON 데이터에서 상품 목록을 재귀적으로 탐색"""
        if depth > 6:
            return []

        if isinstance(data, list) and len(data) >= 5:
            # 상품 목록처럼 보이는지 확인
            if all(isinstance(x, dict) for x in data[:3]):
                first = data[0]
                if any(k in first for k in ["itemNo", "frontItemNo", "itemName", "item_no"]):
                    products = []
                    for i, item in enumerate(data[:100]):
                        p = self._extract_product(item, i + 1)
                        if p:
                            products.append(p)
                    if products:
                        return products

        if isinstance(data, dict):
            for v in data.values():
                result = self._deep_find_items(v, depth + 1)
                if result:
                    return result

        return []

    def _extract_product(self, item, rank):
        """다양한 키 이름에서 상품 정보 추출"""
        try:
            # 상품명
            name = (
                item.get("itemName") or item.get("item_name") or
                item.get("name") or item.get("productName") or ""
            )
            # 브랜드
            brand = (
                item.get("frontBrandNameKo") or item.get("brandName") or
                item.get("brand") or item.get("brandNameKo") or "Unknown"
            )
            # 가격
            original = int(item.get("consumerPrice") or item.get("originalPrice") or
                          item.get("price") or item.get("retailPrice") or 0)
            final = int(item.get("salePrice") or item.get("finalPrice") or
                       item.get("discountPrice") or original or 0)

            if final == 0 and original > 0:
                final = original

            discount = 0
            if original > 0 and final < original:
                discount = round((original - final) / original * 100)

            # 상품 URL
            item_no = (item.get("itemNo") or item.get("frontItemNo") or
                      item.get("id") or item.get("productId") or "")
            url = f"https://www.29cm.co.kr/product/{item_no}" if item_no else ""

            if not name:
                return None

            return {
                "rank": rank,
                "brand": str(brand).strip(),
                "product_name": str(name).strip(),
                "original_price": original,
                "final_price": final,
                "discount_rate": discount,
                "tag_exclusive": bool(item.get("exclusive") or item.get("isExclusive")),
                "tag_new": bool(item.get("new") or item.get("isNew") or item.get("newYn")),
                "tag_coupon": bool(item.get("coupon") or item.get("hasCoupon")),
                "tag_free_shipping": bool(item.get("freeDelivery") or item.get("freeShipping")),
                "product_url": url,
                "collected_date": self.today,
            }
        except Exception:
            return None

    def _parse_product_cards(self, soup):
        """HTML 카드에서 직접 파싱 (fallback)"""
        products = []
        selectors = [
            "[class*='ProductCard']",
            "[class*='product-card']",
            "[class*='item-card']",
            "li[class*='item']",
        ]

        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if len(cards) >= 5:
                break

        for i, card in enumerate(cards[:100]):
            try:
                name_el = card.select_one(
                    "[class*='name'], [class*='Name'], [class*='title'], h3, h4"
                )
                brand_el = card.select_one(
                    "[class*='brand'], [class*='Brand']"
                )
                price_el = card.select_one(
                    "[class*='sale'], [class*='Sale'], [class*='price'], [class*='Price']"
                )

                name = name_el.get_text(strip=True) if name_el else ""
                brand = brand_el.get_text(strip=True) if brand_el else "Unknown"
                price_text = price_el.get_text(strip=True) if price_el else "0"
                price = int("".join(filter(str.isdigit, price_text)) or 0)

                if name:
                    products.append({
                        "rank": i + 1,
                        "brand": brand,
                        "product_name": name,
                        "original_price": price,
                        "final_price": price,
                        "discount_rate": 0,
                        "tag_exclusive": False,
                        "tag_new": False,
                        "tag_coupon": False,
                        "tag_free_shipping": False,
                        "product_url": "",
                        "collected_date": self.today,
                    })
            except Exception:
                continue

        return products
