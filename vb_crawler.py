import os
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin

import pytz
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from supabase import create_client, Client
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()

# --- 환경 변수 및 Supabase 설정 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Supabase 클라이언트 초기화
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase 클라이언트 초기화 성공")
except Exception as e:
    print(f"Supabase 클라이언트 초기화 실패: {e}")
    supabase = None

def crawl_venturebeat():
    """VentureBeat AI 카테고리 기사 목록을 크롤링합니다."""
    BASE_URL = 'https://venturebeat.com'
    CRAWL_URL = f'{BASE_URL}/category/ai'
    print("DEBUG: crawl_venturebeat 함수 시작")

    if not supabase:
        print("DEBUG: Supabase 클라이언트가 유효하지 않아 크롤링을 중단합니다.")
        return None

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(CRAWL_URL)

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article"))
            )
        except TimeoutException:
            print("DEBUG: 기사 목록 컨테이너 로드 시간 초과.")
            return None

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        articles = []
        utc = pytz.utc

        article_elements = soup.find_all('article')

        for article_el in article_elements:
            title_tag = article_el.find('h2')
            if not title_tag or not title_tag.find('a'):
                continue

            title = title_tag.get_text(strip=True)
            relative_link = title_tag.find('a')['href']
            absolute_link = urljoin(BASE_URL, relative_link)

            summary_tag = article_el.find('p')
            summary = summary_tag.get_text(strip=True) if summary_tag else ""

            date_tag = article_el.find('time')
            if date_tag and date_tag.has_attr('datetime'):
                date_str = date_tag['datetime']
                try:
                    article_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))

                    if article_date.tzinfo is None:
                        article_date = utc.localize(article_date)

                    articles.append({
                        'title': title,
                        'link': absolute_link,
                        'summary': re.sub(r'\s+', ' ', summary),
                        'published_at': article_date.isoformat(),
                        'source': 'VentureBeat'
                    })
                except ValueError as ve:
                    print(f"DEBUG: 날짜 파싱 오류: {date_str} - {ve}")
                    continue

        print(f"총 {len(articles)}개의 기사를 수집했습니다.")
        print("DEBUG: crawl_venturebeat 함수 종료 (성공)")
        return articles

    except Exception as e:
        print(f"DEBUG: 크롤링 중 오류 발생: {e}")
        return None
    finally:
        if driver:
            driver.quit()


def save_to_supabase(articles):
    """크롤링한 기사를 Supabase DB에 저장합니다."""
    if not articles:
        print("DB에 저장할 기사가 없습니다.")
        return

    if not supabase:
        print("Supabase 클라이언트가 유효하지 않아 저장을 건너뜁니다.")
        return

    # 크롤링한 기사들의 링크 목록
    crawled_links = {article['link'] for article in articles}

    try:
        # DB에 이미 저장된 기사들의 링크 조회
        existing_links_response = supabase.table('articles').select('link').in_('link', list(crawled_links)).execute()
        if existing_links_response.data:
            existing_links = {item['link'] for item in existing_links_response.data}
            print(f"DB에서 {len(existing_links)}개의 기존 기사 링크를 확인했습니다.")
        else:
            existing_links = set()
            print("DB에 일치하는 기존 기사가 없습니다.")

        # DB에 없는 새로운 기사만 필터링
        new_articles = [article for article in articles if article['link'] not in existing_links]

        if not new_articles:
            print("저장할 새로운 기사가 없습니다.")
            return

        print(f"{len(new_articles)}개의 새로운 기사를 Supabase DB에 저장을 시도합니다.")

        # 새로운 기사만 upsert
        response = supabase.table('articles').upsert(new_articles, on_conflict='link').execute()
        print(f"Supabase 저장 응답: {response}")
        if response.data:
            print(f"Supabase 저장 완료: {len(response.data)}개 행이 처리되었습니다.")
        else:
            # 이 경우는 보통 에러가 없으면 발생하지 않지만, 디버깅을 위해 남겨둡니다.
            print(f"Supabase에 데이터가 저장되지 않았습니다. 응답: {response}")

    except Exception as e:
        print(f"Supabase 처리 중 오류 발생: {e}")


if __name__ == "__main__":
    print("DEBUG: 메인 스크립트 시작")
    crawled_articles = crawl_venturebeat()

    if crawled_articles:
        print("DEBUG: Supabase 저장 함수 호출 전")
        save_to_supabase(crawled_articles)
        print("DEBUG: Supabase 저장 함수 호출 후")
    else:
        print("DEBUG: 크롤링된 기사가 없어 Supabase 저장을 건너뜁니다.")
    print("DEBUG: 메인 스크립트 종료")
