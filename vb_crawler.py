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
        now = datetime.now(utc)
        one_day_ago = now - timedelta(days=1)

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

                    if article_date > one_day_ago:
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

        print(f"총 {len(articles)}개의 새 기사를 찾았습니다.")
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
        print("DB에 저장할 새 기사가 없습니다.")
        return

    if not supabase:
        print("Supabase 클라이언트가 유효하지 않아 저장을 건너뜁니다.")
        return

    unique_articles = {article['link']: article for article in articles}
    articles_to_save = list(unique_articles.values())

    print(f"{len(articles_to_save)}개의 고유한 기사를 Supabase DB에 저장을 시도합니다.")

    try:
        response = supabase.table('articles').upsert(articles_to_save, on_conflict='link').execute()
        print(f"Supabase 저장 응답: {response}")
        if response.data:
            print(f"Supabase 저장 완료: {len(response.data)}개 행이 처리되었습니다.")
        else:
            print(f"Supabase에 데이터가 저장되지 않았습니다. 응답을 확인하세요.")

    except Exception as e:
        print(f"Supabase 저장 중 오류 발생: {e}")


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
