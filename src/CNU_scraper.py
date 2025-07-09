# CNU 채용공고 스크래퍼
# 이 스크립트는 충남대학교의 채용공고 페이지를 스크래핑하여
# 게시물 목록과 각 게시물의 상세 정보를 수집합니다.

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import logging
import os
import json

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import logging
import os
import json

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("cnu_scraper")

class CNURecruitScraper:
    def __init__(self):
        self.base_url = "https://cnuint.cnu.ac.kr/cnuint/notice/recruit.do"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def scrape_recruit_list(self, offset=0, limit=10):
        """채용공고 목록 스크래핑"""
        params = {
            'mode': 'list',
            'article.offset': offset,
            'articleLimit': limit
        }
        
        try:
            logger.info(f"Scraping recruit list with offset={offset}, limit={limit}")
            response = requests.get(self.base_url, params=params, headers=self.headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to retrieve page: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 게시물 목록 찾기
            table = soup.find('table', class_='board-table')
            if not table:
                logger.warning("Table not found in the response")
                return []
                
            posts = []
            rows = table.find_all('tr')[1:]  # 헤더 제외
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    post_num = cols[0].text.strip()
                    
                    # 제목과 링크 추출
                    title_col = cols[1]
                    title_a = title_col.find('a')
                    title = title_a.text.strip() if title_a else "제목 없음"
                    
                    # 상세 페이지 링크 추출
                    link = ""
                    if title_a:
                        # onclick 속성이 있는 경우
                        if 'onclick' in title_a.attrs:
                            onclick = title_a['onclick']
                            if 'fnView' in onclick:
                                article_id = onclick.split("'")[1]
                                link = f"{self.base_url}?mode=view&articleNo={article_id}"
                        # href 속성이 있는 경우
                        elif 'href' in title_a.attrs:
                            link = title_a['href']
                            if not link.startswith('http'):
                                link = f"https://cnuint.cnu.ac.kr{link}"
                    
                    writer = cols[2].text.strip()
                    date = cols[3].text.strip()
                    views = cols[4].text.strip()
                    
                    posts.append({
                        'post_num': post_num,
                        'title': title,
                        'writer': writer,
                        'date': date,
                        'views': views,
                        'link': link,
                        'content': None,  # 상세 내용은 별도 함수로 수집
                        'scrape_time': datetime.datetime.now().isoformat()
                    })
            
            logger.info(f"Successfully scraped {len(posts)} posts")
            return posts
            
        except Exception as e:
            logger.error(f"Error scraping recruit list: {e}")
            return []
    
    def scrape_post_detail(self, url):
        """게시물 상세 페이지 스크래핑"""
        try:
            logger.info(f"Scraping post detail from {url}")
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to retrieve post detail: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 게시물 제목
            title = soup.find('h2', class_='board-view-title')
            title_text = title.text.strip() if title else "제목 없음"
            
            # 게시물 정보 (작성자, 날짜 등)
            info_div = soup.find('div', class_='board-view-info')
            info = {}
            if info_div:
                info_items = info_div.find_all('dl')
                for item in info_items:
                    dt = item.find('dt')
                    dd = item.find('dd')
                    if dt and dd:
                        key = dt.text.strip()
                        value = dd.text.strip()
                        info[key] = value
            
            # 게시물 내용
            content_div = soup.find('div', class_='board-view-content')
            content_text = ""
            img_urls = []
            
            if content_div:
                # 이미지 및 첨부 파일 정보 포함
                images = content_div.find_all('img')
                for img in images:
                    if 'src' in img.attrs:
                        src = img['src']
                        if not src.startswith('http'):
                            src = f"https://cnuint.cnu.ac.kr{src}"
                        img_urls.append(src)
                
                # 텍스트 내용
                content_text = content_div.get_text(separator='\n', strip=True)
            
            # 첨부 파일
            attachments = []
            attach_div = soup.find('div', class_='board-attach')
            if attach_div:
                attach_links = attach_div.find_all('a')
                for link in attach_links:
                    if 'href' in link.attrs:
                        file_name = link.text.strip()
                        file_url = link['href']
                        if not file_url.startswith('http'):
                            file_url = f"https://cnuint.cnu.ac.kr{file_url}"
                        attachments.append({
                            'name': file_name,
                            'url': file_url
                        })
            
            post_detail = {
                'title': title_text,
                'info': info,
                'content': content_text,
                'images': img_urls,
                'attachments': attachments,
                'url': url,
                'scrape_time': datetime.datetime.now().isoformat()
            }
            
            logger.info(f"Successfully scraped post detail: {title_text}")
            return post_detail
            
        except Exception as e:
            logger.error(f"Error scraping post detail: {e}")
            return None
    
    def scrape_and_enrich_posts(self, offset=0, limit=10):
        """채용공고 목록을 가져와 각 게시물의 상세 정보로 보강"""
        posts = self.scrape_recruit_list(offset, limit)
        enriched_posts = []
        
        for post in posts:
            if post['link']:
                detail = self.scrape_post_detail(post['link'])
                if detail:
                    post.update({
                        'content': detail['content'],
                        'attachments': detail.get('attachments', []),
                        'images': detail.get('images', []),
                        'detailed_info': detail.get('info', {})
                    })
            enriched_posts.append(post)
            
        return enriched_posts
    
    def scrape_multiple_pages(self, pages=2, limit=10):
        """여러 페이지의 채용공고 스크래핑"""
        all_posts = []
        
        for page in range(pages):
            offset = page * limit
            logger.info(f"Scraping page {page+1} (offset={offset})")
            posts = self.scrape_and_enrich_posts(offset, limit)
            all_posts.extend(posts)
            
        return all_posts
    
    def save_to_json(self, posts, filename="cnu_recruit_posts.json"):
        """스크래핑한 게시물을 JSON 파일로 저장"""
        try:
            # data 디렉토리 생성 (없는 경우)
            os.makedirs("data", exist_ok=True)
            
            file_path = os.path.join("data", filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(posts, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Successfully saved {len(posts)} posts to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving posts to JSON: {e}")
            return False
    
    def save_to_csv(self, posts, filename="cnu_recruit_posts.csv"):
        """스크래핑한 게시물을 CSV 파일로 저장"""
        try:
            # data 디렉토리 생성 (없는 경우)
            os.makedirs("data", exist_ok=True)
            
            # 중첩된 필드 처리 (attachments, images 등)
            for post in posts:
                if 'attachments' in post:
                    post['attachment_count'] = len(post['attachments'])
                    post['attachment_names'] = ', '.join([a['name'] for a in post['attachments']])
                if 'images' in post:
                    post['image_count'] = len(post['images'])
            
            df = pd.DataFrame(posts)
            
            # 중첩 구조는 CSV에서 제외
            if 'attachments' in df.columns:
                df = df.drop(columns=['attachments'])
            if 'images' in df.columns:
                df = df.drop(columns=['images'])
            if 'detailed_info' in df.columns:
                df = df.drop(columns=['detailed_info'])
            
            file_path = os.path.join("data", filename)
            df.to_csv(file_path, index=False, encoding='utf-8-sig')  # Excel에서 한글 지원
            
            logger.info(f"Successfully saved {len(posts)} posts to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving posts to CSV: {e}")
            return False

# 테스트 코드
if __name__ == "__main__":
    # DB 핸들러 임포트
    import sys
    import os
    
    # 프로젝트 루트 추가하여 다른 모듈 임포트 가능하게
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    try:
        from src.database.db_handler import MongoDBHandler
        
        # 스크래퍼 및 DB 핸들러 초기화
        scraper = CNURecruitScraper()
        db_handler = MongoDBHandler()
        
        # 채용공고 스크래핑
        print("채용공고 스크래핑 시작...")
        posts = scraper.scrape_multiple_pages(pages=2)
        print(f"총 {len(posts)}개의 채용공고를 수집했습니다.")
        
        # MongoDB에 저장
        print("MongoDB에 데이터 저장 중...")
        result = db_handler.store_posts(posts)
        print(f"저장 결과: {result['stored']}개 추가, {result['updated']}개 업데이트")
        
        # 파일로도 저장 (백업)
        scraper.save_to_json(posts)
        scraper.save_to_csv(posts)
        
        # 연결 종료
        db_handler.close()
        
        print("\n작업 완료! 데이터가 MongoDB와 파일에 저장되었습니다.")
        
    except ImportError:
        print("DB 핸들러를 임포트할 수 없습니다. 파일로만 저장합니다.")
        scraper = CNURecruitScraper()
        posts = scraper.scrape_multiple_pages(pages=2)
        scraper.save_to_json(posts)
        scraper.save_to_csv(posts)
        print(f"총 {len(posts)}개의 채용공고를 수집하여 파일로 저장했습니다.")
        
    except Exception as e:
        print(f"오류 발생: {e}")