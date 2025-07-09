from pymongo import MongoClient, DESCENDING, UpdateOne
import logging
import datetime
import os
import sys
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("db_handler")

class MongoDBHandler:
    def __init__(self):
        # MongoDB 연결 문자열 환경변수에서 로드, 없으면 기본값 사용
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        db_name = os.getenv("DB_NAME", "cnu_chatbot_db")
        
        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client[db_name]
            logger.info(f"Connected to MongoDB: {db_name}")
            
            # 인덱스 생성
            self._create_indexes()
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """필요한 인덱스 생성"""
        try:
            # post_num 필드에 유니크 인덱스 생성
            self.db["recruit_posts"].create_index([("post_num", 1)], unique=True)
            # 날짜 필드에 인덱스 생성 (내림차순)
            self.db["recruit_posts"].create_index([("date", DESCENDING)])
            # 제목 필드에 텍스트 인덱스 생성 (검색 성능 향상)
            self.db["recruit_posts"].create_index([("title", "text"), ("content", "text")])
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")
    
    def store_posts(self, posts):
        """채용공고 게시물을 데이터베이스에 저장"""
        if not posts:
            logger.warning("No posts to store")
            return {"stored": 0, "updated": 0}
            
        collection = self.db["recruit_posts"]
        
        stored_count = 0
        updated_count = 0
        
        try:
            for post in posts:
                # 이미 존재하는 게시물인지 확인
                existing = collection.find_one({"post_num": post["post_num"]})
                
                if existing:
                    # 내용이 업데이트되었는지 확인
                    if existing.get("content") != post.get("content") or existing.get("title") != post.get("title"):
                        collection.update_one(
                            {"post_num": post["post_num"]},
                            {
                                "$set": {
                                    "title": post["title"],
                                    "content": post["content"],
                                    "attachments": post.get("attachments", []),
                                    "images": post.get("images", []),
                                    "detailed_info": post.get("detailed_info", {}),
                                    "last_updated": datetime.datetime.now(),
                                    "embedding_updated": False  # 내용 변경으로 임베딩 갱신 필요
                                }
                            }
                        )
                        updated_count += 1
                        logger.info(f"Updated post: {post['title']}")
                else:
                    # 새 게시물 추가
                    post["created_at"] = datetime.datetime.now()
                    post["last_updated"] = datetime.datetime.now()
                    post["embedding_updated"] = False  # 임베딩 상태 추적
                    
                    collection.insert_one(post)
                    stored_count += 1
                    logger.info(f"Stored new post: {post['title']}")
            
            logger.info(f"Database update completed. {stored_count} new posts stored, {updated_count} posts updated")
            return {"stored": stored_count, "updated": updated_count}
        except Exception as e:
            logger.error(f"Error storing posts: {e}")
            return {"stored": 0, "updated": 0, "error": str(e)}
    
    def bulk_store_posts(self, posts):
        """대량의 게시물을 효율적으로 데이터베이스에 저장 (벌크 연산)"""
        if not posts:
            logger.warning("No posts to store in bulk")
            return {"stored": 0, "updated": 0}
            
        collection = self.db["recruit_posts"]
        operations = []
        
        try:
            # 각 게시물에 대해 upsert 연산 생성
            now = datetime.datetime.now()
            
            for post in posts:
                post_num = post["post_num"]
                
                # 업데이트할 데이터 준비
                update_data = {
                    "title": post["title"],
                    "writer": post["writer"],
                    "date": post["date"],
                    "views": post["views"],
                    "link": post["link"],
                    "last_updated": now,
                    "embedding_updated": False
                }
                
                # 선택적 필드 추가
                if "content" in post and post["content"]:
                    update_data["content"] = post["content"]
                if "attachments" in post:
                    update_data["attachments"] = post["attachments"]
                if "images" in post:
                    update_data["images"] = post["images"]
                if "detailed_info" in post:
                    update_data["detailed_info"] = post["detailed_info"]
                
                # upsert 연산 추가
                operations.append(
                    UpdateOne(
                        {"post_num": post_num},
                        {
                            "$set": update_data,
                            "$setOnInsert": {"created_at": now}  # 새 문서인 경우에만 생성 시간 설정
                        },
                        upsert=True
                    )
                )
            
            # 벌크 연산 실행
            if operations:
                result = collection.bulk_write(operations)
                logger.info(f"Bulk operation completed: {result.upserted_count} inserted, {result.modified_count} modified")
                return {"stored": result.upserted_count, "updated": result.modified_count}
            else:
                return {"stored": 0, "updated": 0}
                
        except Exception as e:
            logger.error(f"Error in bulk store operation: {e}")
            return {"stored": 0, "updated": 0, "error": str(e)}
    
    def get_posts_for_embedding(self):
        """임베딩이 필요한 게시물 가져오기"""
        collection = self.db["recruit_posts"]
        posts = list(collection.find(
            {"$or": [{"embedding_updated": False}, {"embedding_updated": {"$exists": False}}]},
            {"_id": 1, "post_num": 1, "title": 1, "content": 1}
        ))
        logger.info(f"Found {len(posts)} posts that need embedding updates")
        return posts
    
    def mark_embedding_updated(self, post_ids):
        """임베딩 업데이트 완료 표시"""
        if not post_ids:
            return 0
            
        collection = self.db["recruit_posts"]
        result = collection.update_many(
            {"_id": {"$in": post_ids}},
            {"$set": {"embedding_updated": True, "embedding_date": datetime.datetime.now()}}
        )
        logger.info(f"Marked {result.modified_count} posts as embedding updated")
        return result.modified_count
    
    def get_recent_posts(self, limit=10):
        """최근 채용공고 가져오기"""
        collection = self.db["recruit_posts"]
        posts = list(collection.find().sort("date", DESCENDING).limit(limit))
        return posts
    
    def search_posts(self, query, limit=10):
        """게시물 검색"""
        collection = self.db["recruit_posts"]
        
        # 텍스트 검색 (인덱스 필요)
        results = list(collection.find(
            {"$text": {"$search": query}},
            {"score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})]).limit(limit))
        
        return results
    
    def get_post_by_id(self, post_num):
        """게시물 번호로 단일 게시물 조회"""
        collection = self.db["recruit_posts"]
        return collection.find_one({"post_num": post_num})
    
    def close(self):
        """MongoDB 연결 닫기"""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("MongoDB connection closed")

# 테스트 코드
if __name__ == "__main__":
    # 연결 테스트
    db_handler = MongoDBHandler()
    
    # 테스트 데이터
    test_posts = [
        {
            "post_num": "test-001",
            "title": "테스트 게시물 1",
            "writer": "관리자",
            "date": "2023-10-01",
            "views": "10",
            "link": "https://example.com/test-001",
            "content": "이것은 테스트 게시물 내용입니다.",
        },
        {
            "post_num": "test-002",
            "title": "테스트 게시물 2",
            "writer": "관리자",
            "date": "2023-10-02",
            "views": "5",
            "link": "https://example.com/test-002",
            "content": "두 번째 테스트 게시물입니다.",
        }
    ]
    
    # 게시물 저장 테스트
    result = db_handler.store_posts(test_posts)
    print(f"저장 결과: {result}")
    
    # 검색 테스트
    search_results = db_handler.search_posts("테스트")
    print(f"검색 결과: {len(search_results)}개 게시물 찾음")
    for post in search_results:
        print(f" - {post['title']}")
    
    # 연결 종료
    db_handler.close()