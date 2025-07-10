from pymongo import MongoClient, DESCENDING
import logging
import datetime
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_handler")

class MongoDBHandler:
    def __init__(self):
        # MongoDB 연결 문자열 환경변수에서 로드
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        db_name = os.getenv("DB_NAME", "cnu_chatbot_db")
        
        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client[db_name]
            logger.info(f"Connected to MongoDB: {db_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
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
                                    "last_updated": datetime.datetime.now()
                                }
                            }
                        )
                        updated_count += 1
                        logger.info(f"Updated post: {post['title']}")
                else:
                    # 새 게시물 추가
                    post["created_at"] = datetime.datetime.now()
                    post["last_updated"] = datetime.datetime.now()
                    
                    collection.insert_one(post)
                    stored_count += 1
                    logger.info(f"Stored new post: {post['title']}")
            
            logger.info(f"Database update completed. {stored_count} new posts stored, {updated_count} posts updated")
            return {"stored": stored_count, "updated": updated_count}
        except Exception as e:
            logger.error(f"Error storing posts: {e}")
            return {"stored": 0, "updated": 0, "error": str(e)}
    
    def get_recent_posts(self, limit=10):
        """최근 채용공고 가져오기"""
        collection = self.db["recruit_posts"]
        posts = list(collection.find().sort("date", DESCENDING).limit(limit))
        return posts
    
    def search_posts(self, query, limit=10):
        """게시물 검색"""
        collection = self.db["recruit_posts"]
        
        # 간단한 텍스트 검색
        results = list(collection.find({
            "$or": [
                {"title": {"$regex": query, "$options": "i"}},
                {"content": {"$regex": query, "$options": "i"}}
            ]
        }).limit(limit))
        
        return results
    
    def close(self):
        """MongoDB 연결 닫기"""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("MongoDB connection closed")