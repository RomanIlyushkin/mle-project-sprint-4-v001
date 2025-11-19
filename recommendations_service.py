from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
from typing import List, Optional
import logging
import boto3
import os
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Music Recommendation Service",
    description="API для выдачи персонализированных рекомендаций треков",
    version="1.0.0"
)

# Модели данных
class RecommendationRequest(BaseModel):
    user_id: str
    online_history: Optional[List[str]] = []
    n_recommendations: int = 10

class RecommendationResponse(BaseModel):
    user_id: str
    recommendations: List[str]  # Должны быть строками!
    strategy: str
    timestamp: str

# Загрузка данных
def load_recommendation_data():
    """Загружает данные рекомендаций"""
    try:
        # Загрузка из локальных файлов
        personal_recs = pd.read_parquet('personal_als.parquet')
        similar_tracks = pd.read_parquet('similar.parquet')
        top_popular = pd.read_parquet('top_popular.parquet')
        
        # Преобразуем track_id в строки при загрузке
        personal_recs['track_id'] = personal_recs['track_id'].astype(str)
        similar_tracks['track_id'] = similar_tracks['track_id'].astype(str)
        similar_tracks['similar_track_id'] = similar_tracks['similar_track_id'].astype(str)
        top_popular['track_id'] = top_popular['track_id'].astype(str)
        
        logger.info("Данные рекомендаций загружены успешно")
        return personal_recs, similar_tracks, top_popular
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")
        # Создаем пустые датафреймы если файлы не найдены
        return (
            pd.DataFrame(columns=['user_id', 'track_id', 'score']),
            pd.DataFrame(columns=['track_id', 'similar_track_id', 'similarity_score']),
            pd.DataFrame(columns=['track_id', 'popularity_score'])
        )

# Инициализация данных
personal_recs, similar_tracks, top_popular = load_recommendation_data()

class RecommendationEngine:
    def __init__(self, personal_recs, similar_tracks, top_popular):
        self.personal_recs = personal_recs
        self.similar_tracks = similar_tracks
        self.top_popular = top_popular
        
    def get_personal_recommendations(self, user_id: str, n: int = 10) -> List[str]:
        """Получает персональные рекомендации для пользователя"""
        user_recs = self.personal_recs[
            self.personal_recs['user_id'] == user_id
        ].sort_values('score', ascending=False)
        
        if len(user_recs) == 0:
            return []
            
        return user_recs.head(n)['track_id'].tolist()
    
    def get_similar_tracks(self, track_ids: List[str], n_per_track: int = 3) -> List[str]:
        """Получает похожие треки на основе истории прослушиваний"""
        if not track_ids:
            return []
            
        similar_list = []
        for track_id in track_ids:
            track_similar = self.similar_tracks[
                self.similar_tracks['track_id'] == track_id
            ].sort_values('similarity_score', ascending=False)
            
            if len(track_similar) > 0:
                similar_list.extend(
                    track_similar.head(n_per_track)['similar_track_id'].tolist()
                )
        
        return similar_list
    
    def get_top_popular(self, n: int = 10) -> List[str]:
        """Получает топ популярных треков"""
        return self.top_popular.head(n)['track_id'].tolist()
    
    def mix_recommendations(
        self, 
        user_id: str, 
        online_history: List[str], 
        n_recommendations: int = 10
    ) -> tuple[List[str], str]:
        """
        Смешивает онлайн и офлайн рекомендации
        """
        
        # 1. Персональные рекомендации (офлайн)
        personal = self.get_personal_recommendations(user_id, n_recommendations)
        
        # 2. Похожие треки на основе онлайн-истории
        similar_online = self.get_similar_tracks(online_history, n_per_track=2)
        
        # 3. Топ популярные (фолбэк)
        top_popular = self.get_top_popular(n_recommendations * 2)
        
        # Объединяем все рекомендации
        all_recommendations = []
        
        # Приоритет 1: Персональные рекомендации
        all_recommendations.extend(personal)
        
        # Приоритет 2: Похожие на онлайн-историю (если есть)
        if online_history:
            all_recommendations.extend(similar_online)
        
        # Приоритет 3: Топ популярные (если не хватает)
        if len(all_recommendations) < n_recommendations:
            additional = [track for track in top_popular 
                         if track not in all_recommendations]
            all_recommendations.extend(additional)
        
        # Убираем дубликаты, сохраняя порядок
        seen = set()
        unique_recommendations = []
        for track in all_recommendations:
            if track not in seen:
                seen.add(track)
                unique_recommendations.append(track)
        
        # Исключаем треки из истории прослушиваний
        history_set = set(online_history)
        final_recommendations = [
            track for track in unique_recommendations 
            if track not in history_set
        ]
        
        # Определяем стратегию
        if online_history and personal:
            strategy = "online_history + personal"
        elif online_history:
            strategy = "online_history + top_popular"
        elif personal:
            strategy = "personal_only"
        else:
            strategy = "top_popular_only"
        
        return final_recommendations[:n_recommendations], strategy

# Инициализация движка рекомендаций
engine = RecommendationEngine(personal_recs, similar_tracks, top_popular)

@app.get("/")
async def root():
    return {"message": "Music Recommendation Service"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/recommend", response_model=RecommendationResponse)
async def get_recommendations(request: RecommendationRequest):
    """Основной endpoint для получения рекомендаций"""
    try:
        logger.info(f"Получен запрос для user_id: {request.user_id}, "
                   f"online_history: {len(request.online_history)} треков")
        
        recommendations, strategy = engine.mix_recommendations(
            user_id=request.user_id,
            online_history=request.online_history,
            n_recommendations=request.n_recommendations
        )
        
        logger.info(f"Сгенерировано {len(recommendations)} рекомендаций "
                   f"по стратегии: {strategy}")
        
        return RecommendationResponse(
            user_id=request.user_id,
            recommendations=recommendations,
            strategy=strategy,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации рекомендаций: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)