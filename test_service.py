import requests
import json
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_service.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Конфигурация
SERVICE_URL = "http://localhost:8010"

def test_user_without_personal_recommendations():
    """Тест 1: Пользователь без персональных рекомендаций"""
    logger.info("=== Тест 1: Пользователь без персональных рекомендаций ===")
    
    test_user = "non_existent_user_12345"
    
    request_data = {
        "user_id": test_user,
        "online_history": [],
        "n_recommendations": 5
    }
    
    try:
        response = requests.post(
            f"{SERVICE_URL}/recommend",
            json=request_data,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f" Успех! Получено {len(result['recommendations'])} рекомендаций")
            logger.info(f"   Стратегия: {result['strategy']}")
            logger.info(f"   Рекомендации: {result['recommendations']}")
            return True
        else:
            logger.error(f" Ошибка: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Исключение: {e}")
        return False

def test_user_with_personal_no_online_history():
    """Тест 2: Пользователь с персональными рекомендациями, но без онлайн-истории"""
    logger.info("=== Тест 2: Пользователь с персональными рекомендациями, но без онлайн-истории ===")
    
    # Берем реального пользователя из наших данных
    import pandas as pd
    try:
        personal_recs = pd.read_parquet('personal_als.parquet')
        if len(personal_recs) > 0:
            test_user = personal_recs.iloc[0]['user_id']
        else:
            logger.warning("Нет данных персональных рекомендаций, используем тестового пользователя")
            test_user = "test_user_with_recs"
    except:
        test_user = "test_user_with_recs"
    
    request_data = {
        "user_id": test_user,
        "online_history": [],
        "n_recommendations": 5
    }
    
    try:
        response = requests.post(
            f"{SERVICE_URL}/recommend",
            json=request_data,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Успех! Получено {len(result['recommendations'])} рекомендаций")
            logger.info(f"   Стратегия: {result['strategy']}")
            logger.info(f"   Рекомендации: {result['recommendations'][:3]}...")  # Показываем первые 3
            return True
        else:
            logger.error(f"Ошибка: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Исключение: {e}")
        return False

def test_user_with_personal_and_online_history():
    """Тест 3: Пользователь с персональными рекомендациями и онлайн-историей"""
    logger.info("=== Тест 3: Пользователь с персональными рекомендациями и онлайн-историей ===")
    
    # Берем реального пользователя
    import pandas as pd
    try:
        personal_recs = pd.read_parquet('personal_als.parquet')
        if len(personal_recs) > 0:
            test_user = personal_recs.iloc[0]['user_id']
            # Берем несколько треков для онлайн-истории
            online_history = personal_recs['track_id'].head(3).tolist()
        else:
            logger.warning("Нет данных персональных рекомендаций, используем тестовые данные")
            test_user = "test_user_with_recs"
            online_history = ["track_1", "track_2", "track_3"]
    except:
        test_user = "test_user_with_recs"
        online_history = ["track_1", "track_2", "track_3"]
    
    request_data = {
        "user_id": test_user,
        "online_history": online_history,
        "n_recommendations": 5
    }
    
    try:
        response = requests.post(
            f"{SERVICE_URL}/recommend",
            json=request_data,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Успех! Получено {len(result['recommendations'])} рекомендаций")
            logger.info(f"   Стратегия: {result['strategy']}")
            logger.info(f"   Онлайн-история: {online_history}")
            logger.info(f"   Рекомендации: {result['recommendations']}")
            return True
        else:
            logger.error(f" Ошибка: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f" Исключение: {e}")
        return False

def health_check():
    """Проверка доступности сервиса"""
    logger.info("=== Проверка здоровья сервиса ===")
    
    try:
        response = requests.get(f"{SERVICE_URL}/health", timeout=5)
        if response.status_code == 200:
            logger.info(" Сервис доступен")
            return True
        else:
            logger.error(f"Сервис недоступен: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f" Сервис недоступен: {e}")
        return False

def run_all_tests():
    """Запуск всех тестов"""
    logger.info("Запуск тестирования рекомендательного сервиса")
    logger.info(f"Время начала: {datetime.now().isoformat()}")
    logger.info("=" * 60)
    
    # Проверяем доступность сервиса
    if not health_check():
        logger.error("Сервис недоступен! Запустите сервис перед тестированием.")
        return False
    
    results = []
    
    # Запускаем тесты
    results.append(("Health Check", health_check()))
    results.append(("User without personal recommendations", 
                   test_user_without_personal_recommendations()))
    results.append(("User with personal, no online history", 
                   test_user_with_personal_no_online_history()))
    results.append(("User with personal and online history", 
                   test_user_with_personal_and_online_history()))
    
    # Вывод итогов
    logger.info("=" * 60)
    logger.info("ИТОГИ ТЕСТИРОВАНИЯ:")
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    for test_name, success in results:
        status = "ПРОЙДЕН" if success else " ПРОВАЛЕН"
        logger.info(f"  {test_name}: {status}")
    
    logger.info(f"Успешно пройдено: {success_count}/{total_count} тестов")
    
    if success_count == total_count:
        logger.info("Все тесты пройдены успешно!")
    else:
        logger.warning(" Некоторые тесты не пройдены")
    
    return success_count == total_count

if __name__ == "__main__":
    run_all_tests()