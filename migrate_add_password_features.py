"""Скрипт для добавления поля must_change_password в существующую базу данных."""

from ipsas.web.app import create_app
from ipsas.database import db
from sqlalchemy import text


def migrate_database():
    """Добавление поля must_change_password в таблицу users."""
    app = create_app()
    
    with app.app_context():
        try:
            # Проверяем, существуют ли уже колонки
            inspector = db.inspect(db.engine)
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            if 'must_change_password' not in columns:
                print("Добавление поля must_change_password...")
                db.session.execute(text("ALTER TABLE users ADD COLUMN must_change_password BOOLEAN DEFAULT 0"))
                db.session.commit()
                print("✓ Поле must_change_password добавлено")
            else:
                print("✓ Поле must_change_password уже существует")
            
            print("\nМиграция завершена успешно!")
            
        except Exception as e:
            print(f"Ошибка при миграции: {e}")
            print("\nЕсли миграция не удалась, можно пересоздать базу данных:")
            print("1. Остановите сервер (Ctrl+C)")
            print("2. Удалите файл ipsas.db")
            print("3. Запустите сервер снова - база данных создастся автоматически")


if __name__ == "__main__":
    migrate_database()

