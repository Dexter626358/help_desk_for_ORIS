"""Скрипт для добавления полей ФИО в существующую базу данных."""

from ipsas.web.app import create_app
from ipsas.database import db
from sqlalchemy import text


def migrate_database():
    """Добавление полей ФИО в таблицу users."""
    app = create_app()
    
    with app.app_context():
        try:
            # Проверяем, существуют ли уже колонки
            inspector = db.inspect(db.engine)
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            if 'last_name' not in columns:
                print("Добавление поля last_name...")
                db.session.execute(text("ALTER TABLE users ADD COLUMN last_name VARCHAR(100)"))
                db.session.commit()
                print("✓ Поле last_name добавлено")
            else:
                print("✓ Поле last_name уже существует")
            
            if 'first_name' not in columns:
                print("Добавление поля first_name...")
                db.session.execute(text("ALTER TABLE users ADD COLUMN first_name VARCHAR(100)"))
                db.session.commit()
                print("✓ Поле first_name добавлено")
            else:
                print("✓ Поле first_name уже существует")
            
            if 'middle_name' not in columns:
                print("Добавление поля middle_name...")
                db.session.execute(text("ALTER TABLE users ADD COLUMN middle_name VARCHAR(100)"))
                db.session.commit()
                print("✓ Поле middle_name добавлено")
            else:
                print("✓ Поле middle_name уже существует")
            
            print("\nМиграция завершена успешно!")
            
        except Exception as e:
            print(f"Ошибка при миграции: {e}")
            print("\nЕсли миграция не удалась, можно пересоздать базу данных:")
            print("1. Остановите сервер (Ctrl+C)")
            print("2. Удалите файл ipsas.db")
            print("3. Запустите сервер снова - база данных создастся автоматически")


if __name__ == "__main__":
    migrate_database()

