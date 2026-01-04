"""Быстрое исправление базы данных - добавление недостающих полей."""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

from ipsas.web.app import create_app
from ipsas.database import db
from sqlalchemy import text, inspect

def fix_database():
    """Добавление недостающих полей в базу данных."""
    app = create_app()
    
    with app.app_context():
        try:
            inspector = inspect(db.engine)
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            print("Проверка полей в базе данных...")
            print(f"Найдено полей: {len(columns)}")
            
            # Добавление must_change_password
            if 'must_change_password' not in columns:
                print("\nДобавление поля must_change_password...")
                db.session.execute(text("ALTER TABLE users ADD COLUMN must_change_password BOOLEAN DEFAULT 0"))
                db.session.commit()
                print("✓ Поле must_change_password добавлено")
            else:
                print("✓ Поле must_change_password уже существует")
            
            # Добавление полей ФИО (если их нет)
            for field in ['last_name', 'first_name', 'middle_name']:
                if field not in columns:
                    print(f"\nДобавление поля {field}...")
                    db.session.execute(text(f"ALTER TABLE users ADD COLUMN {field} VARCHAR(100)"))
                    db.session.commit()
                    print(f"✓ Поле {field} добавлено")
                else:
                    print(f"✓ Поле {field} уже существует")
            
            print("\n" + "="*50)
            print("Миграция завершена успешно!")
            print("="*50)
            
        except Exception as e:
            print(f"\nОшибка при миграции: {e}")
            print("\nАльтернативный вариант:")
            print("1. Остановите сервер (Ctrl+C)")
            print("2. Удалите файл ipsas.db")
            print("3. Запустите сервер снова")
            return False
    
    return True

if __name__ == "__main__":
    success = fix_database()
    sys.exit(0 if success else 1)

