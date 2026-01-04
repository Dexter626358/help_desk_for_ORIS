"""Setup файл для установки IPSAS."""

from setuptools import setup, find_packages
from pathlib import Path

# Чтение README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

setup(
    name="ipsas",
    version="0.1.0",
    author="IPSAS Team",
    description="Internal Publishing Support System - система для автоматической обработки данных и валидации",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/ipsas",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.10",
    install_requires=[
        # Базовые зависимости будут добавлены по мере необходимости
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.7.0",
            "flake8>=6.1.0",
            "mypy>=1.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ipsas=run:main",
        ],
    },
)

