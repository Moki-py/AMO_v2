"""
Setup file for AmoCRM Data Exporter
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="amocrm-exporter",
    version="2.0.0",
    author="AmoCRM Exporter Team",
    description="Система экспорта данных из AmoCRM с веб-интерфейсом",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-repo/amocrm-exporter",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "amocrm-exporter=amocrm_exporter.cli:main",
            "amocrm-web=amocrm_exporter.cli:web_main",
            "amocrm-worker=amocrm_exporter.cli:worker_main",
        ],
    },
    include_package_data=True,
    package_data={
        "amocrm_exporter.web": ["templates/*.html"],
    },
)