#!/usr/bin/env python
import setuptools

requirements = ["apache-airflow", "requests", "geojson"]

setuptools.setup(
    name="airflow_insideairbnb",
    version="0.0.1",
    description="Airflow DAGs for Inside Airbnb with custom hookd, iperators and sensors.",
    author="LCS",
    author_email="lcs@gmail.com",
    install_requires=requirements,
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/laszlocsunderlik/insideairbnb",
    license="MIT",
)
