from setuptools import setup, find_packages


setup(
    name="polars_demo",
    version="0.1",
    packages=find_packages(),
    author="James Duguid",
    python_requires=">=3.10",
    install_requires=[
        "pandas",
        "polars",
        "ipython",
    ],
)

