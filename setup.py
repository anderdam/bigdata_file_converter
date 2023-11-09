import setuptools

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="bigdata_file_converter",
    version="1.0.0",
    description="Library for converting bigdata file formats",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    extras_require={"dev": ["pytest", "black", "mypy"]},
    python_requires=">=3.9",
    entry_points={"console_scripts": ["file-converter=file_converter.cli:main"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
)
