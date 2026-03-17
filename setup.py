from setuptools import setup, find_packages

setup(
    name="runfox",
    use_scm_version={
        "write_to": "runfox/_version.py",
        "fallback_version": "0.0.0",
    },
    description="Orchestration and execution engine",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="Ed Grundy",
    author_email="ed@bayis.co.uk",
    url="https://github.com/bayinfosys/runfox",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=[
      "json-logic-path", "pyyaml"
    ],
    extras_require={
        "dev": ["pytest", "setuptools-scm", "pytest-cov", "mutmut"],
        "aws": ["boto3", "dynawrap"],
    },
)
