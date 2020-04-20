import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

# TODO: move to poetry to maintain a single file and not setup.py and Pipfile both
# TODO track issues for better solution:
# TODO pipenv: https://github.com/pypa/pipenv/issues/1263
# TODO poetry: https://github.com/python-poetry/poetry/issues/1135
# TODO pyproject.toml: https://snarky.ca/clarifying-pep-518/

setuptools.setup(
    name="kgai_py_commons",
    version="0.0.1",
    author="Ritaja Sengupta",
    author_email="ritaja.sengupta90@gmail.com",
    description="Common Py utils for the KGIO project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=["pyaml", "confluent-kafka[avro]", "dataclasses-avroschema", "dacite"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
