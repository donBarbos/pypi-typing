# Typing Status of PyPI Packages

[![Kaggle](https://img.shields.io/badge/Kaggle-035a7d?logo=kaggle&logoColor=white)](https://www.kaggle.com/datasets/donbarbos/pypi-typing)
[![GitHub Release](https://img.shields.io/github/v/release/donbarbos/pypi-typing)](https://github.com/donBarbos/pypi-typing/releases/)

## About

The initial idea behind this dataset was to explore type coverage among the most widely used libraries on PyPI, and to help systematize adding such stubs to [typeshed](https://github.com/python/typeshed/).

For parsing, I adapted the core logic from typeshed’s [stubsabot](https://github.com/python/typeshed/blob/main/scripts/stubsabot.py)..

The list of the most popular PyPI packages was originally taken from the [top-pypi-packages](https://hugovk.github.io/top-pypi-packages/) dump, thanks to [Hugo van Kemenade](https://github.com/hugovk).

### Schema of `pypi-packages-typing.csv`

| Column            | Type        | Description                                                                 |
|-------------------|-------------|-----------------------------------------------------------------------------|
| `package`         | string      | The name of the PyPI project.                                               |
| `has_py_typed`    | boolean     | `True` if the package bundles inline type hints (includes a `py.typed` file), otherwise `False`. |
| `has_types_package` | boolean / null | Indicates whether a `types-<package>` stub package exists on PyPI. |
