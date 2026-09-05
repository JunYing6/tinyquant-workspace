"""Data domain of the user project.

Three responsibilities, one pipeline direction:

- ``data.downloader``   vendor API -> raw parquet volume (E:/ProgramData layout)
- ``data.adapters``     raw volume -> standard ``tools.data`` gateway datasets
- ``data.processing``   canonical datasets -> derived datasets (indicators etc.)
"""

