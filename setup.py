#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-doris"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "0.3.5"
dbt_core_version = "1.9.8"
description = """The doris adapter plugin for dbt """

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="long2ice,catpineapple,JNSimba",
    author_email="1391869588@qq.com",
    url="https://github.com/selectdb/dbt-doris",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        f"dbt-core>={dbt_core_version}",
        "mysql-connector-python==9.3.0",
        "urllib3~=1.0",
    ],
    python_requires=">=3.7.2",
)
