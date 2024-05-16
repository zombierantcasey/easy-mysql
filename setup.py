from setuptools import setup

setup(
    name="easymysql",
    version="1.0.10",
    author="Pierre MacKay",
    author_email="pierre@propdata.com",
    description="A simple wrapper for mysql-connector-python.",
    url="https://github.com/zombierantcasey/easy-mysql",
    packages=["easymysql"],
    install_requires=[
        "mysql-connector-python",
    ],
)
