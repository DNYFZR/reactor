from setuptools import setup, find_packages

data = dict(
    name="reactor",
    version="0.1",
    packages=find_packages(include=['src*']),
)

if __name__ == '__main__':
    setup(**data)