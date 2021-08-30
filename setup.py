from setuptools import setup


def long_description():
    from os import path

    this_directory = path.abspath(path.dirname(__file__))
    with open(path.join(this_directory, "README.md")) as readme_f:
        contents = readme_f.read()
        return contents


setup(
    name="omen2",
    version="1.0.7",
    description="Database object & cache manager",
    packages=["omen2"],
    long_description=long_description(),
    long_description_content_type="text/markdown",
    setup_requires=["wheel"],
    install_requires=[
        "notanorm",
    ],
    entry_points={"console_scripts": ["omen2-codegen=omen2.codegen:main"]},
)
