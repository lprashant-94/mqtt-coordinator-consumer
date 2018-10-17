import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mqtt_coordinated",
    version="0.0.2",
    author="Prashant Lokhande",
    author_email="lprashant94@gmail.com",
    description="Coordinated Producer Consumer for MQTT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lprashant-94/mqtt-coordinator-consumer",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ),
)
