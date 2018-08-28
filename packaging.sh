# One Time setup
pip install --user --upgrade setuptools wheel
pip install --user --upgrade twine

# Everytime
python setup.py sdist bdist_wheel
twine upload dist/*
