from setuptools import setup, find_packages

setup(
	name="mfm_learner",
	version="1.0",
	description="mfm_learner",
	author="piginzoo",
	author_email="piginzoo@qq.com",
	url="https://github.com/piginzoo/mfm_learner.git",
	license="LGPL",
	packages=find_packages(where=".", exclude=('test','test.*','conf'), include=('*',))
)