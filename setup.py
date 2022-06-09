from setuptools import setup, find_packages
setup(
	name="mfm_learner",
	version="1.0",
	description="mfm_learner",
	author="Tian",
	author_email="piginzoo@qq.com",
	url="https://github.com/piginzoo/mfm_learner.git",
	license="LGPL",
	packages=find_packages(where='.', exclude=('data','debug','temp','conf'), include=('*',)),
	package_data={'correcter.bert_modified':['vocab.txt','bert_config.json']}
)