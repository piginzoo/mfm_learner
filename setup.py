import os

from setuptools import setup, find_packages, Command


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


# Further down when you call setup()
setup(
    cmdclass={
        'clean': CleanCommand,
    },
    name="mfm_learner",
    version="1.0",
    description="mfm_learner",
    author="piginzoo",
    author_email="piginzoo@qq.com",
    url="https://github.com/piginzoo/mfm_learner.git",
    license="LGPL",
    packages=find_packages(where=".", exclude=('test', 'test.*', 'conf'), include=('*',))
)
