from setuptools import setup


setup(name="call_rc_data",
      packages=["call_rc_data"],
      license="BSD2CLAUSE",
      install_requires=[''],
      scripts=['scripts/get_rc_data'],
      version='0.1',
      description='A simple collector for some call data from RC database.',
      author='Silvio Ap Silva a.k.a Kanazuchi',
      author_email='contato@kanazuchi.com',
      url='http://github.com/kanazux/call_rc_data',
      zip_safe=False)
