from setuptools import setup, find_packages

setup(name='financialdataprovider',
      version='1.0.2',
      description='Download, store and access historical financial data for stocks.',
      url='http://github.com/brettelliot/financial-data-provider',
      author='Brett Elliot',
      author_email='brett@theelliots.net',
      license='MIT',
      packages=find_packages(),
      zip_safe=False)