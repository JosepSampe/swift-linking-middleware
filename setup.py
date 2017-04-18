from setuptools import setup, find_packages

paste_factory = ['softlink = '
                 'softlink:filter_factory']

setup(name='swift-softlink-middleware',
      version='0.1.0',
      description='Softlink middleware for OpenStack Swift',
      author='Josep Sampe',
      packages=find_packages(),
      requires=['swift(>=1.4)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
