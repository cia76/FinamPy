from setuptools import setup, find_packages

setup(name='FinamPy',
      version='2.7.0',
      author='Чечет Игорь Александрович',
      description='Библиотека-обертка, которая позволяет работать с Finam Trade API брокера Финам из Python',
      url='https://github.com/cia76/FinamPy',
      packages=find_packages(),
      install_requires=[
            'pytz',  # ВременнЫе зоны
            'grpcio',  # gRPC
            'protobuf',  # proto
            'googleapis-common-protos'  # Google API
      ],
      python_requires='>=3.12',
      )
