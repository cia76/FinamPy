from setuptools import setup

setup(name='FinamPy',
      version='2.8.0',
      author='Чечет Игорь Александрович',
      description='Библиотека-обертка, которая позволяет работать с Finam Trade API брокера Финам из Python',
      url='https://github.com/cia76/FinamPy',
      packages=['FinamPy'],
      install_requires=[
            'pytz',  # ВременнЫе зоны
            'grpcio',  # gRPC
            'protobuf',  # proto
            'googleapis-common-protos',  # Google API
            'types-protobuf',  # Timestamp
      ],
      python_requires='>=3.12',
      package_data={'FinamPy': ['grpc/**/*']},  # Дополнительно копируем скрипты из папки grpc и вложенных в нее папок
      include_package_data=True,  # Включаем дополнительные скрипты
      )
